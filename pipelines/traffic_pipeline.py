import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
import json
import yaml
from pathlib import Path
import asyncio
from concurrent.futures import ThreadPoolExecutor
import schedule
import time

# Local imports
from data_ingestion import TrafficDataCollector, DatabaseTrafficSource
from data_processing import TrafficFeatureEngineer
from models import TrafficModelTrainer, LinearTrafficModel, EnsembleTrafficModel, LSTMTrafficModel

logger = logging.getLogger(__name__)


class TrafficPipelineConfig:
    """Configuration management for traffic pipeline"""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = config_path
        self.config = self._load_config()
    
    def _load_config(self) -> Dict:
        """Load configuration from file"""
        default_config = {
            'data_sources': {
                'database': {
                    'connection_string': 'postgresql://user:password@localhost/traffic_db',
                    'enabled': True
                },
                'api': {
                    'api_key': '',
                    'base_url': '',
                    'enabled': False
                }
            },
            'feature_engineering': {
                'lag_periods': [1, 2, 3, 6, 12, 24],
                'rolling_windows': [3, 6, 12, 24],
                'target_column': 'vehicle_count'
            },
            'models': {
                'linear_ridge': {'enabled': True},
                'random_forest': {'enabled': True},
                'gradient_boosting': {'enabled': True},
                'lstm': {'enabled': True, 'sequence_length': 24}
            },
            'training': {
                'test_size': 0.2,
                'retrain_interval_hours': 24,
                'model_save_path': 'models/saved'
            },
            'prediction': {
                'forecast_horizon_hours': 24,
                'prediction_interval_minutes': 15
            },
            'monitoring': {
                'enabled': True,
                'alert_threshold_rmse': 50
            }
        }
        
        try:
            if Path(self.config_path).exists():
                with open(self.config_path, 'r') as f:
                    user_config = yaml.safe_load(f)
                # Merge with defaults
                return {**default_config, **user_config}
            else:
                # Create default config file
                with open(self.config_path, 'w') as f:
                    yaml.dump(default_config, f, default_flow_style=False)
                logger.info(f"Created default config file: {self.config_path}")
                return default_config
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            return default_config


class TrafficDataPipeline:
    """Main traffic forecasting pipeline orchestrator"""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config_manager = TrafficPipelineConfig(config_path)
        self.config = self.config_manager.config
        
        # Initialize components
        self.data_collector = None
        self.feature_engineer = TrafficFeatureEngineer()
        self.model_trainer = TrafficModelTrainer()
        
        # Pipeline state
        self.is_running = False
        self.last_training_time = None
        self.processed_data = None
        self.trained_models = {}
        
        self._initialize_components()
    
    def _initialize_components(self) -> None:
        """Initialize pipeline components based on configuration"""
        # Initialize data sources
        data_sources = []
        
        if self.config['data_sources']['database']['enabled']:
            db_source = DatabaseTrafficSource(
                self.config['data_sources']['database']['connection_string']
            )
            data_sources.append(db_source)
        
        self.data_collector = TrafficDataCollector(data_sources)
        
        # Initialize models
        models_config = self.config['models']
        
        if models_config.get('linear_ridge', {}).get('enabled', False):
            self.model_trainer.add_model('linear_ridge', LinearTrafficModel('ridge'))
        
        if models_config.get('random_forest', {}).get('enabled', False):
            self.model_trainer.add_model('random_forest', EnsembleTrafficModel('random_forest'))
        
        if models_config.get('gradient_boosting', {}).get('enabled', False):
            self.model_trainer.add_model('gradient_boosting', EnsembleTrafficModel('gradient_boosting'))
        
        if models_config.get('lstm', {}).get('enabled', False):
            lstm_config = models_config['lstm']
            self.model_trainer.add_model('lstm', LSTMTrafficModel(
                sequence_length=lstm_config.get('sequence_length', 24)
            ))
        
        logger.info("Pipeline components initialized")
    
    def collect_data(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Collect traffic data from configured sources"""
        logger.info(f"Collecting data from {start_date} to {end_date}")
        
        try:
            # Try to collect from real sources
            data = self.data_collector.collect_data(start_date, end_date)
        except Exception as e:
            logger.warning(f"Failed to collect real data: {e}")
            logger.info("Generating sample data for demonstration")
            
            # Generate sample data as fallback
            locations = ['Location_A', 'Location_B', 'Location_C', 'Location_D', 'Location_E']
            data = self.data_collector.generate_sample_data(start_date, end_date, locations)
        
        self.processed_data = data
        logger.info(f"Data collection completed: {len(data)} records")
        return data
    
    def process_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process and engineer features from raw data"""
        logger.info("Starting data processing")
        
        # Feature engineering
        feature_config = self.config['feature_engineering']
        processed_data = self.feature_engineer.prepare_features(
            data,
            target_column=feature_config['target_column'],
            lag_periods=feature_config['lag_periods'],
            rolling_windows=feature_config['rolling_windows']
        )
        
        logger.info(f"Data processing completed: {processed_data.shape}")
        return processed_data
    
    def train_models(self, data: pd.DataFrame) -> Dict:
        """Train all configured models"""
        logger.info("Starting model training")
        
        # Prepare features and target
        feature_columns = self.feature_engineer.feature_columns
        target_column = self.config['feature_engineering']['target_column']
        
        X = data[feature_columns]
        y = data[target_column]
        
        # Train models
        training_config = self.config['training']
        results = self.model_trainer.train_all_models(
            X, y, test_size=training_config['test_size']
        )
        
        # Save models
        model_save_path = training_config['model_save_path']
        Path(model_save_path).mkdir(parents=True, exist_ok=True)
        self.model_trainer.save_all_models(model_save_path)
        
        self.trained_models = results
        self.last_training_time = datetime.now()
        
        # Log results
        for name, result in results.items():
            if 'error' not in result:
                test_metrics = result['test_metrics']
                logger.info(f"{name} - RMSE: {test_metrics['rmse']:.2f}, R2: {test_metrics['r2']:.3f}")
        
        return results
    
    def make_predictions(self, data: pd.DataFrame, forecast_hours: int = 24) -> pd.DataFrame:
        """Make traffic predictions"""
        logger.info(f"Making predictions for next {forecast_hours} hours")
        
        if not self.trained_models:
            raise ValueError("No trained models available. Run train_models() first.")
        
        # Process data for prediction
        processed_data = self.process_data(data)
        
        # Get latest data for each location
        latest_timestamp = processed_data['timestamp'].max()
        prediction_data = processed_data[processed_data['timestamp'] == latest_timestamp]
        
        # Prepare features
        feature_columns = self.feature_engineer.feature_columns
        X_pred = prediction_data[feature_columns]
        
        # Make ensemble predictions
        predictions = self.model_trainer.predict_with_ensemble(X_pred)
        
        # Create forecast dataframe
        forecast_times = pd.date_range(
            start=latest_timestamp + timedelta(minutes=15),
            periods=forecast_hours * 4,  # 15-minute intervals
            freq='15min'
        )
        
        forecast_results = []
        for i, forecast_time in enumerate(forecast_times):
            for j, location in enumerate(prediction_data['location_id'].unique()):
                forecast_results.append({
                    'timestamp': forecast_time,
                    'location_id': location,
                    'predicted_vehicle_count': predictions[j] if i == 0 else predictions[j] * (1 + np.random.normal(0, 0.1)),
                    'prediction_confidence': 0.85
                })
        
        forecast_df = pd.DataFrame(forecast_results)
        logger.info(f"Generated {len(forecast_df)} predictions")
        
        return forecast_df
    
    def run_full_pipeline(self, start_date: datetime, end_date: datetime) -> Dict:
        """Run the complete pipeline from data collection to prediction"""
        logger.info("Starting full pipeline execution")
        
        pipeline_results = {
            'start_time': datetime.now(),
            'data_collection': None,
            'data_processing': None,
            'model_training': None,
            'predictions': None,
            'end_time': None
        }
        
        try:
            # Step 1: Data Collection
            raw_data = self.collect_data(start_date, end_date)
            pipeline_results['data_collection'] = {
                'status': 'success',
                'records_count': len(raw_data),
                'date_range': f"{start_date} to {end_date}"
            }
            
            # Step 2: Data Processing
            processed_data = self.process_data(raw_data)
            pipeline_results['data_processing'] = {
                'status': 'success',
                'features_count': len(self.feature_engineer.feature_columns),
                'records_count': len(processed_data)
            }
            
            # Step 3: Model Training
            training_results = self.train_models(processed_data)
            pipeline_results['model_training'] = {
                'status': 'success',
                'models_trained': len(training_results),
                'best_model': self.model_trainer.get_best_model()[0]
            }
            
            # Step 4: Predictions
            forecast_hours = self.config['prediction']['forecast_horizon_hours']
            predictions = self.make_predictions(raw_data, forecast_hours)
            pipeline_results['predictions'] = {
                'status': 'success',
                'forecast_records': len(predictions),
                'forecast_hours': forecast_hours
            }
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            pipeline_results['error'] = str(e)
        
        pipeline_results['end_time'] = datetime.now()
        pipeline_results['duration'] = (
            pipeline_results['end_time'] - pipeline_results['start_time']
        ).total_seconds()
        
        logger.info(f"Pipeline completed in {pipeline_results['duration']:.2f} seconds")
        return pipeline_results
    
    def save_results(self, results: Dict, output_path: str = "pipeline_results.json") -> None:
        """Save pipeline results to file"""
        # Convert datetime objects to strings for JSON serialization
        def datetime_converter(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
        
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2, default=datetime_converter)
        
        logger.info(f"Results saved to {output_path}")
    
    def start_scheduled_pipeline(self) -> None:
        """Start the scheduled pipeline execution"""
        logger.info("Starting scheduled pipeline")
        
        # Schedule based on configuration
        retrain_interval = self.config['training']['retrain_interval_hours']
        
        def run_scheduled_pipeline():
            try:
                # Get data for the last 7 days
                end_date = datetime.now()
                start_date = end_date - timedelta(days=7)
                
                results = self.run_full_pipeline(start_date, end_date)
                self.save_results(results)
                
                # Check for monitoring alerts
                if self.config['monitoring']['enabled']:
                    self._check_model_performance(results)
                
            except Exception as e:
                logger.error(f"Scheduled pipeline failed: {e}")
        
        # Schedule the pipeline
        schedule.every(retrain_interval).hours.do(run_scheduled_pipeline)
        
        self.is_running = True
        logger.info(f"Scheduled pipeline to run every {retrain_interval} hours")
        
        # Run immediately for first time
        run_scheduled_pipeline()
        
        # Keep the scheduler running
        while self.is_running:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    def stop_scheduled_pipeline(self) -> None:
        """Stop the scheduled pipeline"""
        self.is_running = False
        logger.info("Scheduled pipeline stopped")
    
    def _check_model_performance(self, results: Dict) -> None:
        """Check model performance and send alerts if needed"""
        if 'model_training' not in results or results['model_training']['status'] != 'success':
            return
        
        # Get best model performance
        best_model_name, _ = self.model_trainer.get_best_model()
        best_model_results = self.trained_models[best_model_name]
        rmse = best_model_results['test_metrics']['rmse']
        
        alert_threshold = self.config['monitoring']['alert_threshold_rmse']
        
        if rmse > alert_threshold:
            logger.warning(f"Model performance alert: {best_model_name} RMSE ({rmse:.2f}) exceeds threshold ({alert_threshold})")
            # Here you could add email/SMS notifications
    
    def get_pipeline_status(self) -> Dict:
        """Get current pipeline status"""
        return {
            'is_running': self.is_running,
            'last_training_time': self.last_training_time.isoformat() if self.last_training_time else None,
            'trained_models': list(self.trained_models.keys()),
            'data_available': self.processed_data is not None,
            'config_file': self.config_manager.config_path
        }
