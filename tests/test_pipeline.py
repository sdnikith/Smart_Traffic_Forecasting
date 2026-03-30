import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import tempfile
import os
from pathlib import Path
import sys

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from pipelines import TrafficDataPipeline
from data_ingestion import TrafficDataCollector
from data_processing import TrafficFeatureEngineer
from models import TrafficModelTrainer, LinearTrafficModel


class TestTrafficDataPipeline:
    """Test cases for TrafficDataPipeline"""
    
    @pytest.fixture
    def sample_data(self):
        """Generate sample traffic data for testing"""
        dates = pd.date_range(start='2024-01-01', end='2024-01-07', freq='15min')
        locations = ['Location_A', 'Location_B']
        
        data = []
        for date in dates:
            for location in locations:
                data.append({
                    'timestamp': date,
                    'location_id': location,
                    'vehicle_count': np.random.randint(10, 100),
                    'avg_speed': np.random.uniform(20, 60),
                    'source': 'Test'
                })
        
        return pd.DataFrame(data)
    
    @pytest.fixture
    def temp_config(self):
        """Create temporary configuration file"""
        config_content = """
data_sources:
  database:
    connection_string: 'sqlite:///test.db'
    enabled: false
  api:
    api_key: 'test_key'
    base_url: 'http://test.com'
    enabled: false

feature_engineering:
  lag_periods: [1, 2, 3]
  rolling_windows: [3, 6]
  target_column: 'vehicle_count'

models:
  linear_ridge:
    enabled: true
  random_forest:
    enabled: false
  gradient_boosting:
    enabled: false
  lstm:
    enabled: false

training:
  test_size: 0.3
  retrain_interval_hours: 24
  model_save_path: 'test_models'

prediction:
  forecast_horizon_hours: 12
  prediction_interval_minutes: 15

monitoring:
  enabled: false
  alert_threshold_rmse: 50
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_content)
            yield f.name
        os.unlink(f.name)
    
    def test_pipeline_initialization(self, temp_config):
        """Test pipeline initialization"""
        pipeline = TrafficDataPipeline(temp_config)
        
        assert pipeline.config_manager.config_path == temp_config
        assert pipeline.data_collector is not None
        assert pipeline.feature_engineer is not None
        assert pipeline.model_trainer is not None
        assert len(pipeline.model_trainer.models) > 0
    
    def test_data_processing(self, sample_data):
        """Test data processing functionality"""
        engineer = TrafficFeatureEngineer()
        
        # Test data cleaning
        cleaned_data = engineer.clean_data(sample_data)
        assert len(cleaned_data) > 0
        assert 'timestamp' in cleaned_data.columns
        
        # Test feature engineering
        processed_data = engineer.prepare_features(sample_data)
        assert len(processed_data) > 0
        assert len(engineer.feature_columns) > 0
    
    def test_model_training(self, sample_data):
        """Test model training functionality"""
        engineer = TrafficFeatureEngineer()
        processed_data = engineer.prepare_features(sample_data)
        
        trainer = TrafficModelTrainer()
        trainer.add_model('linear', LinearTrafficModel('ridge'))
        
        # Prepare training data
        feature_columns = engineer.feature_columns
        X = processed_data[feature_columns]
        y = processed_data['vehicle_count']
        
        # Train model
        results = trainer.train_all_models(X, y, test_size=0.3)
        
        assert 'linear' in results
        assert 'error' not in results['linear']
        assert 'train_metrics' in results['linear']
        assert 'test_metrics' in results['linear']
    
    def test_pipeline_execution(self, sample_data, temp_config):
        """Test full pipeline execution"""
        # This is a simplified test since we can't run the full pipeline
        # without proper data sources
        pipeline = TrafficDataPipeline(temp_config)
        
        # Test that pipeline components are properly initialized
        assert pipeline.data_collector is not None
        assert pipeline.feature_engineer is not None
        assert pipeline.model_trainer is not None
        
        # Test pipeline status
        status = pipeline.get_pipeline_status()
        assert 'is_running' in status
        assert 'trained_models' in status
        assert 'config_file' in status


class TestTrafficFeatureEngineer:
    """Test cases for TrafficFeatureEngineer"""
    
    @pytest.fixture
    def sample_data(self):
        """Generate sample traffic data"""
        dates = pd.date_range(start='2024-01-01', periods=100, freq='15min')
        
        data = []
        for date in dates:
            data.append({
                'timestamp': date,
                'location_id': 'Test_Location',
                'vehicle_count': np.random.randint(10, 100),
                'avg_speed': np.random.uniform(20, 60),
                'source': 'Test'
            })
        
        return pd.DataFrame(data)
    
    def test_data_cleaning(self, sample_data):
        """Test data cleaning functionality"""
        engineer = TrafficFeatureEngineer()
        
        # Add some missing values
        dirty_data = sample_data.copy()
        dirty_data.loc[10:15, 'vehicle_count'] = np.nan
        
        cleaned_data = engineer.clean_data(dirty_data)
        
        # Check that missing values are handled
        assert not cleaned_data['vehicle_count'].isnull().any()
        assert len(cleaned_data) > 0
    
    def test_time_features(self, sample_data):
        """Test time feature creation"""
        engineer = TrafficFeatureEngineer()
        
        time_features = engineer.create_time_features(sample_data)
        
        # Check that time features are created
        expected_features = ['hour', 'day_of_week', 'month', 'hour_sin', 'hour_cos']
        for feature in expected_features:
            assert feature in time_features.columns
    
    def test_lag_features(self, sample_data):
        """Test lag feature creation"""
        engineer = TrafficFeatureEngineer()
        
        lag_features = engineer.create_lag_features(
            sample_data, ['vehicle_count'], [1, 2, 3]
        )
        
        # Check that lag features are created
        expected_lags = ['vehicle_count_lag_1', 'vehicle_count_lag_2', 'vehicle_count_lag_3']
        for lag in expected_lags:
            assert lag in lag_features.columns
    
    def test_rolling_features(self, sample_data):
        """Test rolling feature creation"""
        engineer = TrafficFeatureEngineer()
        
        rolling_features = engineer.create_rolling_features(
            sample_data, ['vehicle_count'], [3, 6]
        )
        
        # Check that rolling features are created
        expected_rolling = ['vehicle_count_rolling_mean_3', 'vehicle_count_rolling_std_3']
        for feature in expected_rolling:
            assert feature in rolling_features.columns
    
    def test_complete_feature_engineering(self, sample_data):
        """Test complete feature engineering pipeline"""
        engineer = TrafficFeatureEngineer()
        
        processed_data = engineer.prepare_features(sample_data)
        
        # Check that processing completed successfully
        assert len(processed_data) > 0
        assert len(engineer.feature_columns) > 0
        assert 'vehicle_count' in processed_data.columns


class TestTrafficModels:
    """Test cases for traffic forecasting models"""
    
    @pytest.fixture
    def sample_training_data(self):
        """Generate sample training data"""
        n_samples = 1000
        n_features = 10
        
        X = np.random.randn(n_samples, n_features)
        y = np.random.randint(10, 100, n_samples)
        
        feature_names = [f'feature_{i}' for i in range(n_features)]
        X_df = pd.DataFrame(X, columns=feature_names)
        y_series = pd.Series(y)
        
        return X_df, y_series
    
    def test_linear_model(self, sample_training_data):
        """Test linear regression model"""
        X, y = sample_training_data
        model = LinearTrafficModel('ridge')
        
        # Test training
        model.train(X, y)
        assert model.is_trained == True
        assert len(model.feature_columns) == len(X.columns)
        
        # Test prediction
        predictions = model.predict(X)
        assert len(predictions) == len(X)
        assert all(isinstance(pred, (int, float)) for pred in predictions)
        
        # Test evaluation
        metrics = model.evaluate(X, y)
        expected_metrics = ['mae', 'mse', 'rmse', 'r2', 'mape']
        for metric in expected_metrics:
            assert metric in metrics
            assert isinstance(metrics[metric], (int, float))
    
    def test_model_trainer(self, sample_training_data):
        """Test model trainer functionality"""
        X, y = sample_training_data
        trainer = TrafficModelTrainer()
        
        # Add models
        trainer.add_model('linear', LinearTrafficModel('ridge'))
        trainer.add_model('ridge', LinearTrafficModel('ridge'))
        
        # Train models
        results = trainer.train_all_models(X, y, test_size=0.3)
        
        # Check results
        assert len(results) == 2
        assert 'linear' in results
        assert 'ridge' in results
        
        # Check that models are trained
        for name, result in results.items():
            if 'error' not in result:
                assert 'train_metrics' in result
                assert 'test_metrics' in result
        
        # Test best model selection
        best_name, best_model = trainer.get_best_model('rmse')
        assert best_name in ['linear', 'ridge']
        assert best_model.is_trained == True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
