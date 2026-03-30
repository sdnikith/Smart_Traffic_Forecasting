import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
from loguru import logger
import json
import sqlite3
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots


class TrafficMetricsCollector:
    """Collects and stores traffic pipeline metrics"""
    
    def __init__(self, db_path: str = "monitoring/metrics.db"):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._initialize_database()
    
    def _initialize_database(self) -> None:
        """Initialize metrics database"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME,
                    metric_name TEXT,
                    metric_value REAL,
                    model_name TEXT,
                    location_id TEXT,
                    metadata TEXT
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS data_quality_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME,
                    total_records INTEGER,
                    missing_values INTEGER,
                    duplicate_records INTEGER,
                    outliers INTEGER,
                    data_source TEXT
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS prediction_accuracy (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME,
                    model_name TEXT,
                    location_id TEXT,
                    actual_value REAL,
                    predicted_value REAL,
                    error REAL,
                    absolute_error REAL
                )
            """)
    
    def record_pipeline_metric(self, metric_name: str, metric_value: float,
                             model_name: Optional[str] = None,
                             location_id: Optional[str] = None,
                             metadata: Optional[Dict] = None) -> None:
        """Record a pipeline metric"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO pipeline_metrics 
                (timestamp, metric_name, metric_value, model_name, location_id, metadata)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                datetime.now(),
                metric_name,
                metric_value,
                model_name,
                location_id,
                json.dumps(metadata) if metadata else None
            ))
    
    def record_data_quality(self, total_records: int, missing_values: int,
                          duplicate_records: int, outliers: int,
                          data_source: str) -> None:
        """Record data quality metrics"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO data_quality_metrics 
                (timestamp, total_records, missing_values, duplicate_records, outliers, data_source)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                datetime.now(),
                total_records,
                missing_values,
                duplicate_records,
                outliers,
                data_source
            ))
    
    def record_prediction_accuracy(self, model_name: str, location_id: str,
                                 actual_value: float, predicted_value: float) -> None:
        """Record prediction accuracy"""
        error = actual_value - predicted_value
        absolute_error = abs(error)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO prediction_accuracy 
                (timestamp, model_name, location_id, actual_value, predicted_value, error, absolute_error)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                datetime.now(),
                model_name,
                location_id,
                actual_value,
                predicted_value,
                error,
                absolute_error
            ))
    
    def get_metrics_summary(self, hours: int = 24) -> Dict:
        """Get metrics summary for the last N hours"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        with sqlite3.connect(self.db_path) as conn:
            # Pipeline metrics
            pipeline_df = pd.read_sql_query("""
                SELECT metric_name, AVG(metric_value) as avg_value, 
                       COUNT(*) as count, model_name
                FROM pipeline_metrics 
                WHERE timestamp > ?
                GROUP BY metric_name, model_name
            """, conn, params=(cutoff_time,))
            
            # Data quality
            quality_df = pd.read_sql_query("""
                SELECT AVG(total_records) as avg_records,
                       AVG(missing_values) as avg_missing,
                       AVG(duplicate_records) as avg_duplicates,
                       AVG(outliers) as avg_outliers,
                       data_source
                FROM data_quality_metrics 
                WHERE timestamp > ?
                
                GROUP BY data_source
            """, conn, params=(cutoff_time,))
            
            # Prediction accuracy
            accuracy_df = pd.read_sql_query("""
                SELECT model_name, location_id,
                       AVG(absolute_error) as mae,
                       COUNT(*) as prediction_count
                FROM prediction_accuracy 
                WHERE timestamp > ?
                GROUP BY model_name, location_id
            """, conn, params=(cutoff_time,))
        
        return {
            'pipeline_metrics': pipeline_df.to_dict('records'),
            'data_quality': quality_df.to_dict('records'),
            'prediction_accuracy': accuracy_df.to_dict('records'),
            'time_period_hours': hours
        }


class TrafficAlertManager:
    """Manages alerts and notifications for traffic pipeline"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.alert_thresholds = config.get('alert_thresholds', {})
        self.notification_channels = config.get('notification_channels', [])
        self.active_alerts = []
    
    def check_metric_thresholds(self, metrics: Dict) -> List[Dict]:
        """Check metrics against thresholds and generate alerts"""
        alerts = []
        
        # Check pipeline metrics
        for metric in metrics.get('pipeline_metrics', []):
            metric_name = metric['metric_name']
            avg_value = metric['avg_value']
            
            if metric_name in self.alert_thresholds:
                threshold = self.alert_thresholds[metric_name]
                if avg_value > threshold:
                    alert = {
                        'timestamp': datetime.now(),
                        'type': 'threshold_violation',
                        'metric_name': metric_name,
                        'value': avg_value,
                        'threshold': threshold,
                        'severity': 'high' if avg_value > threshold * 1.5 else 'medium',
                        'model_name': metric.get('model_name')
                    }
                    alerts.append(alert)
        
        # Check data quality
        for quality in metrics.get('data_quality', []):
            missing_ratio = quality['avg_missing'] / quality['avg_records'] if quality['avg_records'] > 0 else 0
            
            if missing_ratio > 0.1:  # More than 10% missing data
                alert = {
                    'timestamp': datetime.now(),
                    'type': 'data_quality',
                    'metric_name': 'missing_data_ratio',
                    'value': missing_ratio,
                    'threshold': 0.1,
                    'severity': 'high' if missing_ratio > 0.2 else 'medium',
                    'data_source': quality['data_source']
                }
                alerts.append(alert)
        
        self.active_alerts.extend(alerts)
        return alerts
    
    def send_alert(self, alert: Dict) -> None:
        """Send alert notification"""
        message = self._format_alert_message(alert)
        
        for channel in self.notification_channels:
            if channel['type'] == 'email':
                self._send_email_alert(channel, message)
            elif channel['type'] == 'slack':
                self._send_slack_alert(channel, message)
            elif channel['type'] == 'log':
                logger.warning(f"ALERT: {message}")
    
    def _format_alert_message(self, alert: Dict) -> str:
        """Format alert message"""
        severity_emoji = {'low': '🟡', 'medium': '🟠', 'high': '🔴'}
        
        message = f"{severity_emoji.get(alert['severity'], '⚠️')} {alert['type'].upper()}: {alert['metric_name']}\n"
        message += f"Value: {alert['value']:.3f}\n"
        message += f"Threshold: {alert['threshold']:.3f}\n"
        message += f"Time: {alert['timestamp']}\n"
        
        if 'model_name' in alert:
            message += f"Model: {alert['model_name']}\n"
        if 'data_source' in alert:
            message += f"Data Source: {alert['data_source']}\n"
        
        return message
    
    def _send_email_alert(self, channel: Dict, message: str) -> None:
        """Send email alert (placeholder implementation)"""
        logger.info(f"Email alert would be sent to {channel['recipients']}: {message}")
    
    def _send_slack_alert(self, channel: Dict, message: str) -> None:
        """Send Slack alert (placeholder implementation)"""
        logger.info(f"Slack alert would be sent to {channel['webhook_url']}: {message}")


class TrafficDashboard:
    """Creates visualizations and dashboards for traffic monitoring"""
    
    def __init__(self, metrics_collector: TrafficMetricsCollector):
        self.metrics_collector = metrics_collector
    
    def create_performance_dashboard(self, hours: int = 24) -> go.Figure:
        """Create performance dashboard"""
        metrics = self.metrics_collector.get_metrics_summary(hours)
        
        # Create subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Model RMSE', 'Data Quality', 'Prediction Error Distribution', 'Alert Frequency'),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # Model RMSE
        pipeline_metrics = metrics['pipeline_metrics']
        rmse_data = [m for m in pipeline_metrics if m['metric_name'] == 'rmse']
        
        if rmse_data:
            models = [m['model_name'] for m in rmse_data]
            values = [m['avg_value'] for m in rmse_data]
            
            fig.add_trace(
                go.Bar(x=models, y=values, name='RMSE'),
                row=1, col=1
            )
        
        # Data Quality
        quality_data = metrics['data_quality']
        if quality_data:
            sources = [q['data_source'] for q in quality_data]
            missing_ratios = [q['avg_missing'] / q['avg_records'] for q in quality_data]
            
            fig.add_trace(
                go.Bar(x=sources, y=missing_ratios, name='Missing Data Ratio'),
                row=1, col=2
            )
        
        # Prediction Error Distribution
        accuracy_data = metrics['prediction_accuracy']
        if accuracy_data:
            mae_values = [a['mae'] for a in accuracy_data]
            
            fig.add_trace(
                go.Histogram(x=mae_values, name='MAE Distribution'),
                row=2, col=1
            )
        
        fig.update_layout(
            title_text=f"Traffic Pipeline Performance Dashboard (Last {hours} hours)",
            showlegend=True,
            height=800
        )
        
        return fig
    
    def create_traffic_forecast_chart(self, historical_data: pd.DataFrame,
                                    forecast_data: pd.DataFrame) -> go.Figure:
        """Create traffic forecast visualization"""
        fig = go.Figure()
        
        # Historical data
        for location in historical_data['location_id'].unique():
            location_data = historical_data[historical_data['location_id'] == location]
            
            fig.add_trace(go.Scatter(
                x=location_data['timestamp'],
                y=location_data['vehicle_count'],
                mode='lines+markers',
                name=f'Historical - {location}',
                line=dict(width=2)
            ))
        
        # Forecast data
        for location in forecast_data['location_id'].unique():
            location_forecast = forecast_data[forecast_data['location_id'] == location]
            
            fig.add_trace(go.Scatter(
                x=location_forecast['timestamp'],
                y=location_forecast['predicted_vehicle_count'],
                mode='lines+markers',
                name=f'Forecast - {location}',
                line=dict(width=2, dash='dash')
            ))
        
        fig.update_layout(
            title='Traffic Forecast',
            xaxis_title='Time',
            yaxis_title='Vehicle Count',
            hovermode='x unified'
        )
        
        return fig
    
    def save_dashboard_html(self, fig: go.Figure, filename: str) -> None:
        """Save dashboard as HTML file"""
        fig.write_html(filename)
        logger.info(f"Dashboard saved to {filename}")


class TrafficMonitor:
    """Main monitoring orchestrator"""
    
    def __init__(self, config_path: str = "monitoring_config.yaml"):
        self.config = self._load_config(config_path)
        self.metrics_collector = TrafficMetricsCollector()
        self.alert_manager = TrafficAlertManager(self.config)
        self.dashboard = TrafficDashboard(self.metrics_collector)
        
        logger.info("Traffic monitoring system initialized")
    
    def _load_config(self, config_path: str) -> Dict:
        """Load monitoring configuration"""
        default_config = {
            'alert_thresholds': {
                'rmse': 50,
                'mae': 30,
                'missing_data_ratio': 0.1
            },
            'notification_channels': [
                {'type': 'log'},
                # {'type': 'email', 'recipients': ['admin@example.com']},
                # {'type': 'slack', 'webhook_url': 'https://hooks.slack.com/...'}
            ],
            'dashboard_settings': {
                'update_interval_minutes': 15,
                'save_dashboards': True
            }
        }
        
        try:
            if Path(config_path).exists():
                import yaml
                with open(config_path, 'r') as f:
                    user_config = yaml.safe_load(f)
                return {**default_config, **user_config}
            else:
                return default_config
        except Exception as e:
            logger.error(f"Error loading monitoring config: {e}")
            return default_config
    
    def monitor_pipeline_execution(self, pipeline_results: Dict) -> None:
        """Monitor pipeline execution and record metrics"""
        logger.info("Monitoring pipeline execution")
        
        # Record pipeline metrics
        if pipeline_results.get('model_training', {}).get('status') == 'success':
            # This would be populated with actual model metrics
            self.metrics_collector.record_pipeline_metric(
                'pipeline_success', 1.0, metadata={'duration': pipeline_results.get('duration')}
            )
        
        # Record data quality metrics
        if pipeline_results.get('data_collection', {}).get('status') == 'success':
            records_count = pipeline_results['data_collection']['records_count']
            self.metrics_collector.record_data_quality(
                total_records=records_count,
                missing_values=0,  # Would be calculated from actual data
                duplicate_records=0,  # Would be calculated from actual data
                outliers=0,  # Would be calculated from actual data
                data_source='pipeline'
            )
        
        # Check for alerts
        current_metrics = self.metrics_collector.get_metrics_summary(hours=1)
        alerts = self.alert_manager.check_metric_thresholds(current_metrics)
        
        for alert in alerts:
            self.alert_manager.send_alert(alert)
        
        # Generate and save dashboard
        if self.config['dashboard_settings']['save_dashboards']:
            dashboard_fig = self.dashboard.create_performance_dashboard()
            self.dashboard.save_dashboard_html(dashboard_fig, "monitoring/dashboard.html")
    
    def get_monitoring_summary(self) -> Dict:
        """Get comprehensive monitoring summary"""
        metrics_24h = self.metrics_collector.get_metrics_summary(hours=24)
        metrics_7d = self.metrics_collector.get_metrics_summary(hours=168)
        
        return {
            'last_24_hours': metrics_24h,
            'last_7_days': metrics_7d,
            'active_alerts': self.alert_manager.active_alerts[-10:],  # Last 10 alerts
            'monitoring_status': 'active'
        }
