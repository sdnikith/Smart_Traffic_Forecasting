"""
Custom Operators for Traffic Platform
"""

from airflow.models.baseoperator import BaseOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults

import boto3
import json
import logging
import subprocess
import tempfile
import os

logger = logging.getLogger(__name__)

class KafkaProducerOperator(BaseOperator):
    """Custom operator for starting Kafka producers"""
    
    template_fields = ['kafka_config']
    
    @apply_defaults
    def __init__(self, kafka_config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kafka_config = kafka_config
    
    def execute(self, context):
        """Start Kafka producer process"""
        logger.info(f"Starting Kafka producer with config: {self.kafka_config}")
        
        try:
            # Extract producer script
            producer_script = self.kafka_config['producer_script']
            
            # Build command
            cmd = [
                'python', producer_script,
                '--bootstrap-servers', self.kafka_config['bootstrap_servers'],
                '--topic', self.kafka_config['topic'],
                '--schema-registry-url', self.kafka_config['schema_registry_url']
            ]
            
            # Add optional parameters
            if 'replay_speed_ms' in self.kafka_config:
                cmd.extend(['--replay-speed', str(self.kafka_config['replay_speed_ms'])])
            
            if 'api_key' in self.kafka_config:
                cmd.extend(['--api-key', self.kafka_config['api_key']])
            
            if 'mock_mode' in self.kafka_config:
                cmd.extend(['--mock', self.kafka_config['mock_mode']])
            
            # Start process
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=os.environ.copy()
            )
            
            logger.info(f"Kafka producer started with PID: {process.pid}")
            
            # Store PID for monitoring
            self.producer_pid = process.pid
            
            return {
                'status': 'started',
                'pid': process.pid,
                'command': ' '.join(cmd)
            }
            
        except Exception as e:
            logger.error(f"Failed to start Kafka producer: {e}")
            raise AirflowException(f"Kafka producer startup failed: {e}")
    
    def on_kill(self):
        """Kill Kafka producer process"""
        if hasattr(self, 'producer_pid'):
            logger.info(f"Killing Kafka producer process {self.producer_pid}")
            try:
                os.kill(self.producer_pid, 15)  # SIGTERM
            except ProcessLookupError:
                logger.warning(f"Process {self.producer_pid} not found")

class SnowflakeOperator(BaseOperator):
    """Custom operator for Snowflake operations"""
    
    template_fields = ['snowflake_config']
    
    @apply_defaults
    def __init__(self, snowflake_config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.snowflake_config = snowflake_config
    
    def execute(self, context):
        """Execute Snowflake operation"""
        logger.info(f"Executing Snowflake operation: {self.snowflake_config['operation']}")
        
        try:
            # Get Snowflake connection
            snowflake_hook = BaseHook.get_connection('snowflake_default')
            
            # Build SQL based on operation
            if self.snowflake_config['operation'] == 'optimize':
                sql = self._build_optimize_sql()
            elif self.snowflake_config['operation'] == 'deploy_models':
                sql = self._build_deploy_sql()
            elif self.snowflake_config['operation'] == 'vacuum':
                sql = self._build_vacuum_sql()
            else:
                raise AirflowException(f"Unknown operation: {self.snowflake_config['operation']}")
            
            # Execute SQL (using snowflake-connector-python)
            import snowflake.connector
            
            conn = snowflake.connector.connect(
                user=snowflake_hook.login,
                password=snowflake_hook.password,
                account=snowflake_hook.host,
                warehouse=self.snowflake_config.get('warehouse', 'TRAFFIC_TRANSFORM_WH')
            )
            
            cursor = conn.cursor()
            cursor.execute(sql)
            results = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            logger.info(f"Snowflake operation completed. Results: {len(results)} rows affected")
            
            return {
                'operation': self.snowflake_config['operation'],
                'rows_affected': len(results),
                'results': results
            }
            
        except Exception as e:
            logger.error(f"Snowflake operation failed: {e}")
            raise AirflowException(f"Snowflake operation failed: {e}")
    
    def _build_optimize_sql(self):
        """Build SQL for table optimization"""
        tables = self.snowflake_config.get('tables', [])
        warehouse = self.snowflake_config.get('warehouse', 'TRAFFIC_ANALYTICS_WH')
        
        sql_statements = []
        
        for table in tables:
            sql = f"""
                ALTER WAREHOUSE {warehouse} RESUME IF SUSPENDED;
                OPTIMIZE TRAFFIC_DB.ANALYTICS.{table} ZORDER BY (sensor_id, reading_timestamp);
            """
            sql_statements.append(sql)
        
        return ';\n'.join(sql_statements)
    
    def _build_deploy_sql(self):
        """Build SQL for model deployment"""
        return """
            USE WAREHOUSE TRAFFIC_ML_WH;
            ALTER SESSION SET QUERY_TAG = 'model-deployment';
            
            -- Deploy trained models to production tables
            INSERT INTO TRAFFIC_DB.ML_OUTPUT.TRAFFIC_PREDICTIONS
            SELECT * FROM TRAFFIC_DB.ML_OUTPUT.MODEL_STAGING;
        """
    
    def _build_vacuum_sql(self):
        """Build SQL for vacuum operation"""
        return """
            VACUUM TRAFFIC_DB.ANALYTICS.MART_HOURLY_TRAFFIC RETAIN 168 HOURS;
            VACUUM TRAFFIC_DB.RAW.TRAFFIC_EVENTS RETAIN 168 HOURS;
        """

class DeltaLakeOperator(BaseOperator):
    """Custom operator for Delta Lake operations"""
    
    template_fields = ['delta_config']
    
    @apply_defaults
    def __init__(self, delta_config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.delta_config = delta_config
    
    def execute(self, context):
        """Execute Delta Lake operation"""
        logger.info(f"Executing Delta Lake operation: {self.delta_config['operation']}")
        
        try:
            if self.delta_config['operation'] == 'maintenance':
                return self._run_maintenance()
            elif self.delta_config['operation'] == 'batch_load':
                return self._run_batch_load()
            else:
                raise AirflowException(f"Unknown Delta Lake operation: {self.delta_config['operation']}")
                
        except Exception as e:
            logger.error(f"Delta Lake operation failed: {e}")
            raise AirflowException(f"Delta Lake operation failed: {e}")
    
    def _run_maintenance(self):
        """Run Delta Lake maintenance"""
        from delta_lake.delta_maintenance import DeltaMaintenance
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder.appName("DeltaMaintenance").getOrCreate()
        maintenance = DeltaMaintenance(spark)
        
        # Run maintenance
        maintenance.run_maintenance()
        
        spark.stop()
        
        return {'status': 'completed', 'operation': 'maintenance'}
    
    def _run_batch_load(self):
        """Run batch load to Delta Lake"""
        s3_path = self.delta_config['source_path']
        delta_path = self.delta_config['target_path']
        
        # Use S3DistCp for efficient copying
        cmd = [
            'aws', 's3', 'cp',
            '--recursive',
            s3_path,
            delta_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise AirflowException(f"S3 copy failed: {result.stderr}")
        
        return {
            'status': 'completed',
            'operation': 'batch_load',
            'files_copied': result.stdout.count('\n')
        }

class ObservabilityOperator(BaseOperator):
    """Custom operator for data observability checks"""
    
    template_fields = ['observability_config']
    
    @apply_defaults
    def __init__(self, observability_config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.observability_config = observability_config
    
    def execute(self, context):
        """Run observability checks"""
        logger.info("Running data observability checks")
        
        results = {
            'status': 'completed',
            'checks_performed': [],
            'issues_found': [],
            'issues_count': 0
        }
        
        try:
            # Freshness check
            if self.observability_config.get('freshness_check', False):
                from observability.freshness_monitor import FreshnessMonitor
                
                monitor = FreshnessMonitor()
                freshness_report = monitor.run_freshness_check()
                
                results['checks_performed'].append('freshness_monitoring')
                
                if freshness_report:
                    critical_sensors = freshness_report.get('sensor_freshness', {}).get('critical_sensors', 0)
                    if critical_sensors > 0:
                        results['issues_found'].append(f"Critical sensors: {critical_sensors}")
                        results['issues_count'] += 1
            
            # Anomaly detection
            if self.observability_config.get('anomaly_detection', False):
                from observability.volume_anomaly_detector import VolumeAnomalyDetector
                
                detector = VolumeAnomalyDetector()
                anomaly_report = detector.run_anomaly_detection()
                
                results['checks_performed'].append('anomaly_detection')
                
                if anomaly_report:
                    total_anomalies = anomaly_report.get('volume_anomalies', {}).get('anomaly_count', 0)
                    if total_anomalies > 0:
                        results['issues_found'].append(f"Volume anomalies: {total_anomalies}")
                        results['issues_count'] += 1
            
            # Schema drift check
            if self.observability_config.get('schema_drift_check', False):
                from observability.schema_drift_checker import SchemaDriftChecker
                
                checker = SchemaDriftChecker()
                drift_report = checker.run_schema_drift_check()
                
                results['checks_performed'].append('schema_drift_check')
                
                if drift_report:
                    total_changes = drift_report.get('summary', {}).get('total_changes', 0)
                    if total_changes > 0:
                        results['issues_found'].append(f"Schema changes: {total_changes}")
                        results['issues_count'] += 1
            
            # Lineage tracking
            if self.observability_config.get('lineage_tracking', False):
                from observability.lineage_tracker import LineageTracker
                
                tracker = LineageTracker()
                lineage_report = tracker.run_lineage_tracking()
                
                results['checks_performed'].append('lineage_tracking')
                
                if lineage_report:
                    results['lineage_report_s3_key'] = lineage_report.get('s3_key')
            
            logger.info(f"Observability checks completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Observability checks failed: {e}")
            raise AirflowException(f"Observability checks failed: {e}")

class TrafficSensor(BaseHook):
    """Custom hook for traffic sensor data"""
    
    def __init__(self, traffic_conn_id='traffic_default'):
        super().__init__()
        self.traffic_conn_id = traffic_conn_id
    
    def get_conn(self):
        """Get traffic connection"""
        conn = self.get_connection(self.traffic_conn_id)
        return {
            'host': conn.host,
            'port': conn.port,
            'database': conn.schema,
            'username': conn.login,
            'password': conn.password
        }
    
    def get_sensor_data(self, sensor_id, start_time, end_time):
        """Get sensor data for time range"""
        import requests
        
        conn = self.get_conn()
        url = f"http://{conn['host']}:{conn['port']}/api/sensors/{sensor_id}/data"
        
        params = {
            'start_time': start_time,
            'end_time': end_time
        }
        
        response = requests.get(url, params=params, auth=(conn['username'], conn['password']))
        response.raise_for_status()
        
        return response.json()
