"""
Custom Sensors for Traffic Platform
"""

from airflow.sensors.base import BaseSensorOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults

import logging
import requests
import time

logger = logging.getLogger(__name__)

class KafkaTopicSensor(BaseSensorOperator):
    """Sensor to check if Kafka topic has data"""
    
    template_fields = ['kafka_config']
    
    @apply_defaults
    def __init__(self, kafka_config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kafka_config = kafka_config
    
    def poke(self, context):
        """Check if Kafka topic has messages"""
        try:
            from confluent_kafka import Consumer, KafkaException
            
            # Create consumer
            consumer_conf = {
                'bootstrap.servers': self.kafka_config['bootstrap_servers'],
                'group.id': 'kafka-sensor-group',
                'auto.offset.reset': 'latest',
                'enable.auto.commit': False
            }
            
            consumer = Consumer(consumer_conf)
            consumer.subscribe([self.kafka_config['topic']])
            
            # Poll for messages
            msg = consumer.poll(timeout=5)
            
            if msg is not None:
                logger.info(f"Found message in topic {self.kafka_config['topic']}")
                consumer.close()
                return True
            else:
                logger.info(f"No messages found in topic {self.kafka_config['topic']}")
                consumer.close()
                return False
                
        except Exception as e:
            logger.error(f"Error checking Kafka topic: {e}")
            return False

class SnowflakeTableSensor(BaseSensorOperator):
    """Sensor to check if Snowflake table has fresh data"""
    
    template_fields = ['snowflake_config']
    
    @apply_defaults
    def __init__(self, snowflake_config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.snowflake_config = snowflake_config
        self.soft_fail = False
    
    def poke(self, context):
        """Check if Snowflake table has fresh data"""
        try:
            import snowflake.connector
            
            # Get connection
            snowflake_hook = BaseHook.get_connection('snowflake_default')
            
            conn = snowflake.connector.connect(
                user=snowflake_hook.login,
                password=snowflake_hook.password,
                account=snowflake_hook.host,
                warehouse=self.snowflake_config.get('warehouse', 'TRAFFIC_ANALYTICS_WH')
            )
            
            cursor = conn.cursor()
            
            # Check for fresh data
            table_name = self.snowflake_config['table']
            freshness_hours = self.snowflake_config.get('freshness_hours', 1)
            
            query = f"""
                SELECT COUNT(*) AS fresh_records
                FROM TRAFFIC_DB.ANALYTICS.{table_name}
                WHERE reading_timestamp >= DATEADD('hour', -{freshness_hours}, CURRENT_TIMESTAMP())
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            fresh_records = result[0] if result else 0
            
            if fresh_records > 0:
                logger.info(f"Found {fresh_records} fresh records in {table_name}")
                return True
            else:
                logger.info(f"No fresh records found in {table_name} (last {freshness_hours} hours)")
                return False
                
        except Exception as e:
            logger.error(f"Error checking Snowflake table: {e}")
            return False

class S3FileSensor(BaseSensorOperator):
    """Sensor to check if S3 files exist"""
    
    template_fields = ['s3_config']
    
    @apply_defaults
    def __init__(self, s3_config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_config = s3_config
    
    def poke(self, context):
        """Check if S3 files exist"""
        try:
            import boto3
            
            s3 = boto3.client('s3')
            
            bucket = self.s3_config['bucket']
            key_prefix = self.s3_config['key_prefix']
            
            # List objects
            response = s3.list_objects_v2(
                Bucket=bucket,
                Prefix=key_prefix,
                MaxKeys=10
            )
            
            objects = response.get('Contents', [])
            
            if objects:
                logger.info(f"Found {len(objects)} objects in s3://{bucket}/{key_prefix}")
                return True
            else:
                logger.info(f"No objects found in s3://{bucket}/{key_prefix}")
                return False
                
        except Exception as e:
            logger.error(f"Error checking S3 files: {e}")
            return False

class APIHealthSensor(BaseSensorOperator):
    """Sensor to check API health"""
    
    template_fields = ['api_config']
    
    @apply_defaults
    def __init__(self, api_config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_config = api_config
    
    def poke(self, context):
        """Check API health"""
        try:
            url = self.api_config['url']
            timeout = self.api_config.get('timeout', 30)
            
            response = requests.get(url, timeout=timeout)
            
            if response.status_code == 200:
                logger.info(f"API health check passed: {url}")
                return True
            else:
                logger.warning(f"API health check failed: {url} - {response.status_code}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"API health check error: {e}")
            return False

class TrafficDataQualitySensor(BaseSensorOperator):
    """Sensor to check data quality metrics"""
    
    template_fields = ['quality_config']
    
    @apply_defaults
    def __init__(self, quality_config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.quality_config = quality_config
    
    def poke(self, context):
        """Check data quality metrics"""
        try:
            import snowflake.connector
            
            # Get connection
            snowflake_hook = BaseHook.get_connection('snowflake_default')
            
            conn = snowflake.connector.connect(
                user=snowflake_hook.login,
                password=snowflake_hook.password,
                account=snowflake_hook.host,
                warehouse='TRAFFIC_ANALYTICS_WH'
            )
            
            cursor = conn.cursor()
            
            # Check data quality metrics
            table_name = self.quality_config['table']
            
            query = f"""
                SELECT 
                    COUNT(*) AS total_records,
                    COUNT(CASE WHEN traffic_volume IS NULL THEN 1 END) AS null_volumes,
                    COUNT(CASE WHEN traffic_volume < 0 THEN 1 END) AS negative_volumes,
                    COUNT(CASE WHEN reading_timestamp > CURRENT_TIMESTAMP() THEN 1 END) AS future_timestamps,
                    AVG(traffic_volume) AS avg_volume,
                    STDDEV(traffic_volume) AS std_volume
                FROM TRAFFIC_DB.ANALYTICS.{table_name}
                WHERE reading_timestamp >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            if result:
                total_records, null_volumes, negative_volumes, future_timestamps, avg_volume, std_volume = result
                
                # Define quality thresholds
                quality_issues = []
                
                if null_volumes > 0:
                    quality_issues.append(f"Null volumes: {null_volumes}")
                
                if negative_volumes > 0:
                    quality_issues.append(f"Negative volumes: {negative_volumes}")
                
                if future_timestamps > 0:
                    quality_issues.append(f"Future timestamps: {future_timestamps}")
                
                if total_records < 1000:  # Minimum expected records
                    quality_issues.append(f"Low record count: {total_records}")
                
                if len(quality_issues) == 0:
                    logger.info(f"Data quality check passed for {table_name}")
                    return True
                else:
                    logger.warning(f"Data quality issues in {table_name}: {quality_issues}")
                    return False
            else:
                logger.error(f"No data found for quality check in {table_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error checking data quality: {e}")
            return False
