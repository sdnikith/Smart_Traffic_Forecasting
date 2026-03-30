#!/usr/bin/env python3
"""
Traffic Data Freshness Monitor
Checks how fresh our data is across the platform
"""

import os
import logging
import snowflake.connector
from datetime import datetime, timedelta
import boto3
import json

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Snowflake config
SNOWFLAKE_CONFIG = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': 'TRAFFIC_ANALYTICS_WH',
    'database': 'TRAFFIC_DB',
    'schema': 'ANALYTICS'
}

CLOUDWATCH_CLIENT = boto3.client('cloudwatch', region_name='us-east-1')

class FreshnessMonitor:
    """Monitors data freshness across traffic platform"""
    
    def __init__(self):
        self.snowflake_conn = None
        self.cloudwatch = CLOUDWATCH_CLIENT
        
    def get_snowflake_connection(self):
        """Get Snowflake connection"""
        if not self.snowflake_conn:
            self.snowflake_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        return self.snowflake_conn
    
    def check_sensor_freshness(self):
        """Check freshness of sensor data"""
        logger.info("Checking sensor data freshness")
        
        conn = self.get_snowflake_connection()
        cursor = conn.cursor()
        
        try:
            # Query for latest sensor readings
            query = """
                SELECT 
                    sensor_id,
                    MAX(reading_timestamp) AS last_reading,
                    CURRENT_TIMESTAMP() AS check_time,
                    DATEDIFF('minute', MAX(reading_timestamp), CURRENT_TIMESTAMP()) AS minutes_since_last_reading,
                    CASE 
                        WHEN DATEDIFF('minute', MAX(reading_timestamp), CURRENT_TIMESTAMP()) > 120 
                             AND EXTRACT('hour', CURRENT_TIMESTAMP()) BETWEEN 6 AND 22
                        THEN 'stale'
                        WHEN DATEDIFF('minute', MAX(reading_timestamp), CURRENT_TIMESTAMP()) > 360
                        THEN 'critical'
                        ELSE 'fresh'
                    END AS freshness_status
                FROM ANALYTICS.MART_HOURLY_TRAFFIC
                WHERE reading_timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP())
                GROUP BY sensor_id
                ORDER BY minutes_since_last_reading DESC
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            # Analyze freshness
            fresh_sensors = 0
            stale_sensors = 0
            critical_sensors = 0
            
            for row in results:
                sensor_id, last_reading, check_time, minutes_ago, status = row
                
                if status == 'fresh':
                    fresh_sensors += 1
                elif status == 'stale':
                    stale_sensors += 1
                    logger.warning(f"Stale sensor: {sensor_id}, last reading: {last_reading}, {minutes_ago} minutes ago")
                elif status == 'critical':
                    critical_sensors += 1
                    logger.error(f"Critical sensor: {sensor_id}, last reading: {last_reading}, {minutes_ago} minutes ago")
            
            total_sensors = len(results)
            freshness_score = (fresh_sensors / total_sensors) * 100 if total_sensors > 0 else 0
            
            # Log overall freshness
            logger.info(f"Freshness Summary:")
            logger.info(f"  Total sensors: {total_sensors}")
            logger.info(f"  Fresh sensors: {fresh_sensors}")
            logger.info(f"  Stale sensors: {stale_sensors}")
            logger.info(f"  Critical sensors: {critical_sensors}")
            logger.info(f"  Overall freshness score: {freshness_score:.2f}%")
            
            # Publish to CloudWatch
            self.publish_freshness_metrics(fresh_sensors, stale_sensors, critical_sensors, freshness_score)
            
            return {
                'total_sensors': total_sensors,
                'fresh_sensors': fresh_sensors,
                'stale_sensors': stale_sensors,
                'critical_sensors': critical_sensors,
                'freshness_score': freshness_score,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error checking sensor freshness: {e}")
            return None
        finally:
            cursor.close()
    
    def check_weather_freshness(self):
        """Check freshness of weather data"""
        logger.info("Checking weather data freshness")
        
        conn = self.get_snowflake_connection()
        cursor = conn.cursor()
        
        try:
            query = """
                SELECT 
                    MAX(weather_timestamp) AS last_weather_update,
                    CURRENT_TIMESTAMP() AS check_time,
                    DATEDIFF('minute', MAX(weather_timestamp), CURRENT_TIMESTAMP()) AS minutes_since_last_update,
                    CASE 
                        WHEN DATEDIFF('minute', MAX(weather_timestamp), CURRENT_TIMESTAMP()) > 30
                        THEN 'stale'
                        ELSE 'fresh'
                    END AS freshness_status
                FROM RAW.WEATHER_UPDATES
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            if result:
                last_update, check_time, minutes_ago, status = result
                
                logger.info(f"Weather Freshness:")
                logger.info(f"  Last update: {last_update}")
                logger.info(f"  Minutes since last update: {minutes_ago}")
                logger.info(f"  Status: {status}")
                
                # Publish to CloudWatch
                self.publish_weather_freshness(minutes_ago, status)
                
                return {
                    'last_weather_update': last_update.isoformat() if last_update else None,
                    'minutes_since_last_update': minutes_ago,
                    'freshness_status': status,
                    'timestamp': datetime.now().isoformat()
                }
            
        except Exception as e:
            logger.error(f"Error checking weather freshness: {e}")
            return None
        finally:
            cursor.close()
    
    def publish_freshness_metrics(self, fresh_sensors, stale_sensors, critical_sensors, freshness_score):
        """Publish freshness metrics to CloudWatch"""
        try:
            metrics = [
                {
                    'MetricName': 'FreshSensors',
                    'Value': fresh_sensors,
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'StaleSensors',
                    'Value': stale_sensors,
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'CriticalSensors',
                    'Value': critical_sensors,
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'FreshnessScore',
                    'Value': freshness_score,
                    'Unit': 'Percent'
                }
            ]
            
            self.cloudwatch.put_metric_data(
                Namespace='TrafficPlatform/Observability',
                MetricData=metrics
            )
            
            logger.info("Published freshness metrics to CloudWatch")
            
        except Exception as e:
            logger.error(f"Error publishing freshness metrics: {e}")
    
    def publish_weather_freshness(self, minutes_ago, status):
        """Publish weather freshness metrics to CloudWatch"""
        try:
            metrics = [
                {
                    'MetricName': 'WeatherDataAgeMinutes',
                    'Value': minutes_ago,
                    'Unit': 'Minutes'
                },
                {
                    'MetricName': 'WeatherDataFresh',
                    'Value': 1 if status == 'fresh' else 0,
                    'Unit': 'Count'
                }
            ]
            
            self.cloudwatch.put_metric_data(
                Namespace='TrafficPlatform/Observability',
                MetricData=metrics
            )
            
            logger.info("Published weather freshness metrics to CloudWatch")
            
        except Exception as e:
            logger.error(f"Error publishing weather freshness metrics: {e}")
    
    def run_freshness_check(self):
        """Run complete freshness check"""
        logger.info("Starting data freshness monitoring")
        
        # Check sensor freshness
        sensor_freshness = self.check_sensor_freshness()
        
        # Check weather freshness
        weather_freshness = self.check_weather_freshness()
        
        # Create freshness report
        report = {
            'sensor_freshness': sensor_freshness,
            'weather_freshness': weather_freshness,
            'check_timestamp': datetime.now().isoformat()
        }
        
        return report
    
    def close(self):
        """Close connections"""
        if self.snowflake_conn:
            self.snowflake_conn.close()

def main():
    """Main freshness monitor function"""
    logger.info("Starting Data Freshness Monitor")
    
    monitor = FreshnessMonitor()
    
    try:
        # Run freshness check
        report = monitor.run_freshness_check()
        
        # Log summary
        if report:
            logger.info("Freshness monitoring completed successfully")
            logger.info(f"Report generated at: {report['check_timestamp']}")
        
    except Exception as e:
        logger.error(f"Error in freshness monitoring: {e}")
    finally:
        monitor.close()

if __name__ == "__main__":
    main()
