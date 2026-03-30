#!/usr/bin/env python3
"""
Volume Anomaly Detector for Traffic Platform
"""

import os
import logging
import snowflake.connector
from datetime import datetime, timedelta
import boto3
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
SNOWFLAKE_CONFIG = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': 'TRAFFIC_ANALYTICS_WH',
    'database': 'TRAFFIC_DB',
    'schema': 'ANALYTICS'
}

CLOUDWATCH_CLIENT = boto3.client('cloudwatch', region_name='us-east-1')

class VolumeAnomalyDetector:
    """Detects volume anomalies in traffic data"""
    
    def __init__(self):
        self.snowflake_conn = None
        self.cloudwatch = CLOUDWATCH_CLIENT
        
    def get_snowflake_connection(self):
        """Get Snowflake connection"""
        if not self.snowflake_conn:
            self.snowflake_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        return self.snowflake_conn
    
    def check_volume_anomalies(self):
        """Check for volume anomalies by comparing today vs 7-day average"""
        logger.info("Checking volume anomalies")
        
        conn = self.get_snowflake_connection()
        cursor = conn.cursor()
        
        try:
            # Query for daily volume comparison
            query = """
                WITH daily_volumes AS (
                    SELECT 
                        sensor_id,
                        reading_timestamp::DATE AS traffic_date,
                        COUNT(*) AS daily_readings,
                        AVG(traffic_volume) AS avg_volume,
                        MAX(traffic_volume) AS max_volume,
                        MIN(traffic_volume) AS min_volume
                    FROM ANALYTICS.MART_HOURLY_TRAFFIC
                    WHERE reading_timestamp >= DATEADD('day', -14, CURRENT_TIMESTAMP())
                    GROUP BY sensor_id, reading_timestamp::DATE
                ),
                rolling_stats AS (
                    SELECT 
                        sensor_id,
                        traffic_date,
                        daily_readings,
                        avg_volume,
                        max_volume,
                        min_volume,
                        AVG(avg_volume) OVER (
                            PARTITION BY sensor_id 
                            ORDER BY traffic_date 
                            ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
                        ) AS rolling_7day_avg,
                        STDDEV(avg_volume) OVER (
                            PARTITION BY sensor_id 
                            ORDER BY traffic_date 
                            ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
                        ) AS rolling_7day_std
                    FROM daily_volumes
                )
                SELECT 
                    sensor_id,
                    traffic_date,
                    daily_readings,
                    avg_volume,
                    max_volume,
                    min_volume,
                    rolling_7day_avg,
                    rolling_7day_std,
                    CASE 
                        WHEN rolling_7day_avg IS NULL THEN 'insufficient_data'
                        WHEN daily_readings < (rolling_7day_avg * 0.8) THEN 'volume_drop'
                        WHEN daily_readings > (rolling_7day_avg * 1.5) THEN 'volume_spike'
                        ELSE 'normal'
                    END AS anomaly_type,
                    CASE 
                        WHEN rolling_7day_avg > 0 
                        THEN ((daily_readings - rolling_7day_avg) / rolling_7day_avg) * 100
                        ELSE 0
                    END AS volume_pct_change,
                    CASE 
                        WHEN rolling_7day_std > 0 
                        THEN ABS(avg_volume - rolling_7day_avg) / rolling_7day_std
                        ELSE 0
                    END AS z_score
                FROM rolling_stats
                WHERE traffic_date >= DATEADD('day', -7, CURRENT_TIMESTAMP())
                ORDER BY traffic_date DESC, sensor_id
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            # Analyze anomalies
            anomalies = []
            normal_count = 0
            
            for row in results:
                (sensor_id, traffic_date, daily_readings, avg_volume, max_volume, min_volume,
                 rolling_avg, rolling_std, anomaly_type, pct_change, z_score) = row
                
                if anomaly_type in ['volume_drop', 'volume_spike']:
                    anomalies.append({
                        'sensor_id': sensor_id,
                        'traffic_date': traffic_date.isoformat() if traffic_date else None,
                        'anomaly_type': anomaly_type,
                        'daily_readings': daily_readings,
                        'avg_volume': avg_volume,
                        'rolling_7day_avg': rolling_avg,
                        'volume_pct_change': pct_change,
                        'z_score': z_score,
                        'severity': 'high' if abs(z_score) > 3 else 'medium'
                    })
                    
                    if anomaly_type == 'volume_drop':
                        logger.warning(f"Volume drop: sensor {sensor_id}, expected ~{rolling_avg:.0f}, got {daily_readings}")
                    elif anomaly_type == 'volume_spike':
                        logger.warning(f"Volume spike: sensor {sensor_id}, expected ~{rolling_avg:.0f}, got {daily_readings}")
                else:
                    normal_count += 1
            
            # Log anomaly summary
            total_records = len(results)
            anomaly_count = len(anomalies)
            
            logger.info(f"Volume Anomaly Summary:")
            logger.info(f"  Total records analyzed: {total_records}")
            logger.info(f"  Normal records: {normal_count}")
            logger.info(f"  Anomalies detected: {anomaly_count}")
            logger.info(f"  Anomaly rate: {(anomaly_count/total_records)*100:.2f}%")
            
            # Publish metrics to CloudWatch
            self.publish_anomaly_metrics(anomalies, total_records)
            
            return {
                'total_records': total_records,
                'normal_records': normal_count,
                'anomalies': anomalies,
                'anomaly_count': anomaly_count,
                'anomaly_rate': (anomaly_count/total_records)*100 if total_records > 0 else 0,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error checking volume anomalies: {e}")
            return None
        finally:
            cursor.close()
    
    def check_sensor_health_anomalies(self):
        """Check for sensor health anomalies"""
        logger.info("Checking sensor health anomalies")
        
        conn = self.get_snowflake_connection()
        cursor = conn.cursor()
        
        try:
            query = """
                SELECT 
                    sensor_id,
                    COUNT(*) AS total_readings,
                    COUNT(CASE WHEN traffic_volume = 0 THEN 1 END) AS zero_volume_count,
                    COUNT(CASE WHEN traffic_volume > 8000 THEN 1 END) AS extreme_volume_count,
                    AVG(traffic_volume) AS avg_volume,
                    STDDEV(traffic_volume) AS std_volume,
                    MIN(reading_timestamp) AS first_reading,
                    MAX(reading_timestamp) AS last_reading,
                    CASE 
                        WHEN COUNT(CASE WHEN traffic_volume = 0 THEN 1 END) > COUNT(*) * 0.5 THEN 'malfunctioning'
                        WHEN COUNT(CASE WHEN traffic_volume > 8000 THEN 1 END) > COUNT(*) * 0.1 THEN 'suspicious'
                        WHEN STDDEV(traffic_volume) / AVG(traffic_volume) > 2 THEN 'unstable'
                        ELSE 'healthy'
                    END AS health_status
                FROM ANALYTICS.MART_HOURLY_TRAFFIC
                WHERE reading_timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP())
                GROUP BY sensor_id
                HAVING COUNT(*) >= 24  -- At least 24 readings in last week
                ORDER BY health_status DESC, zero_volume_count DESC
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            # Analyze sensor health
            healthy_sensors = 0
            malfunctioning_sensors = 0
            suspicious_sensors = 0
            unstable_sensors = 0
            
            for row in results:
                (sensor_id, total_readings, zero_count, extreme_count, avg_vol, std_vol,
                 first_reading, last_reading, health_status) = row
                
                if health_status == 'healthy':
                    healthy_sensors += 1
                elif health_status == 'malfunctioning':
                    malfunctioning_sensors += 1
                    logger.error(f"Malfunctioning sensor: {sensor_id}, {zero_count}/{total_readings} zero readings")
                elif health_status == 'suspicious':
                    suspicious_sensors += 1
                    logger.warning(f"Suspicious sensor: {sensor_id}, {extreme_count}/{total_readings} extreme readings")
                elif health_status == 'unstable':
                    unstable_sensors += 1
                    logger.warning(f"Unstable sensor: {sensor_id}, high volatility detected")
            
            # Log sensor health summary
            total_sensors = len(results)
            
            logger.info(f"Sensor Health Summary:")
            logger.info(f"  Total sensors: {total_sensors}")
            logger.info(f"  Healthy sensors: {healthy_sensors}")
            logger.info(f"  Malfunctioning sensors: {malfunctioning_sensors}")
            logger.info(f"  Suspicious sensors: {suspicious_sensors}")
            logger.info(f"  Unstable sensors: {unstable_sensors}")
            
            # Publish health metrics
            self.publish_health_metrics(healthy_sensors, malfunctioning_sensors, 
                                   suspicious_sensors, unstable_sensors)
            
            return {
                'total_sensors': total_sensors,
                'healthy_sensors': healthy_sensors,
                'malfunctioning_sensors': malfunctioning_sensors,
                'suspicious_sensors': suspicious_sensors,
                'unstable_sensors': unstable_sensors,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error checking sensor health: {e}")
            return None
        finally:
            cursor.close()
    
    def publish_anomaly_metrics(self, anomalies, total_records):
        """Publish anomaly metrics to CloudWatch"""
        try:
            volume_drops = len([a for a in anomalies if a['anomaly_type'] == 'volume_drop'])
            volume_spikes = len([a for a in anomalies if a['anomaly_type'] == 'volume_spike'])
            
            metrics = [
                {
                    'MetricName': 'VolumeAnomalies',
                    'Value': len(anomalies),
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'VolumeDrops',
                    'Value': volume_drops,
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'VolumeSpikes',
                    'Value': volume_spikes,
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'AnomalyRate',
                    'Value': (len(anomalies)/total_records)*100 if total_records > 0 else 0,
                    'Unit': 'Percent'
                }
            ]
            
            self.cloudwatch.put_metric_data(
                Namespace='TrafficPlatform/Observability',
                MetricData=metrics
            )
            
            logger.info("Published anomaly metrics to CloudWatch")
            
        except Exception as e:
            logger.error(f"Error publishing anomaly metrics: {e}")
    
    def publish_health_metrics(self, healthy, malfunctioning, suspicious, unstable):
        """Publish sensor health metrics to CloudWatch"""
        try:
            metrics = [
                {
                    'MetricName': 'HealthySensors',
                    'Value': healthy,
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'MalfunctioningSensors',
                    'Value': malfunctioning,
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'SuspiciousSensors',
                    'Value': suspicious,
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'UnstableSensors',
                    'Value': unstable,
                    'Unit': 'Count'
                }
            ]
            
            self.cloudwatch.put_metric_data(
                Namespace='TrafficPlatform/Observability',
                MetricData=metrics
            )
            
            logger.info("Published health metrics to CloudWatch")
            
        except Exception as e:
            logger.error(f"Error publishing health metrics: {e}")
    
    def run_anomaly_detection(self):
        """Run complete anomaly detection"""
        logger.info("Starting volume anomaly detection")
        
        # Check volume anomalies
        volume_anomalies = self.check_volume_anomalies()
        
        # Check sensor health
        sensor_health = self.check_sensor_health_anomalies()
        
        # Create comprehensive report
        report = {
            'volume_anomalies': volume_anomalies,
            'sensor_health': sensor_health,
            'check_timestamp': datetime.now().isoformat()
        }
        
        return report
    
    def close(self):
        """Close connections"""
        if self.snowflake_conn:
            self.snowflake_conn.close()

def main():
    """Main anomaly detector function"""
    logger.info("Starting Volume Anomaly Detector")
    
    detector = VolumeAnomalyDetector()
    
    try:
        # Run anomaly detection
        report = detector.run_anomaly_detection()
        
        # Log summary
        if report:
            logger.info("Anomaly detection completed successfully")
            logger.info(f"Report generated at: {report['check_timestamp']}")
        
    except Exception as e:
        logger.error(f"Error in anomaly detection: {e}")
    finally:
        detector.close()

if __name__ == "__main__":
    main()
