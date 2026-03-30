#!/usr/bin/env python3
"""
Schema Drift Checker for Traffic Platform
"""

import os
import logging
import snowflake.connector
from datetime import datetime
import boto3
import json

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

class SchemaDriftChecker:
    """Checks for schema drift in Snowflake tables"""
    
    def __init__(self):
        self.snowflake_conn = None
        self.cloudwatch = CLOUDWATCH_CLIENT
        
        # Expected schemas (defined in code)
        self.expected_schemas = {
            'RAW.TRAFFIC_EVENTS': {
                'sensor_id': 'VARCHAR(50)',
                'timestamp': 'TIMESTAMP_NTZ',
                'traffic_volume': 'INTEGER',
                'speed_mph': 'FLOAT',
                'occupancy_pct': 'FLOAT',
                'road_name': 'VARCHAR(100)',
                'direction': 'VARCHAR(10)',
                'latitude': 'DECIMAL(10,6)',
                'longitude': 'DECIMAL(10,6)',
                'year': 'INTEGER',
                'month': 'INTEGER',
                'day': 'INTEGER',
                'etl_loaded_at': 'TIMESTAMP_NTZ'
            },
            'ANALYTICS.MART_HOURLY_TRAFFIC': {
                'surrogate_key': 'VARCHAR',
                'sensor_id': 'VARCHAR(50)',
                'reading_timestamp': 'TIMESTAMP_NTZ',
                'traffic_volume': 'INTEGER',
                'speed_mph': 'FLOAT',
                'occupancy_pct': 'FLOAT',
                'road_name': 'VARCHAR(100)',
                'direction': 'VARCHAR(10)',
                'latitude': 'DECIMAL(10,6)',
                'longitude': 'DECIMAL(10,6)',
                'hour_of_day': 'INTEGER',
                'day_of_week': 'INTEGER',
                'is_weekend': 'BOOLEAN',
                'is_rush_hour': 'BOOLEAN',
                'month': 'INTEGER',
                'year': 'INTEGER',
                'is_holiday': 'BOOLEAN',
                'holiday_name': 'VARCHAR(100)',
                'city': 'VARCHAR(50)',
                'sensor_road_name': 'VARCHAR(100)',
                'rolling_avg_3hr': 'FLOAT',
                'rolling_avg_24hr': 'FLOAT',
                'volume_pct_change': 'FLOAT',
                'traffic_level': 'VARCHAR(20)',
                'etl_loaded_at': 'TIMESTAMP_NTZ'
            }
        }
    
    def get_snowflake_connection(self):
        """Get Snowflake connection"""
        if not self.snowflake_conn:
            self.snowflake_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        return self.snowflake_conn
    
    def get_current_schema(self, table_name):
        """Get current schema from Snowflake"""
        logger.info(f"Getting current schema for {table_name}")
        
        conn = self.get_snowflake_connection()
        cursor = conn.cursor()
        
        try:
            query = """
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = PARSE_IDENTIFIER(PARSE_IDENTIFIER(%s, 'SCHEMA'))
                  AND TABLE_NAME = PARSE_IDENTIFIER(PARSE_IDENTIFIER(%s, 'TABLE'))
                ORDER BY ORDINAL_POSITION
            """
            
            cursor.execute(query, (table_name, table_name))
            results = cursor.fetchall()
            
            current_schema = {}
            for row in results:
                (col_name, data_type, max_length, precision, scale, is_nullable, 
                 default_val, ordinal_pos) = row
                
                # Build full data type string
                full_type = data_type
                if data_type in ['VARCHAR', 'CHAR'] and max_length:
                    full_type = f"{data_type}({max_length})"
                elif data_type in ['DECIMAL', 'NUMBER'] and precision:
                    full_type = f"{data_type}({precision},{scale or 0})"
                
                current_schema[col_name] = {
                    'data_type': full_type,
                    'is_nullable': is_nullable,
                    'default_value': default_val,
                    'ordinal_position': ordinal_pos
                }
            
            return current_schema
            
        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {e}")
            return {}
        finally:
            cursor.close()
    
    def compare_schemas(self, table_name, current_schema, expected_schema):
        """Compare current schema with expected schema"""
        logger.info(f"Comparing schemas for {table_name}")
        
        drift_analysis = {
            'table_name': table_name,
            'new_columns': [],
            'removed_columns': [],
            'type_changes': [],
            'nullability_changes': [],
            'total_changes': 0
        }
        
        # Check for new columns
        for col_name in current_schema:
            if col_name not in expected_schema:
                drift_analysis['new_columns'].append({
                    'column': col_name,
                    'current_type': current_schema[col_name]['data_type']
                })
                logger.info(f"New column detected: {col_name}")
        
        # Check for removed columns
        for col_name in expected_schema:
            if col_name not in current_schema:
                drift_analysis['removed_columns'].append({
                    'column': col_name,
                    'expected_type': expected_schema[col_name]
                })
                logger.warning(f"Column removed: {col_name}")
        
        # Check for type changes
        for col_name in expected_schema:
            if col_name in current_schema:
                current_type = current_schema[col_name]['data_type']
                expected_type = expected_schema[col_name]
                
                # Normalize types for comparison
                current_normalized = self.normalize_type(current_type)
                expected_normalized = self.normalize_type(expected_type)
                
                if current_normalized != expected_normalized:
                    drift_analysis['type_changes'].append({
                        'column': col_name,
                        'expected_type': expected_type,
                        'current_type': current_type
                    })
                    logger.warning(f"Type change in {col_name}: {expected_type} -> {current_type}")
        
        # Calculate total changes
        drift_analysis['total_changes'] = (
            len(drift_analysis['new_columns']) +
            len(drift_analysis['removed_columns']) +
            len(drift_analysis['type_changes'])
        )
        
        return drift_analysis
    
    def normalize_type(self, data_type):
        """Normalize data type for comparison"""
        # Remove whitespace and convert to uppercase
        normalized = data_type.upper().strip()
        
        # Handle common variations
        replacements = {
            'INTEGER': 'INT',
            'NUMBER': 'DECIMAL',
            'DOUBLE': 'FLOAT',
            'TIMESTAMP': 'TIMESTAMP_NTZ',
            'VARCHAR2': 'VARCHAR'
        }
        
        for old, new in replacements.items():
            if normalized.startswith(old):
                normalized = normalized.replace(old, new)
        
        return normalized
    
    def check_all_tables(self):
        """Check schema drift for all important tables"""
        logger.info("Starting comprehensive schema drift check")
        
        drift_results = []
        total_changes = 0
        
        for table_name in self.expected_schemas.keys():
            # Get current schema
            current_schema = self.get_current_schema(table_name)
            
            if not current_schema:
                logger.error(f"Could not retrieve schema for {table_name}")
                continue
            
            # Compare with expected
            drift_analysis = self.compare_schemas(
                table_name, 
                current_schema, 
                self.expected_schemas[table_name]
            )
            
            drift_results.append(drift_analysis)
            total_changes += drift_analysis['total_changes']
        
        # Log overall results
        logger.info(f"Schema Drift Summary:")
        logger.info(f"  Tables checked: {len(drift_results)}")
        logger.info(f"  Total changes detected: {total_changes}")
        
        for result in drift_results:
            if result['total_changes'] > 0:
                logger.warning(f"Table {result['table_name']}: {result['total_changes']} changes")
        
        # Publish metrics to CloudWatch
        self.publish_drift_metrics(drift_results, total_changes)
        
        return {
            'tables_checked': len(drift_results),
            'total_changes': total_changes,
            'drift_results': drift_results,
            'check_timestamp': datetime.now().isoformat()
        }
    
    def publish_drift_metrics(self, drift_results, total_changes):
        """Publish schema drift metrics to CloudWatch"""
        try:
            tables_with_changes = len([r for r in drift_results if r['total_changes'] > 0])
            new_columns = sum(len(r['new_columns']) for r in drift_results)
            removed_columns = sum(len(r['removed_columns']) for r in drift_results)
            type_changes = sum(len(r['type_changes']) for r in drift_results)
            
            metrics = [
                {
                    'MetricName': 'SchemaDriftTotalChanges',
                    'Value': total_changes,
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'SchemaDriftTablesAffected',
                    'Value': tables_with_changes,
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'SchemaDriftNewColumns',
                    'Value': new_columns,
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'SchemaDriftRemovedColumns',
                    'Value': removed_columns,
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'SchemaDriftTypeChanges',
                    'Value': type_changes,
                    'Unit': 'Count'
                }
            ]
            
            self.cloudwatch.put_metric_data(
                Namespace='TrafficPlatform/Observability',
                MetricData=metrics
            )
            
            logger.info("Published schema drift metrics to CloudWatch")
            
        except Exception as e:
            logger.error(f"Error publishing drift metrics: {e}")
    
    def generate_drift_report(self, drift_results):
        """Generate detailed drift report"""
        report = {
            'summary': {
                'total_tables': len(drift_results),
                'tables_with_changes': len([r for r in drift_results if r['total_changes'] > 0]),
                'total_changes': sum(r['total_changes'] for r in drift_results)
            },
            'details': drift_results,
            'timestamp': datetime.now().isoformat()
        }
        
        # Save report to S3 (optional)
        try:
            s3 = boto3.client('s3')
            s3.put_object(
                Bucket='traffic-platform-lake',
                Key=f'observability/schema-drift-report-{datetime.now().strftime("%Y%m%d-%H%M%S")}.json',
                Body=json.dumps(report, indent=2, default=str),
                ContentType='application/json'
            )
            logger.info("Schema drift report saved to S3")
        except Exception as e:
            logger.error(f"Error saving report to S3: {e}")
        
        return report
    
    def run_schema_drift_check(self):
        """Run complete schema drift check"""
        logger.info("Starting schema drift checker")
        
        try:
            # Check all tables
            drift_results = self.check_all_tables()
            
            # Generate report
            report = self.generate_drift_report(drift_results)
            
            logger.info("Schema drift check completed successfully")
            return report
            
        except Exception as e:
            logger.error(f"Error in schema drift check: {e}")
            return None
    
    def close(self):
        """Close connections"""
        if self.snowflake_conn:
            self.snowflake_conn.close()

def main():
    """Main schema drift checker function"""
    logger.info("Starting Schema Drift Checker")
    
    checker = SchemaDriftChecker()
    
    try:
        # Run schema drift check
        report = checker.run_schema_drift_check()
        
        # Log summary
        if report:
            logger.info(f"Schema drift check completed at: {report['timestamp']}")
            logger.info(f"Summary: {report['summary']}")
        
    except Exception as e:
        logger.error(f"Error in schema drift checker: {e}")
    finally:
        checker.close()

if __name__ == "__main__":
    main()
