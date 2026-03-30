#!/usr/bin/env python3
"""
Delta Lake Maintenance Operations
"""

import os
import logging
from pyspark.sql import SparkSession, functions as F
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
S3_PATH = os.getenv('S3_PATH', 's3://traffic-platform-lake/bronze/traffic_events/')
VACUUM_HOURS = int(os.getenv('VACUUM_HOURS', '168'))  # 7 days
OPTIMIZE_ZORDER_COLS = os.getenv('OPTIMIZE_ZORDER_COLS', 'sensor_id,timestamp')

class DeltaMaintenance:
    """Manages Delta Lake maintenance operations"""
    
    def __init__(self, spark):
        self.spark = spark
        self.delta_table_path = S3_PATH
        self.vacuum_hours = VACUUM_HOURS
        self.zorder_cols = OPTIMIZE_ZORDER_COLS
    
    def vacuum_table(self):
        """Remove old files to manage storage costs"""
        logger.info(f"Vacuuming Delta table with {self.vacuum_hours} hours retention")
        
        try:
            # Read Delta table
            delta_table = self.spark.read.format("delta").load(self.delta_table_path)
            
            # Get table size before vacuum
            size_before = delta_table.rdd.map(lambda row: len(str(row))).sum()
            logger.info(f"Table size before vacuum: {size_before} characters")
            
            # Perform vacuum
            delta_table.vacuum(retention_hours=self.vacuum_hours)
            
            # Get table size after vacuum
            size_after = delta_table.rdd.map(lambda row: len(str(row))).sum()
            logger.info(f"Table size after vacuum: {size_after} characters")
            
            logger.info("Vacuum completed successfully")
            
        except Exception as e:
            logger.error(f"Vacuum failed: {e}")
    
    def optimize_table(self):
        """Compact small files and optimize for query performance"""
        logger.info(f"Optimizing Delta table with Z-order columns: {self.zorder_cols}")
        
        try:
            # Read Delta table
            delta_table = self.spark.read.format("delta").load(self.delta_table_path)
            
            # Create temp view for optimization
            delta_table.createOrReplaceTempView("traffic_events_optimize")
            
            # Get file count before optimization
            file_count_before = delta_table.rdd.count()
            logger.info(f"File count before optimization: {file_count_before}")
            
            # Run OPTIMIZE command
            optimize_sql = f"""
                OPTIMIZE delta.`{self.delta_table_path}`
                ZORDER BY ({self.zorder_cols})
            """
            
            self.spark.sql(optimize_sql)
            
            # Get file count after optimization
            file_count_after = self.spark.read.format("delta").load(self.delta_table_path).rdd.count()
            logger.info(f"File count after optimization: {file_count_after}")
            
            logger.info("Optimization completed successfully")
            
        except Exception as e:
            logger.error(f"Optimization failed: {e}")
    
    def analyze_table_size(self):
        """Analyze Delta table size and performance"""
        logger.info("Analyzing Delta table size and performance")
        
        try:
            # Read Delta table
            delta_table = self.spark.read.format("delta").load(self.delta_table_path)
            
            # Get basic statistics
            stats = delta_table.describe().toPandas()
            
            # Get file statistics
            delta_table.createOrReplaceTempView("traffic_events_stats")
            
            file_stats = self.spark.sql(f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT sensor_id) as unique_sensors,
                    MIN(timestamp) as earliest_timestamp,
                    MAX(timestamp) as latest_timestamp,
                    COUNT(DISTINCT DATE(timestamp)) as unique_dates,
                    SUM(traffic_volume) as total_volume,
                    AVG(traffic_volume) as avg_volume,
                    MAX(traffic_volume) as max_volume,
                    MIN(traffic_volume) as min_volume
                FROM traffic_events_stats
            """).collect()[0]
            
            logger.info("Delta Table Analysis:")
            logger.info(f"  Total records: {stats['total_records']:,}")
            logger.info(f"  Unique sensors: {stats['unique_sensors']}")
            logger.info(f"  Date range: {stats['earliest_timestamp']} to {stats['latest_timestamp']}")
            logger.info(f"  Unique dates: {stats['unique_dates']}")
            logger.info(f"  Total volume: {stats['total_volume']:,}")
            logger.info(f"  Average volume: {stats['avg_volume']:.2f}")
            logger.info(f"  Max volume: {stats['max_volume']}")
            logger.info(f"  Min volume: {stats['min_volume']}")
            
            return stats
            
        except Exception as e:
            logger.error(f"Analysis failed: {e}")
            return None
    
    def get_table_history(self, limit=10):
        """Get Delta table history for audit"""
        logger.info(f"Getting Delta table history (last {limit} operations)")
        
        try:
            # Read Delta table
            delta_table = self.spark.read.format("delta").load(self.delta_table_path)
            
            # Get history
            history = delta_table.history(limit)
            
            logger.info("Delta Table History:")
            for operation in history:
                logger.info(f"  Version: {operation['version']}")
                logger.info(f"  Timestamp: {operation['timestamp']}")
                logger.info(f"  Operation: {operation['operation']}")
                logger.info(f"  Parameters: {operation['operationParameters']}")
                logger.info(f"  User: {operation['userMetadata']}")
                logger.info("")
            
            return history
            
        except Exception as e:
            logger.error(f"Failed to get history: {e}")
            return None
    
    def run_maintenance(self):
        """Run complete maintenance routine"""
        logger.info("Starting Delta Lake maintenance routine")
        
        try:
            # Analyze table
            analysis = self.analyze_table_size()
            
            # Optimize table
            self.optimize_table()
            
            # Vacuum table
            self.vacuum_table()
            
            # Get history
            history = self.get_table_history(limit=5)
            
            logger.info("Delta Lake maintenance completed successfully")
            
        except Exception as e:
            logger.error(f"Maintenance failed: {e}")

def create_spark_session():
    """Create Spark session with Delta Lake support"""
    spark = SparkSession.builder \
        .appName("DeltaMaintenance") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .config("spark.databricks.delta.autoCompact.enabled", "true") \
        .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()
    
    return spark

def main():
    """Main maintenance function"""
    logger.info("Starting Delta Lake Maintenance")
    logger.info(f"Delta table path: {S3_PATH}")
    logger.info(f"Vacuum retention: {VACUUM_HOURS} hours")
    logger.info(f"Z-order columns: {OPTIMIZE_ZORDER_COLS}")
    
    # Create Spark session
    spark = create_spark_session()
    
    # Create maintenance handler
    maintenance = DeltaMaintenance(spark)
    
    try:
        # Run maintenance
        maintenance.run_maintenance()
        
        logger.info("Delta Lake Maintenance initialized successfully")
        
    except Exception as e:
        logger.error(f"Error in Delta Lake Maintenance: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
