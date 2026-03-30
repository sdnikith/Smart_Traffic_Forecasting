#!/usr/bin/env python3
"""
Delta Lake Traffic Data Writer
Handles writing traffic data to Delta Lake tables
"""

import os
import logging
from pyspark.sql import SparkSession, functions as F
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Config
S3_PATH = os.getenv('S3_PATH', 's3://traffic-platform-lake/bronze/traffic_events/')
CHECKPOINT_LOCATION = os.getenv('CHECKPOINT_LOCATION', 's3://traffic-platform-lake/checkpoints/delta-writer/')

class DeltaLakeWriter:
    """Manages Delta Lake writes for traffic data"""
    
    def __init__(self, spark):
        self.spark = spark
        self.delta_table_path = S3_PATH
        
    def write_micro_batches(self, df):
        """Write micro-batches to Delta Lake"""
        logger.info(f"Writing {df.count()} records to Delta Lake at {self.delta_table_path}")
        
        # Setup Delta write
        df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", CHECKPOINT_LOCATION) \
            .option("mergeSchema", "true") \
            .option("autoCompact", "true") \
            .option("optimizeWrite", "true") \
            .partitionBy("year", "month", "day") \
            .trigger(processingTime="5 minutes") \
            .start(self.delta_table_path)
    
    def vacuum_table(self, retention_days=7):
        """Vacuum old files to manage storage costs"""
        logger.info(f"Vacuuming Delta table with {retention_days} days retention")
        
        try:
            # Read Delta table
            delta_table = self.spark.read.format("delta").load(self.delta_table_path)
            
            # Perform vacuum
            delta_table.vacuum(retention_hours=retention_days * 24)
            logger.info("Vacuum completed successfully")
            
        except Exception as e:
            logger.error(f"Vacuum failed: {e}")
    
    def optimize_table(self):
        """Optimize Delta table for better query performance"""
        logger.info("Optimizing Delta table")
        
        try:
            # Read Delta table
            delta_table = self.spark.read.format("delta").load(self.delta_table_path)
            
            # Optimize with Z-ordering
            delta_table.createOrReplaceTempView("traffic_events_temp")
            
            self.spark.sql(f"""
                OPTIMIZE delta.`{self.delta_table_path}`
                ZORDER BY (sensor_id, timestamp)
            """)
            
            logger.info("Optimization completed successfully")
            
        except Exception as e:
            logger.error(f"Optimization failed: {e}")
    
    def get_table_history(self):
        """Get Delta table history for audit"""
        logger.info("Getting Delta table history")
        
        try:
            # Read Delta table
            delta_table = self.spark.read.format("delta").load(self.delta_table_path)
            
            # Get history
            history = delta_table.history(10)  # Last 10 operations
            history.show(truncate=False)
            
            return history
            
        except Exception as e:
            logger.error(f"Failed to get history: {e}")
            return None
    
    def time_travel_query(self, timestamp_str):
        """Time travel query to specific timestamp"""
        logger.info(f"Time travel query to timestamp: {timestamp_str}")
        
        try:
            # Read Delta table at specific timestamp
            delta_table = self.spark.read.format("delta") \
                .option("timestampAsOf", timestamp_str) \
                .load(self.delta_table_path)
            
            logger.info(f"Time travel query returned {delta_table.count()} records")
            return delta_table
            
        except Exception as e:
            logger.error(f"Time travel query failed: {e}")
            return None

def create_spark_session():
    """Create Spark session with Delta Lake support"""
    spark = SparkSession.builder \
        .appName("DeltaLakeWriter") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
        .config("spark.databricks.delta.autoCompact.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()
    
    return spark

def main():
    """Main Delta Lake writer function"""
    logger.info("Starting Delta Lake Writer")
    
    # Create Spark session
    spark = create_spark_session()
    
    # Create Delta Lake writer
    delta_writer = DeltaLakeWriter(spark)
    
    try:
        # Example: Write some sample data (in real use, this would be called from streaming app)
        logger.info("Delta Lake Writer initialized successfully")
        logger.info(f"Delta table path: {S3_PATH}")
        logger.info(f"Checkpoint location: {CHECKPOINT_LOCATION}")
        
        # Show configuration
        logger.info("Delta Lake configuration:")
        logger.info("  - Auto-compaction: enabled")
        logger.info("  - Schema evolution: enabled")
        logger.info("  - Optimized writes: enabled")
        logger.info("  - Partitioning: year, month, day")
        
    except Exception as e:
        logger.error(f"Error in Delta Lake Writer: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
