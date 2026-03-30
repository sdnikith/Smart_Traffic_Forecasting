#!/usr/bin/env python3
"""
Late Arriving Data Handler for Delta Lake
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
LATE_THRESHOLD_HOURS = int(os.getenv('LATE_THRESHOLD_HOURS', '24'))

class LateArrivingHandler:
    """Handles late arriving data in Delta Lake"""
    
    def __init__(self, spark):
        self.spark = spark
        self.delta_table_path = S3_PATH
        self.late_threshold_hours = LATE_THRESHOLD_HOURS
    
    def handle_late_arrivals(self, df):
        """Handle sensor readings that arrive up to 24 hours late"""
        logger.info(f"Processing late arriving data with threshold: {self.late_threshold_hours} hours")
        
        # Add watermark to identify late data
        watermarked_df = df.withWatermark("timestamp", f"{self.late_threshold_hours} hours")
        
        # Filter for late data (data that arrives after watermark)
        late_df = watermarked_df.filter(
            F.col("timestamp") < F.current_timestamp() - F.expr(f"interval({self.late_threshold_hours} hours)")
        )
        
        # Add late arrival indicator
        late_df = late_df.withColumn(
            "is_late_arrival",
            F.lit(True)
        ).withColumn(
            "late_arrival_hours",
            F.round(
                (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("timestamp")) / 3600.0,
                2
            )
        )
        
        # Count late records
        late_count = late_df.count()
        if late_count > 0:
            logger.warning(f"Found {late_count} late arriving records")
            
            # Merge late data into Delta table
            self.merge_late_data(late_df)
            
            # Log sample of late records
            late_samples = late_df.select(
                "sensor_id", "timestamp", "traffic_volume", "late_arrival_hours"
            ).limit(5)
            
            logger.warning("Sample late records:")
            late_samples.show(truncate=False)
        
        return late_df
    
    def merge_late_data(self, late_df):
        """Merge late arriving data into Delta table using MERGE INTO"""
        logger.info("Merging late arriving data into Delta table")
        
        # Create target table alias
        target_table = self.spark.read.format("delta").load(self.delta_table_path).alias("target")
        
        # Perform MERGE INTO operation
        late_df.createOrReplaceTempView("late_data")
        
        merge_sql = f"""
            MERGE INTO delta.`{self.delta_table_path}` AS target
            USING late_data AS source
            ON target.sensor_id = source.sensor_id AND target.timestamp = source.timestamp
            WHEN MATCHED THEN
                UPDATE SET
                    target.traffic_volume = source.traffic_volume,
                    target.speed_mph = source.speed_mph,
                    target.occupancy_pct = source.occupancy_pct,
                    target.road_name = source.road_name,
                    target.direction = source.direction,
                    target.latitude = source.latitude,
                    target.longitude = source.longitude,
                    target.is_late_arrival = true,
                    target.late_arrival_hours = source.late_arrival_hours,
                    target.updated_at = current_timestamp()
            WHEN NOT MATCHED AND source.timestamp IS NOT NULL THEN
                INSERT (
                    sensor_id, timestamp, traffic_volume, speed_mph, occupancy_pct,
                    road_name, direction, latitude, longitude,
                    is_late_arrival, late_arrival_hours, updated_at
                )
        """
        
        try:
            self.spark.sql(merge_sql)
            logger.info(f"Successfully merged {late_df.count()} late records into Delta table")
            
        except Exception as e:
            logger.error(f"Merge failed: {e}")
    
    def get_late_arrival_stats(self):
        """Get statistics about late arriving data"""
        logger.info("Getting late arrival statistics")
        
        try:
            # Read Delta table
            delta_table = self.spark.read.format("delta").load(self.delta_table_path)
            
            # Filter for late arrivals in last 7 days
            seven_days_ago = datetime.now() - timedelta(days=7)
            
            late_stats = delta_table.filter(
                F.col("is_late_arrival") == True
            ).filter(
                F.col("updated_at") >= seven_days_ago
            )
            
            if late_stats.count() > 0:
                # Calculate statistics
                stats = late_stats.agg(
                    F.count("sensor_id").alias("total_late_records"),
                    F.countDistinct("sensor_id").alias("affected_sensors"),
                    F.avg("late_arrival_hours").alias("avg_late_hours"),
                    F.max("late_arrival_hours").alias("max_late_hours"),
                    F.min("late_arrival_hours").alias("min_late_hours")
                ).collect()[0]
                
                logger.info("Late Arrival Statistics (Last 7 Days):")
                logger.info(f"  Total late records: {stats['total_late_records']}")
                logger.info(f"  Affected sensors: {stats['affected_sensors']}")
                logger.info(f"  Average late hours: {stats['avg_late_hours']:.2f}")
                logger.info(f"  Max late hours: {stats['max_late_hours']:.2f}")
                logger.info(f"  Min late hours: {stats['min_late_hours']:.2f}")
                
                return stats
            else:
                logger.info("No late arrivals in the last 7 days")
                return None
                
        except Exception as e:
            logger.error(f"Failed to get late arrival stats: {e}")
            return None
    
    def cleanup_old_late_records(self, days=30):
        """Clean up old late arrival records"""
        logger.info(f"Cleaning up late arrival records older than {days} days")
        
        try:
            # Read Delta table
            delta_table = self.spark.read.format("delta").load(self.delta_table_path)
            
            # Calculate cutoff date
            cutoff_date = datetime.now() - timedelta(days=days)
            
            # Delete old late records
            delta_table.filter(
                F.col("is_late_arrival") == True
            ).filter(
                F.col("updated_at") < cutoff_date
            ).select("sensor_id", "timestamp") \
             .write.format("delta") \
             .mode("overwrite") \
             .option("replaceWhere", f"updated_at < '{cutoff_date.isoformat()}'") \
             .saveAsTable("temp_late_cleanup")
            
            # Vacuum the table
            self.spark.sql("VACUUM delta.`{self.delta_table_path}` RETAIN {days} HOURS")
            
            logger.info(f"Cleaned up late arrival records older than {days} days")
            
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")

def create_spark_session():
    """Create Spark session with Delta Lake support"""
    spark = SparkSession.builder \
        .appName("LateArrivingHandler") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()
    
    return spark

def main():
    """Main late arriving handler function"""
    logger.info("Starting Late Arriving Data Handler")
    logger.info(f"Late threshold: {LATE_THRESHOLD_HOURS} hours")
    
    # Create Spark session
    spark = create_spark_session()
    
    # Create handler
    handler = LateArrivingHandler(spark)
    
    try:
        # Get late arrival statistics
        stats = handler.get_late_arrival_stats()
        
        # Example: Clean up old records
        handler.cleanup_old_late_records(days=30)
        
        logger.info("Late Arriving Handler initialized successfully")
        
    except Exception as e:
        logger.error(f"Error in Late Arriving Handler: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
