#!/usr/bin/env python3
"""
Spark Streaming Traffic Consumer
Processes real-time traffic data from Kafka
"""

import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType, FloatType
from pyspark.sql.streaming import StreamingQuery
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import AvroDeserializer

# Import streaming functions
from windowed_aggregations import compute_rolling_metrics
from anomaly_detector import detect_congestion
from alert_publisher import publish_anomalies

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Config variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092,localhost:9093')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic-sensor-events')
ALERTS_TOPIC = os.getenv('ALERTS_TOPIC', 'traffic-alerts')
S3_PATH = os.getenv('S3_PATH', 's3://traffic-platform-lake/bronze/traffic_events/')
CHECKPOINT_LOCATION = os.getenv('CHECKPOINT_LOCATION', 's3://traffic-platform-lake/checkpoints/traffic-streaming/')

class TrafficStreamingApp:
    """Main Spark Structured Streaming application"""
    
    def __init__(self):
        self.spark = self.create_spark_session()
        self.schema_registry = SchemaRegistryClient({
            'url': SCHEMA_REGISTRY_URL,
            'basic.auth.user.info': 'user:password'
        })
        
    def create_spark_session(self):
        """Create Spark session with optimized configurations"""
        spark = SparkSession.builder \
            .appName("TrafficStreamingApp") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.sql.shuffle.partitions", "10") \
            .config("spark.streaming.backpressure.enabled", "true") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
            .getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        
        return spark
    
    def read_from_kafka(self):
        """Read traffic sensor events from Kafka"""
        logger.info(f"Reading from Kafka topic: {TRAFFIC_TOPIC}")
        
        # Define schema for traffic events
        traffic_schema = StructType([
            StructField("sensor_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("traffic_volume", IntegerType(), True),
            StructField("speed_mph", FloatType(), True),
            StructField("occupancy_pct", FloatType(), True),
            StructField("road_name", StringType(), True),
            StructField("direction", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True)
        ])
        
        # Read stream from Kafka
        traffic_stream = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", TRAFFIC_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", 1000) \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON and apply schema
        traffic_stream = traffic_stream \
            .select(F.from_json("value", traffic_schema).alias("data")) \
            .select("data.*") \
            .withWatermark("timestamp", "10 minutes")  # Handle late data up to 10 minutes
        
        return traffic_stream
    
    def process_stream(self, traffic_stream):
        """Process the traffic stream with windowing and anomaly detection"""
        logger.info("Starting stream processing")
        
        # Compute rolling metrics
        enriched_stream = compute_rolling_metrics(traffic_stream)
        
        # Detect congestion and anomalies
        anomaly_stream = detect_congestion(enriched_stream)
        
        return enriched_stream, anomaly_stream
    
    def write_sinks(self, enriched_stream, anomaly_stream):
        """Write to multiple sinks"""
        logger.info("Setting up stream sinks")
        
        # Sink 1: S3 Delta Lake (for batch cold path)
        enriched_stream.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/delta") \
            .option("mergeSchema", "true") \
            .option("replaceWhere", "") \
            .partitionBy("year", "month", "day") \
            .trigger(processingTime="5 minutes") \
            .start(S3_PATH)
        
        # Sink 2: Console/log for real-time monitoring
        console_query = enriched_stream.writeStream \
            .format("console") \
            .outputMode("append") \
            .option("truncate", "false") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        # Sink 3: Kafka topic for anomalies only
        anomaly_stream.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", ALERTS_TOPIC) \
            .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/alerts") \
            .outputMode("append") \
            .trigger(processingTime="1 minute") \
            .start()
        
        return console_query
    
    def monitor_query(self, query):
        """Monitor streaming query status"""
        try:
            query_status = query.status
            if query_status['isDataAvailable']:
                logger.info(f"Query is active, processing data from {query_status['inputRows']} input rows")
            else:
                logger.info("Query is active, no data available")
            
            # Check for errors
            if query_status['isTerminated']:
                logger.error(f"Query terminated with error: {query_status['exception']}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error monitoring query: {e}")
            return False
    
    def run(self):
        """Main execution method"""
        logger.info("Starting Traffic Streaming Application")
        logger.info(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"Schema Registry: {SCHEMA_REGISTRY_URL}")
        
        try:
            # Read from Kafka
            traffic_stream = self.read_from_kafka()
            
            # Process stream
            enriched_stream, anomaly_stream = self.process_stream(traffic_stream)
            
            # Write to sinks
            console_query = self.write_sinks(enriched_stream, anomaly_stream)
            
            # Monitor the console query
            logger.info("Stream processing started. Monitoring query status...")
            while self.monitor_query(console_query):
                import time
                time.sleep(10)  # Check every 10 seconds
            
        except KeyboardInterrupt:
            logger.info("Stream processing interrupted by user")
        except Exception as e:
            logger.error(f"Error in stream processing: {e}")
        finally:
            logger.info("Stopping stream processing...")
            self.spark.stop()

class StreamingMetricsMonitor:
    """Monitor streaming application metrics"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def print_streaming_metrics(self):
        """Print current streaming metrics"""
        try:
            # Get active streaming queries
            active_queries = self.spark.streams.active
            
            for query in active_queries:
                logger.info(f"Query: {query.name}")
                logger.info(f"  Status: {query.status}")
                logger.info(f"  Input rate: {query.status.get('inputRowsPerSecond', 0)} rows/sec")
                logger.info(f"  Processing rate: {query.status.get('processedRowsPerSecond', 0)} rows/sec")
                logger.info(f"  Total input rows: {query.status.get('inputRows', 0)}")
                logger.info(f"  Total processed rows: {query.status.get('processedRows', 0)}")
                
        except Exception as e:
            logger.error(f"Error getting streaming metrics: {e}")

def main():
    """Main entry point"""
    logger.info("Initializing Traffic Streaming Application")
    
    # Create streaming app
    app = TrafficStreamingApp()
    
    # Create metrics monitor
    metrics_monitor = StreamingMetricsMonitor(app.spark)
    
    try:
        # Run the streaming application
        app.run()
    except Exception as e:
        logger.error(f"Fatal error in streaming application: {e}")
        raise

if __name__ == "__main__":
    main()
