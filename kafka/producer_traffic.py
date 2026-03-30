#!/usr/bin/env python3
"""
Kafka Traffic Data Producer
Sends traffic sensor data to Kafka for real-time processing
"""

import csv
import time
import json
import os
import logging
from datetime import datetime, timezone
from confluent_kafka import Producer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.serialization import AvroSerializer

# Import schemas
from avro_schemas import TrafficSchemas

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get config from environment
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092,localhost:9093')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'traffic-sensor-events')
REPLAY_SPEED_MS = int(os.getenv('REPLAY_SPEED_MS', '100'))  # 1 record every 100ms
UCI_DATASET_PATH = os.getenv('UCI_DATASET_PATH', 'metro_interstate_traffic_volume.csv')

class TrafficDataProducer:
    """Produces traffic sensor data to Kafka"""
    
    def __init__(self):
        # Schema Registry client
        schema_registry_conf = {
            'url': SCHEMA_REGISTRY_URL,
            'basic.auth.user.info': 'user:password'
        }
        self.schema_registry = SchemaRegistryClient(schema_registry_conf)
        
        # Avro serializer
        self.avro_serializer = AvroSerializer(self.schema_registry)
        
        # Kafka producer configuration
        producer_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'key.serializer': 'org.apache.kafka.common.serialization.StringSerializer',
            'value.serializer': self.avro_serializer,
            'partitioner.class': 'org.apache.kafka.clients.producer.internals.DefaultPartitioner',
            'acks': 'all',
            'retries': 3,
            'delivery.timeout.ms': 10000,
            'batch.size': 16384,
            'linger.ms': 5,
            'compression.type': 'snappy',
            'max.in.flight.requests.per.connection': 5,
            'enable.idempotence': True,
            'transactional.id': 'traffic-producer-1'
        }
        
        self.producer = SerializingProducer(producer_conf)
        self.produced_count = 0
        
        # Load UCI dataset
        self.traffic_data = self.load_uci_dataset()
        logger.info(f"Loaded {len(self.traffic_data)} records from UCI dataset")
    
    def load_uci_dataset(self):
        """Load UCI Metro Interstate Traffic Volume dataset"""
        try:
            records = []
            with open(UCI_DATASET_PATH, 'r') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    # Parse UCI dataset fields
                    try:
                        timestamp = datetime.strptime(row['date_time'], '%Y-%m-%d %H:%M:%S')
                        timestamp_ms = int(timestamp.timestamp() * 1000)
                        
                        # Create traffic sensor event
                        event = {
                            'sensor_id': f"i94-sensor-{hash(row['date_time']) % 10}",
                            'timestamp': timestamp_ms,
                            'traffic_volume': int(row['traffic_volume']),
                            'speed_mph': None,  # Not in UCI dataset
                            'occupancy_pct': None,  # Not in UCI dataset
                            'road_name': 'I-94 Westbound',
                            'direction': 'West',
                            'latitude': 44.98,  # Minneapolis coordinates
                            'longitude': -93.27
                        }
                        records.append(event)
                    except (ValueError, KeyError) as e:
                        logger.warning(f"Skipping malformed row: {row}, error: {e}")
                        continue
            
            logger.info(f"Successfully parsed {len(records)} traffic sensor events")
            return records
            
        except FileNotFoundError:
            logger.error(f"UCI dataset not found at {UCI_DATASET_PATH}")
            return []
        except Exception as e:
            logger.error(f"Error loading UCI dataset: {e}")
            return []
    
    def delivery_callback(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            self.produced_count += 1
            if self.produced_count % 1000 == 0:
                logger.info(f"Produced {self.produced_count} events, latest offset={msg.offset()}, rate={self.produced_count/(time.time() - self.start_time):.2f} events/sec")
    
    def produce_traffic_stream(self):
        """Produce traffic data as simulated real-time stream"""
        logger.info(f"Starting traffic data production to topic '{TOPIC_NAME}'")
        logger.info(f"Replay speed: 1 record every {REPLAY_SPEED_MS}ms")
        
        self.start_time = time.time()
        
        try:
            for i, record in enumerate(self.traffic_data):
                # Use sensor_id as key for partitioning
                key = record['sensor_id']
                
                # Produce message
                self.producer.produce(
                    topic=TOPIC_NAME,
                    key=key,
                    value=record,
                    callback=self.delivery_callback
                )
                
                # Flush every 100 messages
                if i % 100 == 0:
                    self.producer.flush()
                
                # Simulate real-time timing
                time.sleep(REPLAY_SPEED_MS / 1000.0)
            
            # Final flush
            self.producer.flush()
            logger.info(f"Production completed. Total events produced: {self.produced_count}")
            
        except KeyboardInterrupt:
            logger.info("Production interrupted by user")
            self.producer.flush()
            logger.info(f"Gracefully flushed remaining messages. Total produced: {self.produced_count}")
        except Exception as e:
            logger.error(f"Error during production: {e}")
    
    def close(self):
        """Clean shutdown"""
        self.producer.flush()
        self.producer.close()

def main():
    """Main producer function"""
    logger.info("Starting Traffic Data Producer")
    logger.info(f"Kafka brokers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Schema Registry: {SCHEMA_REGISTRY_URL}")
    logger.info(f"Topic: {TOPIC_NAME}")
    
    producer = TrafficDataProducer()
    
    try:
        producer.produce_traffic_stream()
    finally:
        producer.close()

if __name__ == "__main__":
    main()
