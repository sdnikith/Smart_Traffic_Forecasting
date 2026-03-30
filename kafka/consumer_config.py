#!/usr/bin/env python3
"""
Kafka Consumer Configuration for Traffic Platform
"""

import os
import logging
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import AvroDeserializer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092,localhost:9093')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')

class KafkaConsumerConfig:
    """Consumer group configurations for different use cases"""
    
    # Consumer group configurations
    CONSUMER_GROUPS = {
        "traffic-streaming-group": {
            "description": "Real-time streaming consumer",
            "auto.offset.reset": "latest",
            "enable.auto.commit": "false",  # Manual commit after Spark checkpoint
            "group.id": "traffic-streaming-group",
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 3000,
            "max.poll.records": 1000,
            "fetch.min.bytes": 1048576,  # 1MB
            "fetch.max.wait.ms": 500
        },
        "traffic-batch-group": {
            "description": "Batch processing for S3 sink",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "true",  # Auto-commit for batch processing
            "group.id": "traffic-batch-group",
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 3000,
            "max.poll.records": 5000,
            "fetch.min.bytes": 5242880,  # 5MB
            "fetch.max.wait.ms": 1000
        },
        "weather-consumer-group": {
            "description": "Weather data consumer",
            "auto.offset.reset": "latest",
            "enable.auto.commit": "true",
            "group.id": "weather-consumer-group",
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 3000,
            "max.poll.records": 100,
            "fetch.min.bytes": 1048576
        }
    }
    
    # Topic configurations
    TOPICS = {
        "traffic-sensor-events": {
            "description": "Real-time traffic sensor readings",
            "partitions": 3,
            "replication_factor": 2,
            "retention_hours": 168,  # 7 days
            "cleanup_policy": "delete"
        },
        "weather-updates": {
            "description": "Weather updates every 15 minutes",
            "partitions": 3,
            "replication_factor": 2,
            "retention_hours": 720,  # 30 days
            "cleanup_policy": "delete"
        },
        "traffic-alerts": {
            "description": "Traffic anomalies and alerts",
            "partitions": 3,
            "replication_factor": 2,
            "retention_hours": 168,  # 7 days
            "cleanup_policy": "delete"
        }
    }
    
    @staticmethod
    def get_consumer_config(group_name):
        """Get consumer configuration for a specific group"""
        if group_name not in KafkaConsumerConfig.CONSUMER_GROUPS:
            raise ValueError(f"Unknown consumer group: {group_name}")
        
        base_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'key.deserializer': 'org.apache.kafka.common.serialization.StringDeserializer',
            'value.deserializer': AvroDeserializer(SchemaRegistryClient({
                'url': SCHEMA_REGISTRY_URL,
                'basic.auth.user.info': 'user:password'
            })),
            'auto.offset.reset': KafkaConsumerConfig.CONSUMER_GROUPS[group_name]['auto.offset.reset'],
            'enable.auto.commit': KafkaConsumerConfig.CONSUMER_GROUPS[group_name]['enable.auto.commit'],
            'group.id': KafkaConsumerConfig.CONSUMER_GROUPS[group_name]['group.id'],
            'session.timeout.ms': KafkaConsumerConfig.CONSUMER_GROUPS[group_name]['session.timeout.ms'],
            'heartbeat.interval.ms': KafkaConsumerConfig.CONSUMER_GROUPS[group_name]['heartbeat.interval.ms'],
            'max.poll.records': KafkaConsumerConfig.CONSUMER_GROUPS[group_name]['max.poll.records'],
            'fetch.min.bytes': KafkaConsumerConfig.CONSUMER_GROUPS[group_name]['fetch.min.bytes'],
            'fetch.max.wait.ms': KafkaConsumerConfig.CONSUMER_GROUPS[group_name]['fetch.max.wait.ms']
        }
        
        return base_config
    
    @staticmethod
    def get_topic_info(topic_name):
        """Get topic information"""
        if topic_name not in KafkaConsumerConfig.TOPICS:
            raise ValueError(f"Unknown topic: {topic_name}")
        
        return KafkaConsumerConfig.TOPICS[topic_name]

class KafkaHealthChecker:
    """Health check for Kafka cluster and consumer groups"""
    
    def __init__(self):
        self.consumer_configs = KafkaConsumerConfig.CONSUMER_GROUPS
        self.topic_configs = KafkaConsumerConfig.TOPICS
    
    def check_topic_exists(self, topic_name):
        """Check if topic exists (simplified check)"""
        # In production, this would use AdminClient to list topics
        # For now, assume topics are created via docker-compose
        return topic_name in self.topic_configs
    
    def check_consumer_lag(self, group_name, topic_name):
        """Calculate consumer lag (simplified)"""
        # In production, this would use consumer group metrics
        # For now, return mock lag data
        import random
        partitions = self.topic_configs.get(topic_name, {}).get('partitions', 3)
        lag_data = {}
        
        for partition in range(partitions):
            lag_data[f"partition_{partition}"] = random.randint(0, 1000)
        
        return lag_data
    
    def health_check(self):
        """Perform comprehensive health check"""
        logger.info("Performing Kafka health check...")
        
        health_status = {
            "timestamp": datetime.now().isoformat(),
            "topics": {},
            "consumer_groups": {}
        }
        
        # Check topics
        for topic_name in self.topic_configs:
            exists = self.check_topic_exists(topic_name)
            health_status["topics"][topic_name] = {
                "exists": exists,
                "status": "healthy" if exists else "missing",
                "partitions": self.topic_configs[topic_name]["partitions"],
                "replication_factor": self.topic_configs[topic_name]["replication_factor"]
            }
        
        # Check consumer groups
        for group_name in self.consumer_configs:
            for topic_name in ["traffic-sensor-events", "weather-updates"]:
                lag = self.check_consumer_lag(group_name, topic_name)
                total_lag = sum(lag.values())
                
                health_status["consumer_groups"][f"{group_name}_{topic_name}"] = {
                    "status": "healthy" if total_lag < 1000 else "lagging",
                    "total_lag": total_lag,
                    "partition_lag": lag
                }
        
        # Log health status
        logger.info("Kafka Health Check Results:")
        for topic, status in health_status["topics"].items():
            logger.info(f"  Topic {topic}: {status['status']} (partitions: {status['partitions']})")
        
        for group, status in health_status["consumer_groups"].items():
            logger.info(f"  Consumer {group}: {status['status']} (lag: {status['total_lag']})")
        
        return health_status

def main():
    """Main health check function"""
    logger.info("Starting Kafka Health Checker")
    logger.info(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Schema Registry: {SCHEMA_REGISTRY_URL}")
    
    health_checker = KafkaHealthChecker()
    health_status = health_checker.health_check()
    
    # Print configuration summary
    logger.info("\n=== Consumer Group Configurations ===")
    for group_name, config in KafkaConsumerConfig.CONSUMER_GROUPS.items():
        logger.info(f"\n{group_name}:")
        logger.info(f"  Description: {config['description']}")
        logger.info(f"  Auto Offset Reset: {config['auto.offset.reset']}")
        logger.info(f"  Auto Commit: {config['enable.auto.commit']}")
        logger.info(f"  Max Poll Records: {config['max.poll.records']}")
    
    logger.info("\n=== Topic Configurations ===")
    for topic_name, config in KafkaConsumerConfig.TOPICS.items():
        logger.info(f"\n{topic_name}:")
        logger.info(f"  Description: {config['description']}")
        logger.info(f"  Partitions: {config['partitions']}")
        logger.info(f"  Replication Factor: {config['replication_factor']}")
        logger.info(f"  Retention: {config['retention_hours']} hours")

if __name__ == "__main__":
    main()
