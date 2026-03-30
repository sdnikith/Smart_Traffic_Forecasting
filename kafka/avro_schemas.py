#!/usr/bin/env python3
"""
Avro schemas for traffic data
Defines the structure of our Kafka messages
"""

from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Schema registry config
SCHEMA_REGISTRY_URL = "http://localhost:8081"
SCHEMA_REGISTRY_CLIENT_CONFIG = {
    'url': SCHEMA_REGISTRY_URL,
    'basic.auth.user.info': 'user:password'  # Default, can be configured via env vars
}

class TrafficSchemas:
    """Traffic platform Avro schemas"""
    
    @staticmethod
    def get_traffic_sensor_schema():
        """Traffic sensor event schema"""
        schema_dict = {
            "type": "record",
            "name": "TrafficSensorEvent",
            "namespace": "com.traffic.platform",
            "fields": [
                {"name": "sensor_id", "type": "string"},
                {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "traffic_volume", "type": "int"},
                {"name": "speed_mph", "type": ["null", "float"], "default": None},
                {"name": "occupancy_pct", "type": ["null", "float"], "default": None},
                {"name": "road_name", "type": "string"},
                {"name": "direction", "type": "string"},
                {"name": "latitude", "type": "double"},
                {"name": "longitude", "type": "double"}
            ]
        }
        return Schema(schema_dict)
    
    @staticmethod
    def get_weather_update_schema():
        """Weather update schema"""
        schema_dict = {
            "type": "record",
            "name": "WeatherUpdate",
            "namespace": "com.traffic.platform",
            "fields": [
                {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "latitude", "type": "double"},
                {"name": "longitude", "type": "double"},
                {"name": "temp_fahrenheit", "type": "float"},
                {"name": "humidity_pct", "type": "float"},
                {"name": "wind_speed_mph", "type": "float"},
                {"name": "weather_main", "type": "string"},
                {"name": "weather_description", "type": "string"},
                {"name": "visibility_miles", "type": "float"},
                {"name": "clouds_pct", "type": "int"},
                {"name": "rain_1h_mm", "type": ["null", "float"], "default": None},
                {"name": "snow_1h_mm", "type": ["null", "float"], "default": None}
            ]
        }
        return Schema(schema_dict)
    
    @staticmethod
    def get_sensor_metadata_schema():
        """Sensor metadata change schema (CDC)"""
        schema_dict = {
            "type": "record",
            "name": "SensorMetadataChange",
            "namespace": "com.traffic.platform",
            "fields": [
                {"name": "operation", "type": {"type": "enum", "name": "Operation", "symbols": ["INSERT", "UPDATE", "DELETE"]}},
                {"name": "sensor_id", "type": "string"},
                {"name": "road_name", "type": "string"},
                {"name": "city", "type": "string"},
                {"name": "direction", "type": "string"},
                {"name": "latitude", "type": "double"},
                {"name": "longitude", "type": "double"},
                {"name": "is_active", "type": "boolean"},
                {"name": "change_timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}}
            ]
        }
        return Schema(schema_dict)

class SchemaManager:
    """Manages Avro schema registration and compatibility"""
    
    def __init__(self):
        self.client = SchemaRegistryClient(SCHEMA_REGISTRY_CLIENT_CONFIG)
    
    def register_schemas(self):
        """Register all schemas with backward compatibility check"""
        schemas = [
            ("traffic-sensor-events", TrafficSchemas.get_traffic_sensor_schema()),
            ("weather-updates", TrafficSchemas.get_weather_update_schema()),
            ("sensor-metadata-changes", TrafficSchemas.get_sensor_metadata_schema())
        ]
        
        for topic_name, schema in schemas:
            try:
                # Check if schema already exists
                existing_schema = self.client.get_latest_version(topic_name)
                logger.info(f"Schema for topic '{topic_name}' already exists with ID {existing_schema.schema_id}")
                
                # Check compatibility before updating
                compatibility = self.client.test_compatibility(topic_name, schema)
                if compatibility.is_compatible:
                    logger.info(f"New schema is compatible with existing schema for '{topic_name}'")
                else:
                    logger.warning(f"New schema is NOT compatible with '{topic_name}': {compatibility.errors}")
                    continue
                
            except Exception:
                # Schema doesn't exist, register it
                schema_id = self.client.register_schema(topic_name, schema)
                logger.info(f"Schema '{schema.schema_dict['name']}' registered with id={schema_id}, compatibility=BACKWARD")
    
    def get_schema(self, topic_name):
        """Get latest schema for a topic"""
        try:
            return self.client.get_latest_version(topic_name)
        except Exception as e:
            logger.error(f"Failed to get schema for topic '{topic_name}': {e}")
            return None

if __name__ == "__main__":
    # Register all schemas
    schema_manager = SchemaManager()
    schema_manager.register_schemas()
    
    # Print registration summary
    logger.info("Schema registration completed. Available schemas:")
    for topic in ["traffic-sensor-events", "weather-updates", "sensor-metadata-changes"]:
        schema = schema_manager.get_schema(topic)
        if schema:
            logger.info(f"  - {topic}: {schema.schema.schema_dict['name']} (ID: {schema.schema_id})")
