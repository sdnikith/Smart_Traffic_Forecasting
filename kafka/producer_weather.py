#!/usr/bin/env python3
"""
Weather Data Producer for Kafka Streaming
"""

import time
import json
import os
import logging
import requests
from datetime import datetime, timezone
from confluent_kafka import Producer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import AvroSerializer

# Import our schemas
from avro_schemas import TrafficSchemas

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092,localhost:9093')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather-updates')
WEATHER_API_KEY = os.getenv('WEATHER_API_KEY', None)
WEATHER_MOCK = os.getenv('WEATHER_MOCK', 'false').lower() == 'true'

# Minneapolis coordinates (I-94 corridor)
LATITUDE = 44.98
LONGITUDE = -93.27

class WeatherDataProducer:
    """Produces weather data to Kafka"""
    
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
            'acks': 'all',
            'retries': 3,
            'delivery.timeout.ms': 10000,
            'batch.size': 16384,
            'linger.ms': 5,
            'compression.type': 'snappy',
            'enable.idempotence': True,
            'transactional.id': 'weather-producer-1'
        }
        
        self.producer = SerializingProducer(producer_conf)
        self.produced_count = 0
        self.retry_count = 0
        
        if not WEATHER_API_KEY and not WEATHER_MOCK:
            logger.warning("WEATHER_API_KEY not set and WEATHER_MOCK=false. Using mock data.")
            WEATHER_MOCK = True
    
    def fetch_weather_data(self):
        """Fetch current weather from OpenWeatherMap API"""
        if WEATHER_MOCK:
            return self.generate_mock_weather()
        
        try:
            url = f"https://api.openweathermap.org/data/2.5/weather"
            params = {
                'lat': LATITUDE,
                'lon': LONGITUDE,
                'appid': WEATHER_API_KEY,
                'units': 'imperial'  # Fahrenheit, mph
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Parse and flatten response
            weather_event = {
                'timestamp': int(datetime.now(timezone.utc).timestamp() * 1000),
                'latitude': LATITUDE,
                'longitude': LONGITUDE,
                'temp_fahrenheit': data['main']['temp'],
                'humidity_pct': data['main']['humidity'],
                'wind_speed_mph': data['wind']['speed'],
                'weather_main': data['weather'][0]['main'],
                'weather_description': data['weather'][0]['description'],
                'visibility_miles': data.get('visibility', 0) / 1609.34,  # Convert meters to miles
                'clouds_pct': data.get('clouds', {}).get('all', 0),
                'rain_1h_mm': data.get('rain', {}).get('1h', None),
                'snow_1h_mm': data.get('snow', {}).get('1h', None)
            }
            
            return weather_event
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch weather data: {e}")
            return None
        except (KeyError, ValueError) as e:
            logger.error(f"Failed to parse weather data: {e}")
            return None
    
    def generate_mock_weather(self):
        """Generate realistic mock weather data"""
        import random
        
        conditions = ['clear', 'clouds', 'rain', 'snow', 'mist', 'fog']
        weather_main = random.choice(['Clear', 'Clouds', 'Rain', 'Snow', 'Mist', 'Fog'])
        
        return {
            'timestamp': int(datetime.now(timezone.utc).timestamp() * 1000),
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temp_fahrenheit': random.uniform(-10, 95),
            'humidity_pct': random.uniform(20, 95),
            'wind_speed_mph': random.uniform(0, 45),
            'weather_main': weather_main,
            'weather_description': random.choice(conditions),
            'visibility_miles': random.uniform(0.1, 10),
            'clouds_pct': random.randint(0, 100),
            'rain_1h_mm': random.uniform(0, 2) if weather_main == 'Rain' else None,
            'snow_1h_mm': random.uniform(0, 1) if weather_main == 'Snow' else None
        }
    
    def delivery_callback(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            logger.error(f"Weather message delivery failed: {err}")
        else:
            self.produced_count += 1
            if self.produced_count % 10 == 0:  # Log every 10 weather updates
                logger.info(f"Weather update produced: temp={self.last_temp}°F, condition={self.last_weather}, offset={msg.offset()}")
    
    def produce_weather_updates(self):
        """Produce weather updates every 15 minutes"""
        logger.info(f"Starting weather data production to topic '{WEATHER_TOPIC}'")
        logger.info(f"Polling interval: 15 minutes")
        logger.info(f"Mock mode: {WEATHER_MOCK}")
        
        while True:
            try:
                # Fetch weather data
                weather_data = self.fetch_weather_data()
                
                if weather_data:
                    # Store for logging
                    self.last_temp = weather_data['temp_fahrenheit']
                    self.last_weather = weather_data['weather_main']
                    
                    # Produce to Kafka
                    key = "minneapolis-i94"
                    
                    self.producer.produce(
                        topic=WEATHER_TOPIC,
                        key=key,
                        value=weather_data,
                        callback=self.delivery_callback
                    )
                    
                    self.producer.flush()
                    self.retry_count = 0  # Reset retry count on success
                    
                else:
                    self.retry_count += 1
                    if self.retry_count <= 3:
                        wait_time = min(60, 2 ** self.retry_count)  # Exponential backoff
                        logger.warning(f"Weather fetch failed, retrying in {wait_time}s (attempt {self.retry_count}/3)")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error("Max retries reached, waiting for next poll cycle")
                        self.retry_count = 0
                
                # Wait 15 minutes before next update
                time.sleep(900)  # 15 minutes
                
            except KeyboardInterrupt:
                logger.info("Weather production interrupted by user")
                break
            except Exception as e:
                logger.error(f"Error during weather production: {e}")
                time.sleep(60)  # Wait 1 minute before retrying
    
    def close(self):
        """Clean shutdown"""
        self.producer.flush()
        self.producer.close()

def main():
    """Main weather producer function"""
    logger.info("Starting Weather Data Producer")
    logger.info(f"Kafka brokers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Schema Registry: {SCHEMA_REGISTRY_URL}")
    logger.info(f"Topic: {WEATHER_TOPIC}")
    logger.info(f"Coordinates: {LATITUDE}, {LONGITUDE}")
    
    producer = WeatherDataProducer()
    
    try:
        producer.produce_weather_updates()
    finally:
        producer.close()

if __name__ == "__main__":
    main()
