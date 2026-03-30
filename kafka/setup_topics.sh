#!/bin/bash

# Kafka Topics Setup Script
# Creates all necessary topics for the Traffic Intelligence Platform

echo "🚦 Setting up Kafka Topics for Traffic Intelligence Platform..."

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
until docker exec kafka-broker-1 kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
  echo "🔄 Kafka not ready, waiting 5 seconds..."
  sleep 5
done

echo "✅ Kafka is ready! Creating topics..."

# Create traffic data topic (6 partitions for high throughput)
echo "📊 Creating traffic-data topic..."
docker exec kafka-broker-1 kafka-topics \
  --create \
  --topic traffic-data \
  --bootstrap-server localhost:9092 \
  --partitions 6 \
  --replication-factor 3 \
  --config cleanup.policy=compact \
  --config segment.ms=86400000 \
  --config retention.ms=604800000

# Create weather data topic (3 partitions)
echo "🌤️ Creating weather-data topic..."
docker exec kafka-broker-1 kafka-topics \
  --create \
  --topic weather-data \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 3 \
  --config cleanup.policy=delete \
  --config retention.ms=259200000

# Create traffic predictions topic (3 partitions)
echo "🔮 Creating traffic-predictions topic..."
docker exec kafka-broker-1 kafka-topics \
  --create \
  --topic traffic-predictions \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 3 \
  --config cleanup.policy=delete \
  --config retention.ms=259200000

# Create traffic anomalies topic (3 partitions)
echo "⚠️ Creating traffic-anomalies topic..."
docker exec kafka-broker-1 kafka-topics \
  --create \
  --topic traffic-anomalies \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 3 \
  --config cleanup.policy=delete \
  --config retention.ms=259200000

# Create system metrics topic (2 partitions)
echo "📈 Creating system-metrics topic..."
docker exec kafka-broker-1 kafka-topics \
  --create \
  --topic system-metrics \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 3 \
  --config cleanup.policy=delete \
  --config retention.ms=86400000

# Create audit logs topic (2 partitions)
echo "📋 Creating audit-logs topic..."
docker exec kafka-broker-1 kafka-topics \
  --create \
  --topic audit-logs \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 3 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000

# Create alerts topic (2 partitions)
echo "🚨 Creating alerts topic..."
docker exec kafka-broker-1 kafka-topics \
  --create \
  --topic alerts \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 3 \
  --config cleanup.policy=delete \
  --config retention.ms=86400000

# List all topics
echo "📋 Listing all created topics:"
docker exec kafka-broker-1 kafka-topics --bootstrap-server localhost:9092 --list

# Show topic details
echo "📊 Showing topic details:"
for topic in traffic-data weather-data traffic-predictions traffic-anomalies system-metrics audit-logs alerts; do
  echo "🔍 Topic: $topic"
  docker exec kafka-broker-1 kafka-topics --bootstrap-server localhost:9092 --describe --topic $topic
  echo ""
done

echo "✅ All Kafka topics created successfully!"
echo "🌐 Kafka UI: http://localhost:8080"
echo "📊 Schema Registry: http://localhost:8081"
echo "🔧 Kafka Connect: http://localhost:8083"
echo "📈 ksqlDB: http://localhost:8088"
