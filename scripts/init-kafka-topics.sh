#!/bin/bash
# scripts/init-kafka-topics.sh

set -e

echo "⏳ Waiting for Kafka to be ready..."
sleep 10

echo "📋 Creating Kafka topics..."

# Create trades_raw topic
kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic trades_raw \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists \
  --config retention.ms=604800000 \
  --config compression.type=snappy

echo "✅ Topic 'trades_raw' created"

# Create other topics if needed
kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic trades_enriched \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

echo "✅ Topic 'trades_enriched' created"

# List all topics
echo "📊 Available topics:"
kafka-topics --list --bootstrap-server kafka:9092

echo " Kafka topics initialized successfully!"