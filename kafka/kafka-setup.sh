#!/bin/bash

set -e

# Start Kafka in background
/opt/bitnami/kafka/bin/kafka-server-start.sh /opt/bitnami/kafka/config/server.properties &

# Wait for Kafka port 9092 to be ready
echo "Waiting for Kafka to be ready on port 9092..."
while ! nc -z localhost 9092; do
  sleep 1
done
echo "Kafka is up!"

# Create topics
/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic video-stream --partitions 1 --replication-factor 1 || true
/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic audio-stream --partitions 1 --replication-factor 1 || true

echo "Topics created."