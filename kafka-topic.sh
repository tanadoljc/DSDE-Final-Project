#!/bin/bash

# Wait for Kafka to be ready
sleep 10

# Create topic
kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic coordinates-topic

# List topics
kafka-topics --list --bootstrap-server kafka:9092