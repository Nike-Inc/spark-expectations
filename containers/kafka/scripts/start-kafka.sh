#!/bin/bash

# Configure Kafka to advertise localhost so host-side clients can connect
echo "advertised.listeners=PLAINTEXT://localhost:9092" >> $KAFKA_HOME/config/server.properties

# Start Zookeeper
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties
sleep 5

# Start Kafka
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
sleep 10

# Create a topic
$KAFKA_HOME/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic dq-sparkexpectations-stats

# Keep the container running
tail -f /dev/null
