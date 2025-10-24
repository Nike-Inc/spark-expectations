#!/bin/bash

stats_topic_name=dq-sparkexpectations-stats
row_dq_res_topic_name=dq-sparkexpectations-row-dq-results

# Check if the stats topic exists
if $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -Fxq "$stats_topic_name"; then
  echo "Kafka topic $stats_topic_name is already exist"
else
  # Create a topic
  echo "creating kafka topic"
  $KAFKA_HOME/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic dq-sparkexpectations-stats
fi

# Check if the row dq results topic exists
if $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -Fxq "$row_dq_res_topic_name"; then
  echo "Kafka topic $row_dq_res_topic_name is already exist"
else
  # Create a topic
  echo "creating kafka topic"
  $KAFKA_HOME/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic dq-sparkexpectations-row-dq-results
fi