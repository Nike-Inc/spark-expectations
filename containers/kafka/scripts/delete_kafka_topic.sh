#!/bin/bash

stats_topic_name=dq-sparkexpectations-stats
row_dq_res_topic_name=dq-sparkexpectations-row-dq-results

# Check if the topic exists
if $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -Fxq "$stats_topic_name"; then
    # Delete the topic
    echo "Deleting Kafka topic $stats_topic_name"
    $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$stats_topic_name"
    #rm -rf /tmp/kafka-logs/dq-sparkexpectations-stats*
else
    echo "Kafka topic $stats_topic_name does not exist"
fi

#check if the row dq results topic exists
if $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -Fxq "$row_dq_res_topic_name"; then
    # Delete the topic
    echo "Deleting Kafka topic $row_dq_res_topic_name"
    $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$row_dq_res_topic_name"
    #rm -rf /tmp/kafka-logs/dq-sparkexpectations-stats*
else
    echo "Kafka topic $row_dq_res_topic_name does not exist"
fi