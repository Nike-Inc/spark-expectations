import pytest
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.sinks.plugins.kafka_writer import SparkExpectationsKafkaWritePluginImpl


def test_kafka_writer_exception():
    delta_writer_handler = SparkExpectationsKafkaWritePluginImpl()

    write_args = {
        "kafka_write_options": {
            "kafka.bootstrap.servers": "localhost:9092",
            "topic": "dq-sparkexpectations-stats",
            "failOnDataLoss": "true",
        },
        "enable_se_streaming": True
    }

    with pytest.raises(SparkExpectationsMiscException, match=r"error occurred while saving data into kafka: Failed to write to Kafka topic 'dq-sparkexpectations-stats' on servers 'localhost:9092'.*"):
        delta_writer_handler.writer(_write_args=write_args)


def test_kafka_writer_exception_missing_topic():
    """Test Kafka writer exception when topic is missing from options"""
    kafka_writer_handler = SparkExpectationsKafkaWritePluginImpl()

    write_args = {
        "kafka_write_options": {
            "kafka.bootstrap.servers": "localhost:9092",
            "failOnDataLoss": "true",
        },
        "enable_se_streaming": True
    }

    with pytest.raises(SparkExpectationsMiscException, match=r"error occurred while saving data into kafka: Failed to write to Kafka topic 'unknown' on servers 'localhost:9092'.*"):
        kafka_writer_handler.writer(_write_args=write_args)


def test_kafka_writer_exception_missing_servers():
    """Test Kafka writer exception when bootstrap servers are missing from options"""
    kafka_writer_handler = SparkExpectationsKafkaWritePluginImpl()

    write_args = {
        "kafka_write_options": {
            "topic": "dq-sparkexpectations-stats",
            "failOnDataLoss": "true",
        },
        "enable_se_streaming": True
    }

    with pytest.raises(SparkExpectationsMiscException, match=r"error occurred while saving data into kafka: Failed to write to Kafka topic 'dq-sparkexpectations-stats' on servers 'unknown'.*"):
        kafka_writer_handler.writer(_write_args=write_args)