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
        }
    }

    with pytest.raises(SparkExpectationsMiscException, match=r"error occurred while saving data into kafka .*"):
        delta_writer_handler.writer(_write_args=write_args)