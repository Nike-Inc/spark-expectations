from spark_expectations.sinks import get_sink_hook
from spark_expectations.sinks.plugins.kafka_writer import (
    SparkExpectationsKafkaWritePluginImpl,
)

def test_get_sink_hook():
    pm = get_sink_hook()
    # Check that the correct number of plugins have been registered
    assert len(pm.list_name_plugin()) == 1

    # Check that the correct plugins have been registered
    assert isinstance(pm.get_plugin("spark_expectations_kafka_write"), SparkExpectationsKafkaWritePluginImpl)
