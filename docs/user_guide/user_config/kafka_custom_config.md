# Kafka Streaming Custom Configuration
See this example config page for context about the user config: [examples page](../../../examples/#configurations).

Many users will set their streaming configuration to use Databricks or Cerberus secrets. However, if you are running locally or not using Cerberus or Databricks and want to specify the streaming topic name and Kafka bootstrap server you can enable the following custom parameters.

!!! important "Setup Note"
    Please note that the specified streaming topic and Kafka bootstrap server have to exist when running Spark Expectations (they will not be generated for you).

## Kafka Custom Configuration Parameters

!!! info "user_config.se_streaming_stats_kafka_custom_config_enable"
    Master toggle to enable using custom Kafka parameters

!!! info "user_config.se_streaming_stats_kafka_bootstrap_server"
    Used to set the Kafka bootstrap server

!!! info "user_config.se_streaming_stats_topic_name"
    Used to set the streaming topic name

!!! important "Defaults"
    If **user_config.se_streaming_stats_kafka_custom_config_enable** is set to `True` but the topic and server options are not specified, the defaults from the `spark_expectations/config/spark-expectations-default-config.yaml` file will be used.

## Configuration Example

```python
from typing import Dict, Union
from spark_expectations.config.user_config import Constants as user_config

stats_streaming_config_dict: Dict[str, Union[bool, str]] = {
    user_config.se_enable_streaming: True,
    user_config.se_streaming_stats_kafka_custom_config_enable: True,
    user_config.se_streaming_stats_topic_name: "dq-sparkexpectations-stats",
    user_config.se_streaming_stats_kafka_bootstrap_server: "localhost:9092",
}
```
