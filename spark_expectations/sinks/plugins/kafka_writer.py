from typing import Dict, Union
from pyspark.sql import DataFrame
from spark_expectations.sinks.plugins.base_writer import (
    SparkExpectationsSinkWriter,
    spark_expectations_writer_impl,
)
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations import _log


class SparkExpectationsKafkaWritePluginImpl(SparkExpectationsSinkWriter):
    """
    class helps to write the stats data into the NSP
    """

    @spark_expectations_writer_impl
    def writer(
        self, _write_args: Dict[Union[str], Union[str, bool, Dict[str, str], DataFrame]]
    ) -> None:
        """
        The functions helps to write data into the kafka topic
        Args:
            _write_args:

        Returns:

        """

        try:
            # kafka_options = {
            #     "kafka.bootstrap.servers": "localhost:9092",
            #     "topic": _write_args.get("nsp_topic_name"),
            #     "failOnDataLoss": "true",
            # }

            if _write_args.pop("enable_se_streaming"):
                _log.info("started write stats data into kafka stats topic")

                df: DataFrame = _write_args.get("stats_df")

                df.selectExpr("to_json(struct(*)) AS value").write.format("kafka").mode(
                    "append"
                ).options(**_write_args.get("kafka_write_options")).save()

                _log.info("ended writing stats data into kafka stats topic")

        except Exception as e:
            raise SparkExpectationsMiscException(
                f"error occurred while saving data into kafka {e}"
            )
