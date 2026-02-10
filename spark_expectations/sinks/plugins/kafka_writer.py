from typing import Dict, Union
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, lit, schema_of_json
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
    def writer(self, _write_args: Dict[Union[str], Union[str, bool, Dict[str, str], DataFrame]]) -> None:
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

            _log.debug(f"_write_args: {_write_args}")

            if _write_args.pop("enable_se_streaming"):
                _log.info("started write stats data into kafka stats topic")

                df: DataFrame = _write_args.get("stats_df")
                kafka_options = _write_args.get("kafka_write_options")
                
                # Log Kafka connection details (mask sensitive info)
                masked_options = {k: (v if "password" not in k.lower() and "secret" not in k.lower() and "token" not in k.lower() else "***MASKED***") for k, v in kafka_options.items()}
                _log.info(f"Writing to Kafka with options: {masked_options}")

                # Convert se_job_metadata from JSON string to a proper struct
                # so it appears as a nested object in the Kafka JSON output
                # instead of a double-escaped string
                if "se_job_metadata" in df.columns:
                    metadata_sample = df.select("se_job_metadata").first()[0]
                    if metadata_sample:
                        metadata_schema = schema_of_json(lit(metadata_sample))
                        df = df.withColumn(
                            "se_job_metadata",
                            from_json(col("se_job_metadata"), metadata_schema),
                        )

                df.selectExpr("to_json(struct(*)) AS value").write.format("kafka").mode("append").options(
                    **kafka_options
                ).save()

                _log.info("ended writing stats data into kafka stats topic")

        except Exception as e:
            # Log detailed error information
            kafka_options = _write_args.get("kafka_write_options", {})
            topic = kafka_options.get("topic", "unknown")
            bootstrap_servers = kafka_options.get("kafka.bootstrap.servers", "unknown")
            
            error_details = f"Failed to write to Kafka topic '{topic}' on servers '{bootstrap_servers}'. Error: {str(e)}"
            _log.error(f"Kafka write failure: {error_details}")
            
            raise SparkExpectationsMiscException(f"error occurred while saving data into kafka: {error_details}")
