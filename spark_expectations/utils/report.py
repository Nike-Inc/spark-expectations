
from dataclasses import dataclass
from typing import Dict, Optional, Tuple, List
from datetime import datetime, timezone
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    lit,
    expr,
    when,
    array,
    to_timestamp,
    round as sql_round,
    create_map,
    explode,
    to_json,
    col,
    split,
    current_date,
    get_json_object
)
from pyspark.sql.types import StructType
from spark_expectations import _log
from spark_expectations.core.exceptions import (
    SparkExpectationsUserInputOrConfigInvalidException,
    SparkExpectationsMiscException,
)
from spark_expectations.secrets import SparkExpectationsSecretsBackend
from spark_expectations.utils.udf import remove_empty_maps
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.sinks import _sink_hook
from spark_expectations.config.user_config import Constants as user_config

@dataclass
class SparkExpectationsReport:
    """
    This class implements/supports writing data into the sink system
    """

    _context: SparkExpectationsContext

    def __post_init__(self) -> None:
        self.spark = self._context.spark

    def dq_obs_report_data_insert(self, df_detailed: DataFrame, df_query_output: DataFrame):
        try:
            print("dq_obs_report_data_insert method called stats_detailed table")
            # df_detailed.show(truncate=False)
            df = df_detailed
            # List of columns to be removed
            columns_to_remove = [
                "target_dq_status",
                "source_expectations",
                "source_dq_actual_outcome",
                "source_dq_expected_outcome",
                "source_dq_start_time",
                "source_dq_end_time",
                "target_expectations",
                "target_dq_actual_outcome",
                "target_dq_expected_outcome",
                "target_dq_actual_row_count",
                "target_dq_error_row_count",
                "target_dq_row_count",
                "target_dq_start_time",
                "target_dq_end_time",
                "dq_job_metadata_info"
            ]
            # Rename the columns
            df = df.withColumnRenamed("source_dq_row_count", "total_records") \
                .withColumnRenamed("source_dq_error_row_count", "failed_records") \
                .withColumnRenamed("source_dq_actual_row_count", "valid_records")
            df = df.withColumn("dag_name", get_json_object(col("dq_job_metadata_info"), "$.job")) \
                .withColumn("Region_cd", get_json_object(col("dq_job_metadata_info"), "$.Region")) \
                .withColumn("env", get_json_object(col("dq_job_metadata_info"), "$.env"))

            # Calculate the success percentage and add it as a new column
            df = df.withColumn("success_percentage", (col("valid_records") / col("total_records")) * 100)

            # Create a new DataFrame by dropping the specified columns
            print("This is the table ")
            new_df = df.drop(*columns_to_remove)
            # Save the DataFrame to a table
            new_df.write.mode("overwrite").saveAsTable("dq_obs_report_data")
            return new_df
        except Exception as e:
            raise SparkExpectationsMiscException(
                f"An error occurred in dq_obs_report_data_insert: {e}"
            )



