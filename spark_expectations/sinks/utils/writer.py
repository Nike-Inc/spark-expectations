from dataclasses import dataclass
from typing import Dict, Optional, Tuple, List, Any
from datetime import datetime, timezone
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
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
)
from pyspark.sql.types import StructType
from spark_expectations import _log
from spark_expectations.core.exceptions import (
    SparkExpectationsUserInputOrConfigInvalidException,
    SparkExpectationsMiscException,
)
from spark_expectations.secrets import SparkExpectationsSecretsBackend
from spark_expectations.notifications.push.alert import SparkExpectationsAlert
from spark_expectations.utils.udf import remove_passing_status_maps
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.sinks import _sink_hook
from spark_expectations.config.user_config import Constants as user_config


@dataclass
class SparkExpectationsWriter:
    """
    This class implements/supports writing data into the sink system
    """

    _context: SparkExpectationsContext

    def __post_init__(self) -> None:
        self.spark = self._context.spark

    def _set_table_properties_with_retry(
        self,
        table_name: str,
        max_retries: int = 3,
        initial_wait: float = 0.5,
        max_wait: float = 10.0
    ) -> None:
        """
        Set table properties with retry logic and exponential backoff.
        
        This method waits for the streaming table to be created and then sets the product_id
        property. Uses exponential backoff to avoid blocking while waiting for table creation.
        
        Args:
            table_name: Name of the table to set properties on
            max_retries: Maximum number of retry attempts (default: 5)
            initial_wait: Initial wait time in seconds (default: 0.5)
            max_wait: Maximum wait time between retries in seconds (default: 10.0)
            
        Returns:
            None
        """
        import time
        
        wait_time = initial_wait
        
        for attempt in range(max_retries):
            try:
                # Check if table exists - backward compatible with Spark < 3.3
                # Try to access table properties to check if table exists
                # This works in all Spark versions (Spark 2.x, 3.x, 3.3+)
                try:
                    table_properties = self.spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()  # pylint: disable=not-an-iterable
                except Exception:
                    # Table doesn't exist yet
                    if attempt < max_retries - 1:
                        _log.debug(
                            f"Table {table_name} not yet available, "
                            f"retrying in {wait_time:.2f}s (attempt {attempt + 1}/{max_retries})"
                        )
                        time.sleep(wait_time)
                        wait_time = min(wait_time * 2, max_wait)  # Exponential backoff with cap
                        continue
                    
                    _log.warning(
                        f"Table {table_name} not available after {max_retries} attempts, "
                        "skipping table properties"
                    )
                    return
                # pylint: disable=not-an-iterable
                table_properties_dict = {row["key"]: row["value"] for row in table_properties}

                # Set product_id if not set or different
                if (
                    table_properties_dict.get("product_id") is None
                    or table_properties_dict.get("product_id") != self._context.product_id
                ):
                    _log.info(f"Setting product_id for table {table_name} in tableproperties")
                    self.spark.sql(
                        f"ALTER TABLE {table_name} SET TBLPROPERTIES ('product_id' = "
                        f"'{self._context.product_id}')"
                    )
                    _log.info(f"Successfully set product_id for table {table_name}")
                else:
                    _log.debug(f"product_id already set for table {table_name}")
                
                # Success - exit retry loop
                return
                
            except Exception as e:
                if attempt < max_retries - 1:
                    _log.warning(
                        f"Error setting table properties for {table_name} (attempt {attempt + 1}/{max_retries}): {e}. "
                        f"Retrying in {wait_time:.2f}s..."
                    )
                    time.sleep(wait_time)
                    wait_time = min(wait_time * 2, max_wait)  # Exponential backoff
                else:
                    _log.warning(
                        f"Could not set table properties for the table {table_name} "
                        f"after {max_retries} attempts: {e}"
                    )
                    return

    def save_df_as_table(self, df: DataFrame, table_name: str, config: dict, stats_table: bool = False) -> Optional[StreamingQuery]:
        """
        This function takes a dataframe and writes into a table. It automatically detects if the DataFrame 
        is streaming and uses the appropriate write method (writeStream or write).

        Args:
            df: Provide the dataframe which need to be written as a table
            table_name: Provide the table name to which the dataframe need to be written to
            config: Provide the config to write the dataframe into the table
            stats_table: Provide if this is for writing stats table

        Returns:
            Optional[StreamingQuery]: Returns StreamingQuery for streaming DataFrames, None for batch DataFrames

        """
        try:
            # Add metadata columns for non-stats tables
            if not stats_table:
                df = df.withColumn(self._context.get_run_id_name, lit(f"{self._context.get_run_id}")).withColumn(
                    self._context.get_run_date_time_name,
                    to_timestamp(lit(f"{self._context.get_run_date}"), "yyyy-MM-dd HH:mm:ss"),
                )

            # Automatically detect if DataFrame is streaming and use appropriate write method
            if df.isStreaming:
                _log.info("Detected streaming DataFrame, using writeStream")
                
                _df_stream_writer = df.writeStream
                
                if config.get("outputMode") is not None:
                    _df_stream_writer = _df_stream_writer.outputMode(config["outputMode"])
                
                if config.get("format") is not None:
                    _df_stream_writer = _df_stream_writer.format(config["format"])
                
                if config.get("queryName") is not None:
                    _df_stream_writer = _df_stream_writer.queryName(f"{config['queryName']}_{table_name.split('.')[-1]}")
                
                if config.get("partitionBy") is not None and config["partitionBy"] != []:
                    _df_stream_writer = _df_stream_writer.partitionBy(config["partitionBy"])
                
                # Check for checkpoint location - critical for production streaming
                has_checkpoint = (
                    config.get("options") is not None 
                    and isinstance(config.get("options"), dict)
                    and "checkpointLocation" in config["options"]
                )
                
                if not has_checkpoint:
                    _log.warning(
                        "⚠️  PRODUCTION WARNING: No checkpointLocation specified for streaming DataFrame. "
                        "For production workloads, it is strongly recommended to set a dedicated checkpoint "
                        "location in the 'options' config when using Spark Expectations (SE) to write in "
                        "streaming fashion to target tables. This ensures fault tolerance and exactly-once "
                        "processing guarantees. Example: config['options']['checkpointLocation'] = '/path/to/checkpoint'"
                    )
                
                # Apply options if they exist
                # Create a copy to avoid mutating the original config (which may be reused for multiple tables)
                stream_options = {}
                if config.get("options") is not None and isinstance(config.get("options"), dict):
                    stream_options = config["options"].copy()
                    
                    if "checkpointLocation" in stream_options:
                        checkpoint_location = stream_options["checkpointLocation"].rstrip("/")
                        # Replace dots with underscores for path safety (table names like "db.table" become "db_table")
                        safe_table_name = table_name.replace(".", "_")
                        
                        # Only append table_name if it's not already the last segment of the path
                        # This prevents duplicate appends when the same config is reused
                        checkpoint_parts = checkpoint_location.split("/")
                        if not checkpoint_parts or checkpoint_parts[-1] != safe_table_name:
                            stream_options["checkpointLocation"] = f"{checkpoint_location}/{safe_table_name}"
                        _log.info(f"Using checkpoint location: {stream_options['checkpointLocation']}")
                    
                    # Only apply options if dict is not empty
                    if stream_options:
                        _df_stream_writer = _df_stream_writer.options(**stream_options)

                # Set trigger configuration if provided
                if config.get("trigger"):
                    trigger_config = config["trigger"]
                    if "processingTime" in trigger_config:
                        _df_stream_writer = _df_stream_writer.trigger(processingTime=trigger_config["processingTime"])
                    elif "once" in trigger_config and trigger_config["once"]:
                        _df_stream_writer = _df_stream_writer.trigger(once=True)
                    elif "continuous" in trigger_config:
                        _df_stream_writer = _df_stream_writer.trigger(continuous=trigger_config["continuous"])

                _log.info(f"Writing streaming records to table: {table_name}")
                streaming_query = _df_stream_writer.toTable(table_name)
                _log.info(f"Successfully started streaming write to table: {table_name}")
                
                # Set table properties for non-stats tables
                if not stats_table:
                    self._set_table_properties_with_retry(table_name)

                return streaming_query
            else:
                _log.info("Detected batch DataFrame, using write")
                
                # Batch DataFrame logic
                _df_writer = df.write

                if config.get("mode") is not None:
                    _df_writer = _df_writer.mode(config["mode"])
                if config.get("format") is not None:
                    _df_writer = _df_writer.format(config["format"])
                if config.get("partitionBy") is not None and config["partitionBy"] != []:
                    _df_writer = _df_writer.partitionBy(config["partitionBy"])
                if config.get("sortBy") is not None and config["sortBy"] != []:
                    _df_writer = _df_writer.sortBy(config["sortBy"])
                if config.get("bucketBy") is not None and config["bucketBy"] != {}:
                    bucket_by_config = config["bucketBy"]
                    _df_writer = _df_writer.bucketBy(bucket_by_config["numBuckets"], bucket_by_config["colName"])
                if config.get("options") is not None and config["options"] != {}:
                    _df_writer = _df_writer.options(**config["options"])

                _log.info(f"Writing records to table: {table_name}")

                if config.get("format") == "bigquery":
                    _df_writer.option("table", table_name).save()
                else:
                    _df_writer.saveAsTable(name=table_name)
                    _log.info(f"finished writing records to table: {table_name}")
                    
                    if not stats_table:
                        # Fetch table properties
                        table_properties = self.spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
                        # pylint: disable=not-an-iterable
                        table_properties_dict = {row["key"]: row["value"] for row in table_properties}

                        # Set product_id in table properties
                        if (
                            table_properties_dict.get("product_id") is None
                            or table_properties_dict.get("product_id") != self._context.product_id
                        ):
                            _log.info(f"product_id is not set for table {table_name} in tableproperties, setting it now")
                            self.spark.sql(
                                f"ALTER TABLE {table_name} SET TBLPROPERTIES ('product_id' = "
                                f"'{self._context.product_id}')"
                            )

                return None

        except Exception as e:
            raise SparkExpectationsUserInputOrConfigInvalidException(
                f"error occurred while writing data in to the table - {table_name}: {e}"
            )

    def get_row_dq_detailed_stats(
        self,
    ) -> List[Tuple[str, str, str, str, str, str, str, str, str, str, None, None, int, str, int, str, str,]]:
        """
        This function writes the detailed stats for row dq into the detailed stats table

        Args:
            df: Provide the dataframe which need to be written as a table
            rule_type: Provide the rule type for which the detailed stats need to be written

        Returns:
            List[]: List of tuples which consist of detailed stats for row dq

        """
        try:
            _run_id = self._context.get_run_id
            _product_id = self._context.product_id
            _table_name = self._context.get_table_name
            _input_count = self._context.get_input_count

            _row_dq_result = []

            _rowdq_expectations = self._context.get_dq_expectations
            _row_dq_expectations = _rowdq_expectations["row_dq_rules"]

            if self._context.get_summarized_row_dq_res is not None and len(self._context.get_summarized_row_dq_res) > 0:
                _row_dq_res = self._context.get_summarized_row_dq_res
                _dq_res = {d["rule"]: d["failed_row_count"] for d in _row_dq_res}

                for _rowdq_rule in _row_dq_expectations:
                    # if _rowdq_rule["rule"] in _dq_res:

                    failed_row_count = _dq_res[_rowdq_rule["rule"]]
                    _row_dq_result.append(
                        (
                            _run_id,
                            _product_id,
                            _table_name,
                            _rowdq_rule["rule_type"],
                            _rowdq_rule["rule"],
                            _rowdq_rule["column_name"],
                            _rowdq_rule["expectation"],
                            _rowdq_rule["tag"],
                            _rowdq_rule["description"],
                            "pass" if int(failed_row_count) == 0 else "fail",
                            None,
                            None,
                            ((_input_count - int(failed_row_count)) if int(failed_row_count) != 0 else _input_count),
                            failed_row_count,
                            _input_count,
                            (
                                self._context.get_row_dq_start_time.replace(tzinfo=timezone.utc).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                )
                                if self._context.get_row_dq_start_time
                                else "1900-01-01 00:00:00"
                            ),
                            (
                                self._context.get_row_dq_end_time.replace(tzinfo=timezone.utc).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                )
                                if self._context.get_row_dq_end_time
                                else "1900-01-01 00:00:00"
                            ),
                        )
                    )
            return _row_dq_result

        except Exception as e:
            raise SparkExpectationsMiscException(
                f"error occurred while fetching the stats from get_row_dq_detailed_stats {e}"
            )

    def _get_detailed_stats_result(
        self,
        agg_dq_detailed_stats_status: bool,
        dq_status: str,
        dq_detailed_stats: Optional[List[Tuple]],
    ) -> Optional[List[Tuple]]:
        """
        Returns the detailed statistics result based on the aggregation status and data quality status.

        Args:
            agg_dq_detailed_stats_status (bool): The status of the aggregated detailed statistics.
            dq_status (str): The status of the data quality.
            dq_detailed_stats (Optional[List[Tuple]]): The detailed statistics.

        Returns:
            Optional[List[Tuple]]: The detailed statistics result if the aggregation status is True
            and the data quality status is not "Skipped", otherwise an empty list.
        """
        if agg_dq_detailed_stats_status is True and dq_status != "Skipped":
            return dq_detailed_stats
        else:
            return []

    def _create_schema(self, field_names: List[str]) -> StructType:
        """
        Create a schema for the given field names.

        Args:
            field_names (List[str]): A list of field names.

        Returns:
            StructType: The created schema.
        """
        from pyspark.sql.types import (
            StructField,
            StringType,
        )

        return StructType([StructField(name, StringType(), True) for name in field_names])

    def _create_dataframe(self, data: Optional[List[Tuple]], schema: StructType) -> DataFrame:
        """
        Create a DataFrame from the given data and schema.

        Args:
            data (Optional[List[Tuple]]): The data to be converted into a DataFrame.
            schema (StructType): The schema of the DataFrame.

        Returns:
            DataFrame: The created DataFrame.
        """
        return self.spark.createDataFrame(data, schema=schema)

    def _prep_secondary_query_output(self) -> DataFrame:
        """
        Prepares the secondary query output by performing various transformations and joins.

        Returns:
            A DataFrame containing the secondary query output with the following columns:
            - run_id
            - product_id
            - table_name
            - rule
            - alias
            - dq_type
            - source_dq
            - run_date
            - compare
            - alias_comp
            - target_output
            - dq_time
            - dq_start_time
            - dq_end_time
        """
        _querydq_secondary_query_source_output = (
            self._context.get_source_query_dq_output
            if self._context.get_source_query_dq_output is not None and self._context.get_query_dq_detailed_stats_status
            else []
        )

        _querydq_secondary_query_target_output = (
            self._context.get_target_query_dq_output
            if self._context.get_target_query_dq_output is not None and self._context.get_query_dq_detailed_stats_status
            else []
        )

        _querydq_secondary_query_source_output.extend(_querydq_secondary_query_target_output)

        _custom_querydq_output_source_schema = self._create_schema(
            [
                "run_id",
                "product_id",
                "table_name",
                "rule",
                "column_name",
                "alias",
                "dq_type",
                "source_dq",
                "run_date",
            ]
        )

        _df_custom_detailed_stats_source = self.spark.createDataFrame(
            _querydq_secondary_query_source_output,
            schema=_custom_querydq_output_source_schema,
        )

        _df_custom_detailed_stats_source = _df_custom_detailed_stats_source.withColumn(
            "compare",
            # pylint: disable=unsubscriptable-object
            split(_df_custom_detailed_stats_source["alias"], "_").getItem(0),
        ).withColumn(
            "alias_comp",
            # pylint: disable=unsubscriptable-object
            split(_df_custom_detailed_stats_source["alias"], "_").getItem(1),
        )

        _df_custom_detailed_stats_source.createOrReplaceTempView("_df_custom_detailed_stats_source")

        _df_custom_detailed_stats_source = self._context.spark.sql(
            "select distinct source.run_id,source.product_id, source.table_name,"
            + "source.rule,source.column_name,source.alias,source.dq_type,source.source_dq as source_output,"
            + "target.source_dq as target_output from _df_custom_detailed_stats_source as source "
            + "left outer join _df_custom_detailed_stats_source as target "
            + "on source.run_id=target.run_id and source.product_id=target.product_id and "
            + "source.table_name=target.table_name and source.rule=target.rule and  "
            + "source.column_name = target.column_name and source.dq_type = target.dq_type "
            + "and source.alias_comp=target.alias_comp "
            + "and source.compare = 'source' and target.compare = 'target' "
        )

        _df_custom_detailed_stats_source = _df_custom_detailed_stats_source.withColumn(
            "dq_time", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )

        return _df_custom_detailed_stats_source

    def _prep_detailed_stats(
        self,
        _source_aggdq_detailed_stats_result: Optional[List[Tuple]],
        _source_querydq_detailed_stats_result: Optional[List[Tuple]],
        _target_aggdq_detailed_stats_result: Optional[List[Tuple]],
        _target_querydq_detailed_stats_result: Optional[List[Tuple]],
    ) -> DataFrame:
        """
        Prepares detailed statistics for the data quality checks.

        Args:
            _source_aggdq_detailed_stats_result (Optional[List[Tuple]]):
            Detailed statistics result for source aggregated data quality checks.
            _source_querydq_detailed_stats_result (Optional[List[Tuple]]):
            Detailed statistics result for source query-based data quality checks.
            _target_aggdq_detailed_stats_result (Optional[List[Tuple]]):
            Detailed statistics result for target aggregated data quality checks.
            _target_querydq_detailed_stats_result (Optional[List[Tuple]]):
            Detailed statistics result for target query-based data quality checks.

        Returns:
            Optional[List[Tuple]]: Detailed statistics for the data quality checks.
        """
        _detailed_stats_source_dq_schema = self._create_schema(
            [
                "run_id",
                "product_id",
                "table_name",
                "rule_type",
                "rule",
                "column_name",
                "source_expectations",
                "tag",
                "description",
                "source_dq_status",
                "source_dq_actual_outcome",
                "source_dq_expected_outcome",
                "source_dq_actual_row_count",
                "source_dq_error_row_count",
                "source_dq_row_count",
                "source_dq_start_time",
                "source_dq_end_time",
            ]
        )
        _detailed_stats_target_dq_schema = self._create_schema(
            [
                "run_id",
                "product_id",
                "table_name",
                "rule_type",
                "rule",
                "column_name",
                "target_expectations",
                "tag",
                "description",
                "target_dq_status",
                "target_dq_actual_outcome",
                "target_dq_expected_outcome",
                "target_dq_actual_row_count",
                "target_dq_error_row_count",
                "target_dq_row_count",
                "target_dq_start_time",
                "target_dq_end_time",
            ]
        )
        rules_execution_settings = self._context.get_rules_execution_settings_config
        _row_dq: bool = rules_execution_settings.get("row_dq", False)
        _target_agg_dq: bool = rules_execution_settings.get("target_agg_dq", False)

        _target_query_dq: bool = rules_execution_settings.get("target_query_dq", False)

        if _source_aggdq_detailed_stats_result is not None and _source_querydq_detailed_stats_result is not None:
            _source_aggdq_detailed_stats_result.extend(_source_querydq_detailed_stats_result)

        if self._context.get_row_dq_status != "Skipped" and _row_dq:
            _rowdq_detailed_stats_result = self.get_row_dq_detailed_stats()

        else:
            _rowdq_detailed_stats_result = []

        if _source_aggdq_detailed_stats_result is not None:
            _source_aggdq_detailed_stats_result.extend(_rowdq_detailed_stats_result)

        _df_source_aggquery_detailed_stats = self._create_dataframe(
            _source_aggdq_detailed_stats_result, _detailed_stats_source_dq_schema
        )

        if (
            (_target_agg_dq is True or _target_query_dq is True)
            and _row_dq is True
            and (_target_aggdq_detailed_stats_result is not None and _target_querydq_detailed_stats_result is not None)
        ):
            _target_aggdq_detailed_stats_result.extend(_target_querydq_detailed_stats_result)
        else:
            _target_aggdq_detailed_stats_result = []

        _df_target_aggquery_detailed_stats = self._create_dataframe(
            _target_aggdq_detailed_stats_result, _detailed_stats_target_dq_schema
        )

        _df_target_aggquery_detailed_stats = _df_target_aggquery_detailed_stats.select(
            *[col for col in _df_target_aggquery_detailed_stats.columns if col not in ["tag", "description"]]  # pylint: disable=not-an-iterable
        )

        _df_detailed_stats = _df_source_aggquery_detailed_stats.join(
            _df_target_aggquery_detailed_stats,
            ["run_id", "product_id", "table_name", "rule_type", "rule", "column_name"],
            "full_outer",
        )

        _df_detailed_stats = _df_detailed_stats.withColumn("dq_date", current_date()).withColumn(
            "dq_time", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )

        _df_detailed_stats = _df_detailed_stats.withColumn(
            "dq_job_metadata_info", lit(self._context.get_job_metadata).cast("string")
        )

        return _df_detailed_stats

    def write_detailed_stats(self) -> None:
        """
        This functions writes the detailed stats for all rule type into the detailed stats table

        Args:
            config: Provide the config to write the dataframe into the table

        Returns:
            None:

        """
        try:
            self.spark.conf.set("spark.sql.session.timeZone", "Etc/UTC")

            _source_aggdq_detailed_stats_result = self._get_detailed_stats_result(
                self._context.get_agg_dq_detailed_stats_status,
                self._context.get_source_agg_dq_status,
                self._context.get_source_agg_dq_detailed_stats,
            )

            _target_aggdq_detailed_stats_result = self._get_detailed_stats_result(
                self._context.get_agg_dq_detailed_stats_status,
                self._context.get_final_agg_dq_status,
                self._context.get_target_agg_dq_detailed_stats,
            )

            _source_querydq_detailed_stats_result = self._get_detailed_stats_result(
                self._context.get_query_dq_detailed_stats_status,
                self._context.get_source_query_dq_status,
                self._context.get_source_query_dq_detailed_stats,
            )

            _target_querydq_detailed_stats_result = self._get_detailed_stats_result(
                self._context.get_query_dq_detailed_stats_status,
                self._context.get_final_query_dq_status,
                self._context.get_target_query_dq_detailed_stats,
            )

            _df_detailed_stats = self._prep_detailed_stats(
                _source_aggdq_detailed_stats_result,
                _source_querydq_detailed_stats_result,
                _target_aggdq_detailed_stats_result,
                _target_querydq_detailed_stats_result,
            )

            self._context.print_dataframe_with_debugger(_df_detailed_stats)

            _log.info(
                "Writing metrics to the detailed stats table: {self._context.get_dq_detailed_stats_table_name}, started"
            )

            self.save_df_as_table(
                _df_detailed_stats,
                self._context.get_dq_detailed_stats_table_name,
                config=self._context.get_detailed_stats_table_writer_config,
                stats_table=True,
            )

            _log.info(
                f"Writing metrics to the detailed stats table: {self._context.get_dq_detailed_stats_table_name}, ended"
            )

            # TODO Create a separate function for writing the custom query dq stats
            _df_custom_detailed_stats_source = self._prep_secondary_query_output()

            _log.info(
                "Writing metrics to the output custom table: {self._context.get_query_dq_output_custom_table_name}, started"
            )

            self.save_df_as_table(
                _df_custom_detailed_stats_source,
                self._context.get_query_dq_output_custom_table_name,
                config=self._context.get_detailed_stats_table_writer_config,
                stats_table=True,
            )

            _log.info(
                "Writing metrics to the output custom table: {self._context.get_query_dq_output_custom_table_name}, ended"
            )
        except Exception as e:
            raise SparkExpectationsMiscException(f"error occurred while saving the data into the stats table {e}")

        if self._context.get_enable_obs_dq_report_result is True:
            context = self._context
            context.set_stats_detailed_dataframe(_df_detailed_stats)
            context.set_custom_detailed_dataframe(_df_custom_detailed_stats_source)
            from spark_expectations.sinks.utils.report import SparkExpectationsReport

            report = SparkExpectationsReport(_context=context)
            _log.info("report being called")
            (
                dq_obs_rpt_gen_status_flag,
                df_report_table,
            ) = report.dq_obs_report_data_insert()
            df_report_table.show(truncate=False)
            if dq_obs_rpt_gen_status_flag is True:
                context.set_dq_obs_rpt_gen_status_flag(True)
            _log.info("set_dq_obs_rpt_gen_status_flag")
            context.set_df_dq_obs_report_dataframe(df_report_table)
        # calling only alert
        if self._context.get_se_dq_obs_alert_flag is True:
            _log.info("alert being called")
            alert = SparkExpectationsAlert(self._context)
            alert.prep_report_data()

    def get_kafka_write_options(self, se_stats_dict: dict) -> dict:
        """Gets Kafka write configuration options based on runtime environment and config settings"""

        if self._context.get_env == "local":
            return {
                "kafka.bootstrap.servers": "localhost:9092",
                "topic": f"{self._context.get_topic_name}",
                "failOnDataLoss": "true",
            }
        
        secret_handler = SparkExpectationsSecretsBackend(se_stats_dict)
        
        if self._context.get_se_streaming_stats_kafka_custom_config_enable:
            options = {
                "kafka.bootstrap.servers": f"{self._context.get_se_streaming_stats_kafka_bootstrap_server}",
                "kafka.security.protocol": "SASL_SSL",
                "kafka.sasl.mechanism": "OAUTHBEARER",
                "kafka.sasl.jaas.config": f"""kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="{secret_handler.get_secret(self._context.get_client_id)}" clientSecret="{secret_handler.get_secret(self._context.get_token)}";""",
                "kafka.sasl.login.callback.handler.class": "kafkashaded.org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler",
                "topic": f"{self._context.get_topic_name}",
            }

        elif self._context.get_dbr_version and self._context.get_dbr_version >= 13.3:
            options = {
                "kafka.bootstrap.servers": f"{secret_handler.get_secret(self._context.get_server_url_key)}",
                "kafka.security.protocol": "SASL_SSL",
                "kafka.sasl.mechanism": "OAUTHBEARER",
                "kafka.sasl.jaas.config": f"""kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="{secret_handler.get_secret(self._context.get_client_id)}" clientSecret="{secret_handler.get_secret(self._context.get_token)}";""",
                "kafka.sasl.oauthbearer.token.endpoint.url": f"{secret_handler.get_secret(self._context.get_token_endpoint_url)}",
                "kafka.sasl.login.callback.handler.class": "kafkashaded.org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler",
                "topic": f"{secret_handler.get_secret(self._context.get_topic_name)}",
            }
        else:
            options = {
                "kafka.bootstrap.servers": f"{secret_handler.get_secret(self._context.get_server_url_key)}",
                "kafka.security.protocol": "SASL_SSL",
                "kafka.sasl.mechanism": "OAUTHBEARER",
                "kafka.sasl.jaas.config": f"""kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.client.id='{secret_handler.get_secret(self._context.get_client_id)}'  oauth.client.secret='{secret_handler.get_secret(self._context.get_token)}' oauth.token.endpoint.uri='{secret_handler.get_secret(self._context.get_token_endpoint_url)}'; """,
                "kafka.sasl.login.callback.handler.class": "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler",
                "topic": f"{secret_handler.get_secret(self._context.get_topic_name)}",
            }

        return options

    def write_error_stats(self) -> None:
        """
        This functions takes the stats table and write it into error table

        Args:
            config: Provide the config to write the dataframe into the table

        Returns:
            None:

        """
        try:
            self.spark.conf.set("spark.sql.session.timeZone", "Etc/UTC")
            from datetime import date

            table_name: str = self._context.get_table_name
            input_count: int = self._context.get_input_count
            error_count: int = self._context.get_error_count
            output_count: int = self._context.get_output_count

            source_agg_dq_result: Optional[List[Dict[str, str]]] = self._context.get_source_agg_dq_result
            final_agg_dq_result: Optional[List[Dict[str, str]]] = self._context.get_final_agg_dq_result
            source_query_dq_result: Optional[List[Dict[str, str]]] = self._context.get_source_query_dq_result
            final_query_dq_result: Optional[List[Dict[str, str]]] = self._context.get_final_query_dq_result

            error_stats_data = [
                (
                    self._context.product_id,
                    table_name,
                    input_count,
                    error_count,
                    output_count,
                    self._context.get_output_percentage,
                    self._context.get_success_percentage,
                    self._context.get_error_percentage,
                    (source_agg_dq_result if source_agg_dq_result and len(source_agg_dq_result) > 0 else None),
                    (final_agg_dq_result if final_agg_dq_result and len(final_agg_dq_result) > 0 else None),
                    (source_query_dq_result if source_query_dq_result and len(source_query_dq_result) > 0 else None),
                    (final_query_dq_result if final_query_dq_result and len(final_query_dq_result) > 0 else None),
                    self._context.get_summarized_row_dq_res,
                    self._context.get_rules_exceeds_threshold,
                    {
                        "run_status": self._context.get_dq_run_status,
                        "source_agg_dq": self._context.get_source_agg_dq_status,
                        "source_query_dq": self._context.get_source_query_dq_status,
                        "row_dq": self._context.get_row_dq_status,
                        "final_agg_dq": self._context.get_final_agg_dq_status,
                        "final_query_dq": self._context.get_final_query_dq_status,
                    },
                    {
                        "run_time": self._context.get_dq_run_time,
                        "source_agg_dq_run_time": self._context.get_source_agg_dq_run_time,
                        "source_query_dq_run_time": self._context.get_source_query_dq_run_time,
                        "row_dq_run_time": self._context.get_row_dq_run_time,
                        "final_agg_dq_run_time": self._context.get_final_agg_dq_run_time,
                        "final_query_dq_run_time": self._context.get_final_query_dq_run_time,
                    },
                    {
                        "rules": {
                            "num_row_dq_rules": self._context.get_num_row_dq_rules,
                            "num_dq_rules": self._context.get_num_dq_rules,
                        },
                        "agg_dq_rules": self._context.get_num_agg_dq_rules,
                        "query_dq_rules": self._context.get_num_query_dq_rules,
                    },
                    self._context.get_run_id,
                    date.fromisoformat(self._context.get_run_date[0:10]),
                    datetime.strptime(
                        self._context.get_run_date,
                        "%Y-%m-%d %H:%M:%S",
                    ),
                )
            ]

            from pyspark.sql.types import (
                StructField,
                StringType,
                IntegerType,
                LongType,
                FloatType,
                DateType,
                ArrayType,
                MapType,
                TimestampType,
            )

            error_stats_schema = StructType(
                [
                    StructField("product_id", StringType(), True),
                    StructField("table_name", StringType(), True),
                    StructField("input_count", LongType(), True),
                    StructField("error_count", LongType(), True),
                    StructField("output_count", LongType(), True),
                    StructField("output_percentage", FloatType(), True),
                    StructField("success_percentage", FloatType(), True),
                    StructField("error_percentage", FloatType(), True),
                    StructField(
                        "source_agg_dq_results",
                        ArrayType(MapType(StringType(), StringType())),
                        True,
                    ),
                    StructField(
                        "final_agg_dq_results",
                        ArrayType(MapType(StringType(), StringType())),
                        True,
                    ),
                    StructField(
                        "source_query_dq_results",
                        ArrayType(MapType(StringType(), StringType())),
                        True,
                    ),
                    StructField(
                        "final_query_dq_results",
                        ArrayType(MapType(StringType(), StringType())),
                        True,
                    ),
                    StructField(
                        "row_dq_res_summary",
                        ArrayType(MapType(StringType(), StringType())),
                        True,
                    ),
                    StructField(
                        "row_dq_error_threshold",
                        ArrayType(MapType(StringType(), StringType())),
                        True,
                    ),
                    StructField("dq_status", MapType(StringType(), StringType()), True),
                    StructField("dq_run_time", MapType(StringType(), FloatType()), True),
                    StructField(
                        "dq_rules",
                        MapType(StringType(), MapType(StringType(), IntegerType())),
                        True,
                    ),
                    StructField(self._context.get_run_id_name, StringType(), True),
                    StructField(self._context.get_run_date_name, DateType(), True),
                    StructField(self._context.get_run_date_time_name, TimestampType(), True),
                ]
            )

            df = self.spark.createDataFrame(error_stats_data, schema=error_stats_schema)
            self._context.print_dataframe_with_debugger(df)

            # get env from user config
            dq_env = "" if "env" not in self._context.get_dq_rules_params else self._context.get_dq_rules_params["env"]

            df = (
                df.withColumn("output_percentage", sql_round(df.output_percentage, 2))
                .withColumn("success_percentage", sql_round(df.success_percentage, 2))
                .withColumn("error_percentage", sql_round(df.error_percentage, 2))
                .withColumn("dq_env", lit(dq_env))
            )

            self._context.set_stats_dict(df)
            _log.info(f"Writing metrics to the stats table: {self._context.get_dq_stats_table_name}, started")
            if self._context.get_stats_table_writer_config["format"] == "bigquery":
                df = df.withColumn("dq_rules", to_json(df["dq_rules"]))

            self.save_df_as_table(
                df,
                self._context.get_dq_stats_table_name,
                config=self._context.get_stats_table_writer_config,
                stats_table=True,
            )

            _log.info(f"Writing metrics to the stats table: {self._context.get_dq_stats_table_name}, ended")

            if (
                self._context.get_agg_dq_detailed_stats_status is True
                or self._context.get_query_dq_detailed_stats_status is True
            ):

                self.write_detailed_stats()

                # TODO Implement the below function for writing the custom query dq stats
                # if self._context.get_query_dq_detailed_stats_status is True:
                #     self.write_query_dq_custom_output()

            # TODO check if streaming_stats is set to off, if it's enabled only then this should run

            _se_stats_dict = self._context.get_se_streaming_stats_dict
            if _se_stats_dict[user_config.se_enable_streaming]:
                try:
                    _log.info("Attempting to write stats to Kafka...")
                    kafka_write_options: dict = self.get_kafka_write_options(_se_stats_dict)
                    _sink_hook.writer(
                        _write_args={
                            "product_id": self._context.product_id,
                            "enable_se_streaming": _se_stats_dict[user_config.se_enable_streaming],
                            "kafka_write_options": kafka_write_options,
                            "stats_df": df,
                        }
                    )
                    self._context.set_kafka_write_status("Success")
                    _log.info("Successfully wrote stats to Kafka")
                except Exception as kafka_error:
                    # Log the detailed error and set status for notifications
                    error_message = str(kafka_error)
                    _log.error(f"Failed to write stats to Kafka: {error_message}")
                    self._context.set_kafka_write_status("Failed")
                    self._context.set_kafka_write_error_message(error_message)
                    # Re-raise the exception to fail the job with enhanced error message
                    raise kafka_error
            else:
                _log.info("Streaming stats to kafka is disabled, hence skipping writing to kafka")
                self._context.set_kafka_write_status("Disabled")

        except Exception as e:
            raise SparkExpectationsMiscException(f"error occurred while saving the data into the stats table {e}")

    def write_error_records_final(self, df: DataFrame, error_table: str, rule_type: str) -> Tuple[int, DataFrame]:
        try:
            _log.info("_write_error_records_final started")

            # Find columns matching the rule_type prefix
            rule_type_columns = [_col for _col in df.columns if _col.startswith(f"{rule_type}")]
            if not rule_type_columns:
                error_msg = (
                    f"No columns found in DataFrame matching rule_type prefix '{rule_type}'. "
                    "Cannot proceed with error record writing. "
                    "Check that rule evaluation has produced the expected columns."
                )
                _log.error(error_msg)
                raise SparkExpectationsMiscException(error_msg)

            failed_records = [f"size({dq_column}) != 0" for dq_column in rule_type_columns]

            failed_records_rules = " or ".join(failed_records)
            # df = df.filter(expr(failed_records_rules))

            df = df.withColumn(
                f"meta_{rule_type}_results",
                when(
                    expr(failed_records_rules),
                    array(*rule_type_columns),
                ).otherwise(array(create_map())),
            ).drop(*rule_type_columns)

            df = (
                df.withColumn(
                    f"meta_{rule_type}_results",
                    remove_passing_status_maps(df[f"meta_{rule_type}_results"]),
                )
                .withColumn(self._context.get_run_id_name, lit(self._context.get_run_id))
                .withColumn(
                    self._context.get_run_date_time_name,
                    lit(self._context.get_run_date),
                )
            )
            error_df = df.filter(f"size(meta_{rule_type}_results) != 0")
            self._context.print_dataframe_with_debugger(error_df)

            print(f"self._context.get_se_enable_error_table : {self._context.get_se_enable_error_table}")
            if self._context.get_se_enable_error_table:
                self.save_df_as_table(
                    error_df,
                    error_table,
                    self._context.get_target_and_error_table_writer_config,
                )

            _error_count = error_df.count() if not error_df.isStreaming else 0
            # if _error_count > 0:
            self.generate_summarized_row_dq_res(error_df, rule_type)

            _log.info("_write_error_records_final ended")
            return _error_count, df

        except Exception as e:
            raise SparkExpectationsMiscException(f"error occurred while saving data into the final error table {e}")

    def generate_summarized_row_dq_res(self, df: DataFrame, rule_type: str) -> None:
        """
        This function implements/supports summarizing row dq error result
        Args:
            df: error dataframe(DataFrame)
            rule_type: type of the rule(str)

        Returns:
            None

        """
        if df.isStreaming:
            _log.info("Skipping summarized row dq res generation for streaming DataFrame")
            return
        
        try:
            df_explode = df.select(explode(f"meta_{rule_type}_results").alias("row_dq_res"))
            df_res = (
                df_explode.withColumn("rule_type", col("row_dq_res")["rule_type"])
                .withColumn("rule", col("row_dq_res")["rule"])
                .withColumn("priority", col("row_dq_res")["priority"])
                .withColumn("description", col("row_dq_res")["description"])
                .withColumn("tag", col("row_dq_res")["tag"])
                .withColumn("action_if_failed", col("row_dq_res")["action_if_failed"])
                .withColumn("column_name", col("row_dq_res")["column_name"])
                .select("rule_type", "rule", "priority", "column_name", "description", "tag", "action_if_failed")
                .groupBy("rule_type", "rule", "priority", "column_name", "description", "tag", "action_if_failed")
                .count()
                .withColumnRenamed("count", "failed_row_count")
            )
            summarized_row_dq_list = [
                {
                    "rule_type": row.rule_type,
                    "rule": row.rule,
                    "priority": row.priority,
                    "column_name": row.column_name,
                    "description": row.description,
                    "tag": row.tag,
                    "action_if_failed": row.action_if_failed,
                    "failed_row_count": row.failed_row_count,
                }
                for row in df_res.select(
                    "rule_type",
                    "rule",
                    "priority",
                    "column_name",
                    "description",
                    "tag",
                    "action_if_failed",
                    "failed_row_count",
                ).collect()
            ]
            failed_rule_list = []
            for failed_rule in summarized_row_dq_list:
                failed_rule_list.append(failed_rule["rule"])

            for (
                each_rule_type,
                all_rule_type_rules,
            ) in self._context.get_dq_expectations.items():
                if each_rule_type in ["row_dq_rules"]:
                    for each_rule in all_rule_type_rules:
                        if each_rule["rule"] not in failed_rule_list:
                            summarized_row_dq_list.append(
                                {
                                    "rule_type": each_rule["rule_type"],
                                    "rule": each_rule["rule"],
                                    "priority": each_rule["priority"],
                                    "column_name": each_rule["column_name"],
                                    "description": each_rule["description"],
                                    "tag": each_rule["tag"],
                                    "action_if_failed": each_rule["action_if_failed"],
                                    "failed_row_count": 0
                                }
                            )

            self._context.set_summarized_row_dq_res(summarized_row_dq_list)

        except Exception as e:
            raise SparkExpectationsMiscException(f"error occurred created summarized row dq statistics {e}")

    def generate_rules_exceeds_threshold(self, rules: dict) -> None:
        """
        This function implements/supports summarizing row dq error threshold
        Args:
            rules: accepts rule metadata within dict
        Returns:
            None
        """
        try:
            error_threshold_list = []
            rules_failed_row_count: Dict[str, int] = {}
            if self._context.get_summarized_row_dq_res is None:
                return None

            rules_failed_row_count = {
                itr["rule"]: int(itr["failed_row_count"]) for itr in self._context.get_summarized_row_dq_res
            }

            for rule in rules[f"{self._context.get_row_dq_rule_type_name}_rules"]:
                # if (
                #         not rule["enable_error_drop_alert"]
                #         or rule["rule"] not in rules_failed_row_count.keys()
                # ):
                #     continue  # pragma: no cover
                rule_name = rule["rule"]
                rule_action = rule["action_if_failed"]
                if rule_name in rules_failed_row_count.keys():
                    failed_row_count = int(rules_failed_row_count[rule_name])
                else:
                    failed_row_count = 0

                if failed_row_count is not None and failed_row_count > 0:
                    error_drop_percentage = round((failed_row_count / self._context.get_input_count) * 100, 2)
                    error_threshold_list.append(
                        {
                            "rule_name": rule_name,
                            "column_name": rule["column_name"],
                            "action_if_failed": rule_action,
                            "description": rule["description"],
                            "rule_type": rule["rule_type"],
                            "error_drop_threshold": str(rule["error_drop_threshold"]),
                            "error_drop_percentage": str(error_drop_percentage),
                        }
                    )

            if len(error_threshold_list) > 0:
                self._context.set_rules_exceeds_threshold(error_threshold_list)

        except Exception as e:
            raise SparkExpectationsMiscException(f"An error occurred while creating error threshold list : {e}")

    def get_streaming_query_status(self, streaming_query: StreamingQuery) -> Dict[str, Any]:
        """
        Get status information for a streaming query
        
        Args:
            streaming_query: The streaming query to check
            
        Returns:
            Dict[str, Any]: Status information including query details. Always includes:
                - query_id: Unique identifier for the query
                - run_id: Run identifier for the query
                - name: Name of the query
                - is_active: Boolean indicating if query is active
                - status: "active", "inactive", "not_running", or "error"
                
            For active queries with progress data, may also include:
                - batch_id: Non-negative integer representing the batch number
                - input_rows_per_second: Input rate
                - processed_rows_per_second: Processing rate
                - batch_duration: Duration of the batch in milliseconds
                - timestamp: Timestamp of the progress update
                
            For inactive queries with errors:
                - error: Error message from the query exception
                
            Note: Progress fields are only included when actually present in the
            streaming query progress. Batch IDs are always non-negative integers.
        """
        try:
            if streaming_query is None:
                return {"status": "not_running", "message": "No streaming query provided"}
                
            status = {
                "query_id": streaming_query.id,
                "run_id": streaming_query.runId,
                "name": streaming_query.name,
                "is_active": streaming_query.isActive,
                "status": "active" if streaming_query.isActive else "inactive"
            }
            
            if streaming_query.isActive:
                progress = streaming_query.lastProgress
                if progress:
                    # Only include fields that are actually present in progress
                    # Batch IDs are always non-negative, so we use None for missing values
                    if "batchId" in progress:
                        status["batch_id"] = progress["batchId"]
                    
                    if "inputRowsPerSecond" in progress:
                        status["input_rows_per_second"] = progress["inputRowsPerSecond"]
                    
                    if "processedRowsPerSecond" in progress:
                        status["processed_rows_per_second"] = progress["processedRowsPerSecond"]
                    
                    if "batchDuration" in progress:
                        status["batch_duration"] = progress["batchDuration"]
                    
                    if "timestamp" in progress:
                        status["timestamp"] = progress["timestamp"]
            else:
                # Query is not active, check for exception
                try:
                    exception = streaming_query.exception()
                    if exception:
                        status["error"] = str(exception)
                except Exception:
                    pass
                    
            return status
            
        except Exception as e:
            return {"status": "error", "message": f"Error getting query status: {e}"}

    def stop_streaming_query(self, streaming_query: StreamingQuery, timeout: Optional[int] = None) -> bool:
        """
        Stop a streaming query gracefully
        
        Args:
            streaming_query: The streaming query to stop
            timeout: Optional timeout in seconds
            
        Returns:
            bool: True if stopped successfully, False otherwise
        """
        try:
            if streaming_query is None or not streaming_query.isActive:
                _log.info("Streaming query is not active or None")
                return True
                
            _log.info(f"Stopping streaming query: {streaming_query.id}")
            
            if timeout:
                streaming_query.stop()
                streaming_query.awaitTermination(timeout)
            else:
                streaming_query.stop()
                
            _log.info(f"Successfully stopped streaming query: {streaming_query.id}")
            return True
            
        except Exception as e:
            _log.error(f"Error stopping streaming query: {e}")
            return False
