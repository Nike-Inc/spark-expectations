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
    monotonically_increasing_id,
    coalesce,
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
class SparkExpectationsWriter:
    """
    This class implements/supports writing data into the sink system
    """

    _context: SparkExpectationsContext

    def __post_init__(self) -> None:
        self.spark = self._context.spark

    def save_df_as_table(
        self, df: DataFrame, table_name: str, config: dict, stats_table: bool = False
    ) -> None:
        """
        This function takes a dataframe and writes into a table

        Args:
            df: Provide the dataframe which need to be written as a table
            table_name: Provide the table name to which the dataframe need to be written to
            config: Provide the config to write the dataframe into the table
            stats_table: Provide if this is for writing stats table

        Returns:
            None:

        """
        try:
            if not stats_table:
                df = df.withColumn(
                    self._context.get_run_id_name, lit(f"{self._context.get_run_id}")
                ).withColumn(
                    self._context.get_run_date_time_name,
                    to_timestamp(
                        lit(f"{self._context.get_run_date}"), "yyyy-MM-dd HH:mm:ss"
                    ),
                )
            _log.info("_save_df_as_table started")

            _df_writer = df.write

            if config["mode"] is not None:
                _df_writer = _df_writer.mode(config["mode"])
            if config["format"] is not None:
                _df_writer = _df_writer.format(config["format"])
            if config["partitionBy"] is not None and config["partitionBy"] != []:
                _df_writer = _df_writer.partitionBy(config["partitionBy"])
            if config["sortBy"] is not None and config["sortBy"] != []:
                _df_writer = _df_writer.sortBy(config["sortBy"])
            if config["bucketBy"] is not None and config["bucketBy"] != {}:
                bucket_by_config = config["bucketBy"]
                _df_writer = _df_writer.bucketBy(
                    bucket_by_config["numBuckets"], bucket_by_config["colName"]
                )
            if config["options"] is not None and config["options"] != {}:
                _df_writer = _df_writer.options(**config["options"])

            _log.info("Writing records to table: %s", table_name)

            if config["format"] == "bigquery":
                _df_writer.option("table", table_name).save()
            else:
                _df_writer.saveAsTable(name=table_name)
                _log.info("finished writing records to table: %s,", table_name)
                if not stats_table:
                    # Fetch table properties
                    table_properties = self.spark.sql(
                        f"SHOW TBLPROPERTIES {table_name}"
                    ).collect()
                    table_properties_dict = {
                        row["key"]: row["value"] for row in table_properties
                    }

                    # Set product_id in table properties
                    if (
                        table_properties_dict.get("product_id") is None
                        or table_properties_dict.get("product_id")
                        != self._context.product_id
                    ):
                        _log.info(
                            "product_id is not set for table %s in tableproperties, setting it now",
                            table_name,
                        )
                        self.spark.sql(
                            f"ALTER TABLE {table_name} SET TBLPROPERTIES ('product_id' = "
                            f"'{self._context.product_id}')"
                        )

        except Exception as e:
            raise SparkExpectationsUserInputOrConfigInvalidException(
                f"error occurred while writing data in to the table - {table_name}: {e}"
            )

    def get_row_dq_detailed_stats(
        self,
    ) -> List[
        Tuple[
            str,
            str,
            str,
            str,
            str,
            str,
            str,
            str,
            str,
            None,
            None,
            int,
            str,
            int,
            str,
            str,
        ]
    ]:
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

            if (
                self._context.get_summarized_row_dq_res is not None
                and len(self._context.get_summarized_row_dq_res) > 0
            ):
                _row_dq_res = self._context.get_summarized_row_dq_res
                _dq_res = {d["rule"]: d["failed_row_count"] for d in _row_dq_res}

                for _rowdq_rule in _row_dq_expectations:
                    if _rowdq_rule["rule"] in _dq_res:
                        failed_row_count = _dq_res[_rowdq_rule["rule"]]
                        _row_dq_result.append(
                            (
                                _run_id,
                                _product_id,
                                _table_name,
                                _rowdq_rule["rule_type"],
                                _rowdq_rule["rule"],
                                _rowdq_rule["expectation"],
                                _rowdq_rule["tag"],
                                _rowdq_rule["description"],
                                "pass" if int(failed_row_count) == 0 else "fail",
                                None,
                                None,
                                (_input_count - int(failed_row_count)),
                                failed_row_count,
                                _input_count,
                                self._context.get_row_dq_start_time.replace(
                                    tzinfo=timezone.utc
                                ).strftime("%Y-%m-%d %H:%M:%S")
                                if self._context.get_row_dq_start_time
                                else "1900-01-01 00:00:00",
                                self._context.get_row_dq_end_time.replace(
                                    tzinfo=timezone.utc
                                ).strftime("%Y-%m-%d %H:%M:%S")
                                if self._context.get_row_dq_end_time
                                else "1900-01-01 00:00:00",
                            )
                        )
                        _row_dq_expectations.remove(_rowdq_rule)

            for _rowdq_rule in _row_dq_expectations:
                _row_dq_result.append(
                    (
                        _run_id,
                        _product_id,
                        _table_name,
                        _rowdq_rule["rule_type"],
                        _rowdq_rule["rule"],
                        _rowdq_rule["expectation"],
                        _rowdq_rule["tag"],
                        _rowdq_rule["description"],
                        "pass",
                        None,
                        None,
                        _input_count,
                        "0",
                        _input_count,
                        self._context.get_row_dq_start_time.replace(
                            tzinfo=timezone.utc
                        ).strftime("%Y-%m-%d %H:%M:%S")
                        if self._context.get_row_dq_start_time
                        else "1900-01-01 00:00:00",
                        self._context.get_row_dq_end_time.replace(
                            tzinfo=timezone.utc
                        ).strftime("%Y-%m-%d %H:%M:%S")
                        if self._context.get_row_dq_end_time
                        else "1900-01-01 00:00:00",
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

        return StructType(
            [StructField(name, StringType(), True) for name in field_names]
        )

    def _create_dataframe(
        self, data: Optional[List[Tuple]], schema: StructType
    ) -> DataFrame:
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
            if self._context.get_source_query_dq_output is not None
            and self._context.get_query_dq_detailed_stats_status
            else []
        )

        _querydq_secondary_query_target_output = (
            self._context.get_target_query_dq_output
            if self._context.get_target_query_dq_output is not None
            and self._context.get_query_dq_detailed_stats_status
            else []
        )

        _querydq_secondary_query_source_output.extend(
            _querydq_secondary_query_target_output
        )

        _custom_querydq_output_source_schema = self._create_schema(
            [
                "run_id",
                "product_id",
                "table_name",
                "rule",
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

        _df_custom_detailed_stats_source.createOrReplaceTempView(
            "_df_custom_detailed_stats_source"
        )

        _df_custom_detailed_stats_source = self._context.spark.sql(
            "select distinct source.run_id,source.product_id, source.table_name,"
            + "source.rule,source.alias,source.dq_type,source.source_dq as source_output,"
            + "target.source_dq as target_output from _df_custom_detailed_stats_source as source "
            + "left outer join _df_custom_detailed_stats_source as target "
            + "on source.run_id=target.run_id and source.product_id=target.product_id and "
            + "source.table_name=target.table_name and source.rule=target.rule "
            + "and source.dq_type = target.dq_type "
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

        if (
            _source_aggdq_detailed_stats_result is not None
            and _source_querydq_detailed_stats_result is not None
        ):
            _source_aggdq_detailed_stats_result.extend(
                _source_querydq_detailed_stats_result
            )

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
            and (
                _target_aggdq_detailed_stats_result is not None
                and _target_querydq_detailed_stats_result is not None
            )
        ):
            _target_aggdq_detailed_stats_result.extend(
                _target_querydq_detailed_stats_result
            )
        else:
            _target_aggdq_detailed_stats_result = []

        _df_target_aggquery_detailed_stats = self._create_dataframe(
            _target_aggdq_detailed_stats_result, _detailed_stats_target_dq_schema
        )

        _df_target_aggquery_detailed_stats = _df_target_aggquery_detailed_stats.select(
            *[
                col
                for col in _df_target_aggquery_detailed_stats.columns
                if col not in ["tag", "description"]
            ]
        )

        _df_detailed_stats = _df_source_aggquery_detailed_stats.join(
            _df_target_aggquery_detailed_stats,
            ["run_id", "product_id", "table_name", "rule_type", "rule"],
            "full_outer",
        )

        _df_detailed_stats = _df_detailed_stats.withColumn(
            "dq_date", current_date()
        ).withColumn("dq_time", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

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
                "Writing metrics to the detailed stats table: %s, started",
                self._context.get_dq_detailed_stats_table_name,
            )

            self.save_df_as_table(
                _df_detailed_stats,
                self._context.get_dq_detailed_stats_table_name,
                config=self._context.get_detailed_stats_table_writer_config,
                stats_table=True,
            )

            _log.info(
                "Writing metrics to the detailed stats table: %s, ended",
                self._context.get_dq_detailed_stats_table_name,
            )

            # TODO Create a separate function for writing the custom query dq stats
            _df_custom_detailed_stats_source = self._prep_secondary_query_output()

            _log.info(
                "Writing metrics to the output custom table: %s, started",
                self._context.get_query_dq_output_custom_table_name,
            )

            self.save_df_as_table(
                _df_custom_detailed_stats_source,
                self._context.get_query_dq_output_custom_table_name,
                config=self._context.get_detailed_stats_table_writer_config,
                stats_table=True,
            )

            _log.info(
                "Writing metrics to the output custom table: %s, ended",
                self._context.get_query_dq_output_custom_table_name,
            )

        except Exception as e:
            raise SparkExpectationsMiscException(
                f"error occurred while saving the data into the stats table {e}"
            )

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
            source_agg_dq_result: Optional[
                List[Dict[str, str]]
            ] = self._context.get_source_agg_dq_result
            final_agg_dq_result: Optional[
                List[Dict[str, str]]
            ] = self._context.get_final_agg_dq_result
            source_query_dq_result: Optional[
                List[Dict[str, str]]
            ] = self._context.get_source_query_dq_result
            final_query_dq_result: Optional[
                List[Dict[str, str]]
            ] = self._context.get_final_query_dq_result

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
                    (
                        source_agg_dq_result
                        if source_agg_dq_result and len(source_agg_dq_result) > 0
                        else None
                    ),
                    (
                        final_agg_dq_result
                        if final_agg_dq_result and len(final_agg_dq_result) > 0
                        else None
                    ),
                    (
                        source_query_dq_result
                        if source_query_dq_result and len(source_query_dq_result) > 0
                        else None
                    ),
                    (
                        final_query_dq_result
                        if final_query_dq_result and len(final_query_dq_result) > 0
                        else None
                    ),
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
                    StructField(
                        "dq_run_time", MapType(StringType(), FloatType()), True
                    ),
                    StructField(
                        "dq_rules",
                        MapType(StringType(), MapType(StringType(), IntegerType())),
                        True,
                    ),
                    StructField(self._context.get_run_id_name, StringType(), True),
                    StructField(self._context.get_run_date_name, DateType(), True),
                    StructField(
                        self._context.get_run_date_time_name, TimestampType(), True
                    ),
                ]
            )

            df = self.spark.createDataFrame(error_stats_data, schema=error_stats_schema)
            self._context.print_dataframe_with_debugger(df)

            df = (
                df.withColumn("output_percentage", sql_round(df.output_percentage, 2))
                .withColumn("success_percentage", sql_round(df.success_percentage, 2))
                .withColumn("error_percentage", sql_round(df.error_percentage, 2))
            )
            _log.info(
                "Writing metrics to the stats table: %s, started",
                self._context.get_dq_stats_table_name,
            )
            if self._context.get_stats_table_writer_config["format"] == "bigquery":
                df = df.withColumn("dq_rules", to_json(df["dq_rules"]))

            self.save_df_as_table(
                df,
                self._context.get_dq_stats_table_name,
                config=self._context.get_stats_table_writer_config,
                stats_table=True,
            )

            _log.info(
                "Writing metrics to the stats table: %s, ended",
                self._context.get_dq_stats_table_name,
            )

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
            if _se_stats_dict["se.enable.streaming"]:
                secret_handler = SparkExpectationsSecretsBackend(
                    secret_dict=_se_stats_dict
                )
                kafka_write_options: dict = (
                    {
                        "kafka.bootstrap.servers": "localhost:9092",
                        "topic": self._context.get_se_streaming_stats_topic_name,
                        "failOnDataLoss": "true",
                    }
                    if self._context.get_env == "local"
                    else (
                        {
                            "kafka.bootstrap.servers": f"{secret_handler.get_secret(self._context.get_server_url_key)}",
                            "kafka.security.protocol": "SASL_SSL",
                            "kafka.sasl.mechanism": "OAUTHBEARER",
                            "kafka.sasl.jaas.config": "kafkashaded.org.apache.kafka.common.security.oauthbearer."
                            "OAuthBearerLoginModule required oauth.client.id="
                            f"'{secret_handler.get_secret(self._context.get_client_id)}'  "
                            + "oauth.client.secret="
                            f"'{secret_handler.get_secret(self._context.get_token)}' "
                            "oauth.token.endpoint.uri="
                            f"'{secret_handler.get_secret(self._context.get_token_endpoint_url)}'; ",
                            "kafka.sasl.login.callback.handler.class": "io.strimzi.kafka.oauth.client"
                            ".JaasClientOauthLoginCallbackHandler",
                            "topic": (
                                self._context.get_se_streaming_stats_topic_name
                                if self._context.get_env == "local"
                                else secret_handler.get_secret(
                                    self._context.get_topic_name
                                )
                            ),
                        }
                    )
                )

                _sink_hook.writer(
                    _write_args={
                        "product_id": self._context.product_id,
                        "enable_se_streaming": _se_stats_dict[
                            user_config.se_enable_streaming
                        ],
                        "kafka_write_options": kafka_write_options,
                        "stats_df": df,
                    }
                )
            else:
                _log.info(
                    "Streaming stats to kafka is disabled, hence skipping writing to kafka"
                )

        except Exception as e:
            raise SparkExpectationsMiscException(
                f"error occurred while saving the data into the stats table {e}"
            )

    def write_error_records_final(
        self, df: DataFrame, error_table: str, rule_type: str
    ) -> Tuple[int, DataFrame]:
        try:
            _log.info("_write_error_records_final started")
            df = df.withColumn("sequence_number", monotonically_increasing_id())

            df_seq = df

            df = df.select(
                "sequence_number",
                *[
                    dq_column
                    for dq_column in df.columns
                    if dq_column.startswith(f"{rule_type}")
                ],
            )
            df.cache()

            failed_records = [
                f"size({dq_column}) != 0"
                for dq_column in df.columns
                if dq_column.startswith(f"{rule_type}")
            ]

            failed_records_rules = " or ".join(failed_records)
            # df = df.filter(expr(failed_records_rules))

            df = df.withColumn(
                f"meta_{rule_type}_results",
                when(
                    expr(failed_records_rules),
                    array(
                        *[
                            _col
                            for _col in df.columns
                            if _col.startswith(f"{rule_type}")
                        ]
                    ),
                ).otherwise(array(create_map())),
            ).drop(*[_col for _col in df.columns if _col.startswith(f"{rule_type}")])

            df = (
                df.withColumn(
                    f"meta_{rule_type}_results",
                    remove_empty_maps(df[f"meta_{rule_type}_results"]),
                )
                .withColumn(
                    self._context.get_run_id_name, lit(self._context.get_run_id)
                )
                .withColumn(
                    self._context.get_run_date_time_name,
                    lit(self._context.get_run_date),
                )
            )
            error_df_seq = df.filter(f"size(meta_{rule_type}_results) != 0")

            error_df = df_seq.join(
                error_df_seq,
                df_seq.sequence_number == error_df_seq.sequence_number,
                "inner",
            )

            # sequence number column removing from the data frame
            error_df_columns = [
                dq_column
                for dq_column in error_df.columns
                if (
                    dq_column.startswith("sequence_number")
                    or dq_column.startswith(rule_type)
                )
                is False
            ]

            error_df = error_df.select(error_df_columns)
            self._context.print_dataframe_with_debugger(error_df)

            print(
                f"self._context.get_se_enable_error_table : {self._context.get_se_enable_error_table}"
            )
            if self._context.get_se_enable_error_table:
                self.save_df_as_table(
                    error_df,
                    error_table,
                    self._context.get_target_and_error_table_writer_config,
                )

            _error_count = error_df.count()
            # if _error_count > 0:
            self.generate_summarized_row_dq_res(error_df, rule_type)

            # sequence number adding to dataframe for passing to action function
            df = df_seq.join(
                error_df_seq,
                df_seq.sequence_number == error_df_seq.sequence_number,
                "left",
            ).withColumn(
                f"meta_{rule_type}_results",
                coalesce(col(f"meta_{rule_type}_results"), array()),
            )

            df = (
                df.select(error_df_columns)
                .withColumn(
                    self._context.get_run_id_name, lit(self._context.get_run_id)
                )
                .withColumn(
                    self._context.get_run_date_time_name,
                    lit(self._context.get_run_date),
                )
            )

            _log.info("_write_error_records_final ended")
            return _error_count, df

        except Exception as e:
            raise SparkExpectationsMiscException(
                f"error occurred while saving data into the final error table {e}"
            )

    def generate_summarized_row_dq_res(self, df: DataFrame, rule_type: str) -> None:
        """
        This function implements/supports summarizing row dq error result
        Args:
            df: error dataframe(DataFrame)
            rule_type: type of the rule(str)

        Returns:
            None

        """
        try:
            df_explode = df.select(
                explode(f"meta_{rule_type}_results").alias("row_dq_res")
            )
            df_res = (
                df_explode.withColumn("rule_type", col("row_dq_res")["rule_type"])
                .withColumn("rule", col("row_dq_res")["rule"])
                .withColumn("description", col("row_dq_res")["description"])
                .withColumn("tag", col("row_dq_res")["tag"])
                .withColumn("action_if_failed", col("row_dq_res")["action_if_failed"])
                .select("rule_type", "rule", "description", "tag", "action_if_failed")
                .groupBy("rule_type", "rule", "description", "tag", "action_if_failed")
                .count()
                .withColumnRenamed("count", "failed_row_count")
            )
            summarized_row_dq_list = [
                {
                    "rule_type": row.rule_type,
                    "rule": row.rule,
                    "description": row.description,
                    "tag": row.tag,
                    "action_if_failed": row.action_if_failed,
                    "failed_row_count": row.failed_row_count,
                }
                for row in df_res.select(
                    "rule_type",
                    "rule",
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
                                    "description": each_rule["description"],
                                    "tag": each_rule["tag"],
                                    "rule": each_rule["rule"],
                                    "action_if_failed": each_rule["action_if_failed"],
                                    "rule_type": each_rule["rule_type"],
                                    "failed_row_count": 0,
                                }
                            )

            self._context.set_summarized_row_dq_res(summarized_row_dq_list)

        except Exception as e:
            raise SparkExpectationsMiscException(
                f"error occurred created summarized row dq statistics {e}"
            )

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
                itr["rule"]: int(itr["failed_row_count"])
                for itr in self._context.get_summarized_row_dq_res
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
                    error_drop_percentage = round(
                        (failed_row_count / self._context.get_input_count) * 100, 2
                    )
                    error_threshold_list.append(
                        {
                            "rule_name": rule_name,
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
            raise SparkExpectationsMiscException(
                f"An error occurred while creating error threshold list : {e}"
            )
