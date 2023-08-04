from dataclasses import dataclass
from typing import Dict, Optional, Tuple, List, Any
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    lit,
    expr,
    when,
    array,
    to_timestamp,
    round,
    create_map,
    explode,
)
from spark_expectations import _log
from spark_expectations.core.exceptions import (
    SparkExpectationsUserInputOrConfigInvalidException,
    SparkExpectationsMiscException,
)
from spark_expectations.core import get_spark_session
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

    product_id: str
    _context: SparkExpectationsContext

    def __post_init__(self) -> None:
        self.spark = get_spark_session()

    def save_df_as_table(
        self,
        df: DataFrame,
        table_name: str,
        spark_conf: Dict[str, str],
        options: Dict[str, str],
    ) -> None:
        """
        This function takes a dataframe and writes into a table

        Args:
            df: Provide the dataframe which need to be written as a table
            table_name: Provide the table name to which the dataframe need to be written to
            spark_conf: Provide the spark conf that need to be set on the SparkSession
            options: Provide the options that need to be used while writing the table

        Returns:
            None:

        """
        try:
            print("run date ", self._context.get_run_date)
            for key, value in spark_conf.items():
                self.spark.conf.set(key, value)
            _df = df.withColumn(
                self._context.get_run_id_name, lit(f"{self._context.get_run_id}")
            ).withColumn(
                self._context.get_run_date_name,
                to_timestamp(lit(f"{self._context.get_run_date}")),
            )
            _log.info("_save_df_as_table started")
            _df.write.saveAsTable(name=table_name, **options)
            self.spark.sql(
                f"ALTER TABLE {table_name} SET TBLPROPERTIES ('product_id' = '{self.product_id}')"
            )
            _log.info("finished writing records to table: %s", table_name)

        except Exception as e:
            raise SparkExpectationsUserInputOrConfigInvalidException(
                f"error occurred while writing data in to the table {e}"
            )

    def write_df_to_table(
        self,
        df: DataFrame,
        table: str,
        spark_conf: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        This function takes in a dataframe which has dq results publish it

        Args:
            df: Provide a dataframe to write the records to a table.
            table : Provide the full original table name into which the data need to be written to
            spark_conf: Provide the spark conf, if you want to set/override the configuration
            options: Provide the options, if you want to override the default.
                    default options available are - {"mode": "append", "format": "delta"}

        Returns:
            None:

        """
        try:
            _spark_conf = (
                {**{"spark.sql.session.timeZone": "Etc/UTC"}, **spark_conf}
                if spark_conf
                else {"spark.sql.session.timeZone": "Etc/UTC"}
            )
            _options = (
                {**{"mode": "append", "format": "delta"}, **options}
                if options
                else {"mode": "append", "format": "delta"}
            )
            self.save_df_as_table(df, table, _spark_conf, _options)
        except Exception as e:
            raise SparkExpectationsMiscException(
                f"error occurred while saving the data into the table  {e}"
            )

    def write_error_stats(self) -> None:
        """
        This functions takes the stats table and write it into error table

        Args:
            table_name: Provide the full table name to which the dq stats will be written to
            input_count: Provide the original input dataframe count
            error_count: Provide the error record count
            output_count: Provide the output dataframe count
            source_agg_dq_result: source aggregated dq results
            final_agg_dq_result: final aggregated dq results

        Returns:
            None:

        """
        try:
            self.spark.conf.set("spark.sql.session.timeZone", "Etc/UTC")
            from datetime import date

            # table_name: str,
            # input_count: int,
            # error_count: int = 0,
            # output_count: int = 0,
            # source_agg_dq_result: Optional[List[Dict[str, str]]] = None,
            # final_agg_dq_result: Optional[List[Dict[str, str]]] = None,

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
                    self.product_id,
                    table_name,
                    input_count,
                    error_count,
                    output_count,
                    self._context.get_output_percentage,
                    self._context.get_success_percentage,
                    self._context.get_error_percentage,
                    source_agg_dq_result
                    if source_agg_dq_result and len(source_agg_dq_result) > 0
                    else None,
                    final_agg_dq_result
                    if final_agg_dq_result and len(final_agg_dq_result) > 0
                    else None,
                    source_query_dq_result
                    if source_query_dq_result and len(source_query_dq_result) > 0
                    else None,
                    final_query_dq_result
                    if final_query_dq_result and len(final_query_dq_result) > 0
                    else None,
                    self._context.get_summarised_row_dq_res,
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
            error_stats_rdd = self.spark.sparkContext.parallelize(error_stats_data)

            from pyspark.sql.types import (
                StructType,
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

            df = self.spark.createDataFrame(error_stats_rdd, schema=error_stats_schema)
            self._context.print_dataframe_with_debugger(df)

            df = (
                df.withColumn("output_percentage", round(df.output_percentage, 2))
                .withColumn("success_percentage", round(df.success_percentage, 2))
                .withColumn("error_percentage", round(df.error_percentage, 2))
            )
            _log.info(
                "Writing metrics to the stats table: %s, started",
                self._context.get_dq_stats_table_name,
            )

            _se_stats_dict = self._context.get_se_streaming_stats_dict

            secret_handler = SparkExpectationsSecretsBackend(secret_dict=_se_stats_dict)

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
                            else secret_handler.get_secret(self._context.get_topic_name)
                        ),
                    }
                    if bool(_se_stats_dict[user_config.se_enable_streaming])
                    else {}
                )
            )

            _sink_hook.writer(
                _write_args={
                    "product_id": self.product_id,
                    "enable_se_streaming": _se_stats_dict[
                        user_config.se_enable_streaming
                    ],
                    "table_name": self._context.get_dq_stats_table_name,
                    "kafka_write_options": kafka_write_options,
                    "stats_df": df,
                }
            )

            _log.info(
                "Writing metrics to the stats table: %s, ended",
                self._context.get_dq_stats_table_name,
            )

        except Exception as e:
            raise SparkExpectationsMiscException(
                f"error occurred while saving the data into the stats table {e}"
            )

    def write_error_records_final(
        self,
        df: DataFrame,
        error_table: str,
        rule_type: str,
        spark_conf: Optional[Dict[str, str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> Tuple[int, DataFrame]:
        try:
            _log.info("_write_error_records_final started")
            _spark_conf = (
                {**{"spark.sql.session.timeZone": "Etc/UTC"}, **spark_conf}
                if spark_conf
                else {"spark.sql.session.timeZone": "Etc/UTC"}
            )
            _options = (
                {**{"mode": "append", "format": "delta"}, **options}
                if options
                else {"mode": "append", "format": "delta"}
            )

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
                    self._context.get_run_date_name,
                    lit(self._context.get_run_date),
                )
            )
            error_df = df.filter(f"size(meta_{rule_type}_results) != 0")
            self._context.print_dataframe_with_debugger(error_df)

            self.save_df_as_table(error_df, error_table, _spark_conf, _options)

            _error_count = error_df.count()
            if _error_count > 0:
                self.generate_summarised_row_dq_res(error_df, rule_type)

            _log.info("_write_error_records_final ended")
            return _error_count, df

        except Exception as e:
            raise SparkExpectationsMiscException(
                f"error occurred while saving data into the final error table {e}"
            )

    def generate_summarised_row_dq_res(self, df: DataFrame, rule_type: str) -> None:
        """
        This function implements/supports summarising row dq error result
        Args:
            df: error dataframe(DataFrame)
            rule_type: type of the rule(str)

        Returns:
            None

        """
        try:

            def update_dict(accumulator: dict) -> dict:  # pragma: no cover
                if accumulator.get("failed_row_count") is None:  # pragma: no cover
                    accumulator["failed_row_count"] = str(2)  # pragma: no cover
                else:  # pragma: no cover
                    accumulator["failed_row_count"] = str(  # pragma: no cover
                        int(accumulator["failed_row_count"]) + 1  # pragma: no cover
                    )  # pragma: no cover

                return accumulator  # pragma: no cover

            summarised_row_dq_dict: Dict[str, Dict[str, str]] = (
                df.select(explode(f"meta_{rule_type}_results").alias("row_dq_res"))
                .rdd.map(
                    lambda rule_meta_dict: (
                        rule_meta_dict[0]["rule"],
                        rule_meta_dict[0],
                    )
                )
                .reduceByKey(lambda acc, itr: update_dict(acc))
            ).collectAsMap()

            self._context.set_summarised_row_dq_res(
                list(summarised_row_dq_dict.values())
            )

        except Exception as e:
            raise SparkExpectationsMiscException(
                f"error occurred created summarised row dq statistics {e}"
            )
