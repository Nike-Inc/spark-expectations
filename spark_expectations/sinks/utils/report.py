from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    split,
    regexp_extract,
    regexp_replace,
    round,
    explode,
    expr,
    coalesce,
    when,
    size,
    concat_ws,
    abs,
    least,
    greatest,
    get_json_object,
    lit,
    trim,
)
from pyspark.sql.types import DoubleType, StringType, DecimalType, TimestampType

from spark_expectations import _log
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.sinks.utils.writer import SparkExpectationsWriter


@dataclass
class SparkExpectationsReport:
    _context: SparkExpectationsContext

    def __post_init__(self) -> None:
        self.spark = self._context.spark
        self._context.set_report_table_name("dq_stats_rpt")

    def dq_obs_report_data_insert(self) -> tuple[bool, DataFrame]:
        try:
            context = self._context

            print("dq_obs_report_data_insert method called stats_detailed table")
            df_stats_detailed = context.get_stats_detailed_dataframe
            df_custom_detailed = context.get_custom_detailed_dataframe
            df = df_custom_detailed
            dq_status_calculation_attribute = "success_percentage"
            source_zero_and_target_zero_is = "pass"
            df = df.filter((df.source_output.isNotNull()) & (df.target_output.isNotNull()))
            df = (
                df.withColumn("success_percentage", lit(None).cast(DoubleType()))
                .withColumn("failed_records", lit(0))
                .withColumn("status", lit(None).cast(StringType()))
                .withColumnRenamed("source_output", "total_records")
                .withColumnRenamed("target_output", "valid_records")
            )
            join_columns = ["run_id", "product_id", "table_name", "column_name", "rule"]
            only_querydq_src_base_df = df
            only_querydq_src_df = (
                only_querydq_src_base_df.withColumn(
                    "extracted_total_records_data",
                    split(regexp_extract("total_records", r"=\[(.*)\]", 1), "},"),
                )
                .select(
                    "*",
                    explode("extracted_total_records_data").alias("total_records_dict"),
                )
                .withColumn(
                    "total_records_dict_split",
                    split(regexp_replace(col("total_records_dict"), "[}{]", ""), ","),
                )
                .withColumn("total_records", expr("element_at(total_records_dict_split, -1)"))
                .withColumn(
                    "column_name",
                    when(
                        size(col("total_records_dict_split")) > 1,
                        concat_ws(
                            ",",
                            expr("slice(total_records_dict_split, 1, size(total_records_dict_split)-1)"),
                        ),
                    ).otherwise(col("column_name")),
                )
            )

            data_types = {
                "rule": StringType(),
                "column_name": StringType(),
                "dq_time": TimestampType(),
                "product_id": StringType(),
                "table_name": StringType(),
                "status": StringType(),
                "total_records": StringType(),
                "failed_records": StringType(),
                "valid_records": StringType(),
                "success_percentage": StringType(),
                "run_id": StringType(),
            }
            dq_column_list = list(data_types.keys())
            src_dq_column_list = [col_name for col_name in dq_column_list if col_name not in ["valid_records"]]
            only_querydq_src_final_df = only_querydq_src_df.selectExpr(*src_dq_column_list)
            only_querydq_tgt_base_df = df
            only_querydq_tgt_df = (
                only_querydq_tgt_base_df.withColumn(
                    "extracted_valid_records_data",
                    split(regexp_extract("valid_records", r"=\[(.*)\]", 1), "},"),
                )
                .select(
                    "*",
                    explode("extracted_valid_records_data").alias("valid_records_dict"),
                )
                .withColumn(
                    "total_valid_dict_split",
                    split(regexp_replace(col("valid_records_dict"), "[}{]", ""), ","),
                )
                .withColumn("valid_records", expr("element_at(total_valid_dict_split, -1)"))
                .withColumn(
                    "column_name",
                    when(
                        size(col("total_valid_dict_split")) > 1,
                        concat_ws(
                            ",",
                            expr("slice(total_valid_dict_split, 1, size(total_valid_dict_split)-1)"),
                        ),
                    ).otherwise(col("column_name")),
                )
            )

            tgt_dq_column_list = [col_name for col_name in dq_column_list if col_name not in ["total_records"]]
            only_querydq_tgt_final_df = only_querydq_tgt_df.selectExpr(*tgt_dq_column_list)

            ignore_colums = ["valid_records", "total_records"]
            only_querydq_src_final_df = only_querydq_src_final_df.select(
                [
                    col(c).alias("src_" + c) if c not in ignore_colums else col(c).alias(c)
                    for c in only_querydq_src_final_df.columns
                ]
            )
            only_querydq_src_final_df.createOrReplaceTempView("src_df")
            only_querydq_tgt_final_df = only_querydq_tgt_final_df.select(
                [
                    col(c).alias("tgt_" + c) if c not in ignore_colums else col(c).alias(c)
                    for c in only_querydq_tgt_final_df.columns
                ]
            )
            only_querydq_tgt_final_df.createOrReplaceTempView("tgt_df")

            # trim_col_list = 'column_name'
            sql_query = (
                "SELECT "
                + ", ".join(
                    [
                        f"COALESCE(src_df.src_{col}, tgt_df.tgt_{col}) AS {col}"
                        if col not in ignore_colums
                        else f"{col}"
                        for col in dq_column_list
                    ]
                )
                + " FROM src_df FULL JOIN tgt_df ON "
                + " AND ".join(
                    [
                        f"REGEXP_REPLACE(REGEXP_REPLACE(lower(src_df.src_{col}), '\"', ''), ' ', '') \
                        = REGEXP_REPLACE(REGEXP_REPLACE(lower(tgt_df.tgt_{col}), '\"', ''), ' ', '')"
                        if col not in ignore_colums
                        else f"src_df.{col} = tgt_df.{col} "
                        for col in join_columns
                    ]
                )
            )
            # Execute the SQL query
            only_querydq_final_after_join_df = self.spark.sql(sql_query)

            only_querydq_final_after_join_df = (
                only_querydq_final_after_join_df.withColumn(
                    "total_records_only_nbr",
                    regexp_extract(col("total_records"), r"\d+", 0).cast("bigint"),
                )
                .withColumn(
                    "valid_records_only_nbr",
                    regexp_extract(col("valid_records"), r"\d+", 0).cast("bigint"),
                )
                .withColumn(
                    "success_percentage",
                    when(
                        (col("total_records_only_nbr") == "") & (col("valid_records_only_nbr").isNull()),
                        lit(100),
                    )
                    .when(
                        (col("total_records_only_nbr") == "") & (col("valid_records_only_nbr") == ""),
                        lit(100),
                    )
                    .when(
                        (col("total_records_only_nbr") != "") & (col("valid_records_only_nbr").isNull()),
                        lit(0),
                    )
                    .otherwise(
                        coalesce(
                            (
                                100
                                * least(
                                    abs(trim(col("valid_records_only_nbr"))),
                                    abs(trim(col("total_records_only_nbr"))),
                                )
                                / greatest(
                                    abs(trim(col("valid_records_only_nbr"))),
                                    abs(trim(col("total_records_only_nbr"))),
                                )
                            ).cast(DecimalType(20, 2)),
                            lit(0),
                        )
                    ),
                )
                .withColumn(
                    "failed_rec_perc_variance",
                    when(
                        (col("total_records_only_nbr") == "") & (col("valid_records_only_nbr").isNull()),
                        lit(0),
                    )
                    .when(
                        (col("total_records_only_nbr") == "") & (col("valid_records_only_nbr") == ""),
                        lit(0),
                    )
                    .when(
                        (col("total_records_only_nbr") != "") & (col("valid_records_only_nbr").isNull()),
                        lit(100),
                    )
                    .when(
                        (coalesce(col("total_records_only_nbr"), lit(0)) != 0)
                        & (coalesce(col("valid_records_only_nbr"), lit(0)) != 0),
                        coalesce(
                            round(
                                (
                                    (col("total_records_only_nbr") - col("valid_records_only_nbr"))
                                    / col("total_records_only_nbr")
                                )
                                * 100,
                                2,
                            ),
                            lit(0),
                        ),
                    )
                    .otherwise(100),
                )
                .withColumn(
                    "failed_records",
                    abs(
                        coalesce(
                            coalesce(
                                trim(col("total_records_only_nbr")).cast("bigint"),
                                lit(0),
                            )
                            - coalesce(
                                trim(col("valid_records_only_nbr")).cast("bigint"),
                                lit(0),
                            ),
                            lit(0),
                        )
                    ),
                )
                .withColumn(
                    "dq_status",
                    when(
                        (col("total_records") == lit(0))
                        & (col("valid_records") == lit(0))
                        & (lit(source_zero_and_target_zero_is) == "pass"),
                        "PASS",
                    )
                    .when(
                        (lit(dq_status_calculation_attribute) == "failed_records") & (col("failed_records") != lit(0)),
                        "PASS",
                    )
                    .when(
                        (col("total_records") == "") & (col("valid_records") == ""),
                        "PASS",
                    )
                    .when(
                        (coalesce(col("total_records"), lit(0)) == 0) & (coalesce(col("valid_records"), lit(0)) == 0),
                        "PASS",
                    )
                    .when(
                        (lit(dq_status_calculation_attribute) != "failed_records")
                        & (col("success_percentage") == lit(100.00)),
                        "PASS",
                    ),
                )
                .drop(
                    "total_records_only_nbr",
                    "valid_records_only_nbr",
                    "failed_rec_perc_variance",
                    "dq_status",
                )
                .withColumn(
                    "dq_job_metadata_info",
                    lit(self._context.get_job_metadata).cast("string"),
                )
            )

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
                "rule_type",
                "description",
                "tag",
                "dq_date",
            ]
            df_stats_detailed = df_stats_detailed.drop(*columns_to_remove)

            df_stats_detailed = (
                df_stats_detailed.withColumnRenamed("source_dq_row_count", "total_records")
                .withColumnRenamed("source_dq_status", "status")
                .withColumnRenamed("source_dq_actual_row_count", "valid_records")
                .withColumnRenamed("source_dq_error_row_count", "failed_records")
                .withColumn(
                    "success_percentage",
                    (col("valid_records") / col("total_records")) * 100,
                )
            )

            df_report_table = only_querydq_final_after_join_df.unionByName(df_stats_detailed)
            df_report_table = (
                df_report_table.withColumn(
                    "job",
                    get_json_object(df_report_table["dq_job_metadata_info"], "$.job"),
                )
                .withColumn(
                    "Region",
                    get_json_object(df_report_table["dq_job_metadata_info"], "$.Region"),
                )
                .withColumn(
                    "Snapshot",
                    get_json_object(df_report_table["dq_job_metadata_info"], "$.Snapshot"),
                )
                .withColumn(
                    "data_object_name",
                    get_json_object(df_report_table["dq_job_metadata_info"], "$.data_object_name"),
                )
            )
            df_report_table = df_report_table.drop("dq_job_metadata_info")
            _log.info("below is the report table")

            return True, df_report_table
        except Exception as e:
            raise SparkExpectationsMiscException(f"An error occurred in dq_obs_report_data_insert: {e}")

    def save_report_table(self, df_report_table: DataFrame) -> None:
        """
        Saves the report table to the specified location.

        Args:
            df_report_table (DataFrame): The PySpark DataFrame containing the report data to be saved.

        Returns:
            None
        """
        context = self._context
        save_df_report_table = SparkExpectationsWriter(_context=context)
        save_df_report_table.save_df_as_table(
            df_report_table,
            context.get_report_table_name,
            {
                "mode": "append",
                "format": "delta",
                "partitionBy": ["product_id", "meta_dq_run_datetime"],
                "sortBy": None,
                "bucketBy": None,
                "options": None,
            },
            stats_table=False,
        )
