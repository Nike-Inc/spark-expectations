from dataclasses import dataclass
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.notifications.push.alert import AlertTrial
# from alert_trial import AlertTrial  # Import AlertTrial

@dataclass
class SparkExpectationsReport:
    _context: SparkExpectationsContext

    def __post_init__(self) -> None:
        self.spark = self._context.spark

    def dq_obs_report_data_insert(self) -> DataFrame:
        try:
            context = self._context
            print("dq_obs_report_data_insert method called stats_detailed table")
            df_stats_detailed = context.get_stats_detailed_dataframe
            df_custom_detailed = context.get_custom_detailed_dataframe
            # df_stats_detailed = df_stats_detailed.withColumn("job", get_json_object(df_stats_detailed["dq_job_metadata_info"], "$.job")) \
            #     .withColumn("Region", get_json_object(df_stats_detailed["dq_job_metadata_info"], "$.Region")) \
            #     .withColumn("Snapshot", get_json_object(df_stats_detailed["dq_job_metadata_info"], "$.Snapshot")) \
            #     .withColumn("data_object_name", get_json_object(df_stats_detailed["dq_job_metadata_info"], "$.data_object_name"))
            print("logic implementation started")
            #final table for stats detailed table
            df_stats_detailed = df_stats_detailed.withColumnRenamed("source_dq_row_count", "total_records") \
                .withColumnRenamed("source_dq_error_row_count", "error_records") \
                .withColumnRenamed("source_dq_actual_row_count", "valid_records")
            df_stats_detailed = df_stats_detailed.withColumn("success_percentage",
                                                             expr("(valid_records / total_records )* 100"))


            df_source = df_custom_detailed
            df_target = df_custom_detailed
            from pyspark.sql.functions import split, regexp_replace, explode, col

            # detailed source calculation
            df_with_array = df_source.withColumn(
                "source_output_array",
                split(regexp_replace(col("source_output"), r"[\{\}\[\]]", ""), ", ")
            )

            # Explode the array to create a new row for each element
            df_with_array = df_with_array.select(
                '*',
                explode(col('source_output_array')).alias('total_records_dict_source')
            )

            df_split = df_with_array.withColumn("total_records_split_source", split("total_records_dict_source", ", "))
            from pyspark.sql.functions import regexp_extract
            df_with_count = df_with_array.withColumn(
                "total_records",
                regexp_extract(col("total_records_dict_source"), r'([^,]+:[^,]+)$', 1)
            )

            df_11 = df_with_array.withColumn('source_f1_array', split(col('source_output'), ','))
            df111 = df_11.withColumn('column_name_', concat_ws(',', element_at(col('source_f1_array'), 1),
                                                               element_at(col('source_f1_array'), 2)))
            df_111 = df111.withColumn(
                "total_records",
                regexp_extract(col("total_records_dict_source"), r'([^,]+:[^,]+)$', 1)
            )
            df_source_final = df_111.select("column_name_", "total_records", "*")
            print("df_source_final")








            #calculation for the detailed table.

            df_with_array1 = df_target.withColumn(
                "target_output_array",
                split(regexp_replace(col("target_output"), r"[\{\}\[\]]", ""), ", ")
            )
            df_with_array1 = df_with_array1.select(
                '*',
                explode(col('target_output_array')).alias('total_records_dict_target')
            )
            from pyspark.sql.functions import split

            df_split1 = df_with_array1.withColumn("total_records_split_target",
                                                  split("total_records_dict_target", ", "))

            # df_with_count1 = df_with_array1.withColumn("valid_records", regexp_extract("total_records_dict_target", r'"count":(\d+)', 1).cast("int"))
            df_with_count1 = df_with_array1.withColumn(
                "valid_records",
                regexp_extract(col("total_records_dict_target"), r'([^,]+:[^,]+)$', 1))
            # df_with_count1 = df_with_count1.withColumn("new_column_name_target", regexp_extract("total_records_dict_target", r'^(.*)"count":\d+(.*)$', 1))
            df_target_final = df_with_count1.select("valid_records")

            df2 = df_target_final
            df1 = df_source_final
            from pyspark.sql.functions import lit, monotonically_increasing_id
            from pyspark.sql.types import IntegerType
            df1 = df1.withColumn("range", monotonically_increasing_id().cast(IntegerType()))
            df2 = df2.withColumn("range", monotonically_increasing_id().cast(IntegerType()))
            df_joined = df2.join(df1, on="range", how="inner")
            df_joined = df_joined.withColumn("valid_records_int",
                                             regexp_extract(col("valid_records"), r'\d+', 0).cast("int"))
            df_joined = df_joined.withColumn("total_records_int",
                                             regexp_extract(col("total_records"), r'\d+', 0).cast("int"))
            df_joined = df_joined.withColumn("failed_records", abs(expr("total_records_int - valid_records_int")))
            df_joined = df_joined.withColumn("success_percentage", expr(
                "(1 - failed_records / greatest(valid_records_int, total_records_int)) * 100"))




            drop_list=["dq_time","column_name","column_name_","source_f1_array","total_records","valid_records","range","alias","dq_type","source_output","source_output","target_output","source_output_array","total_records_dict_source","total_records_split_source","total_records_dict_target","total_records_split_target"]
            print("df_joined")
            print("report_table")
            df_joined=df_joined.drop(*drop_list)
            df_joined = df_joined.withColumn(
                "source_dq_status",
                when(df_joined["failed_records"] > 0, "fail").otherwise("pass")

            )
            df_joined = df_joined.withColumnRenamed("valid_records_int", "valid_records") \
                .withColumnRenamed("failed_records", "error_records") \
                .withColumnRenamed("total_records_int", "total_records")
            df_joined = df_joined.select(
                "run_id",
                "product_id",
                "table_name",
                "rule",
                "source_dq_status",
                "valid_records",
                "error_records",
                "total_records",
                "success_percentage"
            )






            df_joined.show()
            print("df_stats_detailed")
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
                "dq_job_metadata_info",
                "dq_time",
                "description",
                "dq_date",
                "tag",
                "column_name"
            ]
            df_stats_detailed=df_stats_detailed.drop(*columns_to_remove)
            df_stats_detailed.show(truncate=False)





















            # Remove duplicate rows
            print("report_table")
            df_union = df_joined.unionByName(df_stats_detailed)
            # df_stats_detailed = df_stats_detailed.withColumn("job", get_json_object(df_stats_detailed["dq_job_metadata_info"], "$.job")) \
            #     .withColumn("Region", get_json_object(df_stats_detailed["dq_job_metadata_info"], "$.Region")) \
            #     .withColumn("Snapshot", get_json_object(df_stats_detailed["dq_job_metadata_info"], "$.Snapshot")) \
            #     .withColumn("data_object_name", get_json_object(df_stats_detailed["dq_job_metadata_info"], "$.data_object_name"))

            # Show the result


            # Show the result
            df_union.show(truncate=False)


            #writing the data into the report table

            # Create an instance of AlertTrial and call get_report_data


            return True,df_union
        except Exception as e:
            raise SparkExpectationsMiscException(
                f"An error occurred in dq_obs_report_data_insert: {e}"
            )



