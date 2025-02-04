import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from dataclasses import dataclass
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core.expectations import DataFrame
from spark_expectations.core import get_spark_session
from spark_expectations.sinks.utils.report import SparkExpectationsReport
spark = get_spark_session()
from unittest.mock import MagicMock, patch, Mock
# Initialize Spark session for testing

# @pytest.fixture(scope="module")
# def spark_session():
#     spark = SparkSession.builder \
#         .appName("pytest-pyspark") \
#         .master("local[2]") \
#         .getOrCreate()
#     yield spark
#     spark.stop()

# Mock SparkExpectationsContext
@dataclass
class MockSparkExpectationsContext:
    spark: SparkSession
    stats_detailed_dataframe: DataFrame
    custom_detailed_dataframe: DataFrame
    job_metadata: dict

    # def get_stats_detailed_dataframe(self):
    #     return self.stats_detailed_dataframe
    #
    # def get_custom_detailed_dataframe(self):
    #     return self.custom_detailed_dataframe
    #
    # def get_job_metadata(self):
    #     return self.job_metadata

# Test data for stats_detailed_dataframe
stats_detailed_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("rule_type", StringType(), True),
    StructField("rule", StringType(), True),
    StructField("column_name", StringType(), True),
    StructField("source_expectations", StringType(), True),
    StructField("tag", StringType(), True),
    StructField("description", StringType(), True),
    StructField("source_dq_status", StringType(), True),
    StructField("source_dq_actual_outcome", StringType(), True),
    StructField("source_dq_expected_outcome", StringType(), True),
    StructField("source_dq_actual_row_count", IntegerType(), True),
    StructField("source_dq_error_row_count", IntegerType(), True),
    StructField("source_dq_row_count", IntegerType(), True),
    StructField("source_dq_start_time", StringType(), True),
    StructField("source_dq_end_time", StringType(), True),
    StructField("target_expectations", StringType(), True),
    StructField("target_dq_status", StringType(), True),
    StructField("target_dq_actual_outcome", StringType(), True),
    StructField("target_dq_expected_outcome", StringType(), True),
    StructField("target_dq_actual_row_count", IntegerType(), True),
    StructField("target_dq_error_row_count", IntegerType(), True),
    StructField("target_dq_row_count", IntegerType(), True),
    StructField("target_dq_start_time", StringType(), True),
    StructField("target_dq_end_time", StringType(), True),
    StructField("dq_date", StringType(), True),
    StructField("dq_time", StringType(), True),
    StructField("dq_job_metadata_info", StringType(), True)
])

# Define the data
stats_detailed_data = [
    ("your_product_e77f545a-e26b-11ef-b3d2-4240eb7a97f9", "your_product", "dq_spark_dev.customer_order", "query_dq", "product_missing_count_threshold", "testing_sample", "((select count(*) from (SELECT DISTINCT product_id, order_id, order_date, COUNT(*) AS count FROM order_source GROUP BY product_id, order_id, order_date) a) - (select count(*) from (SELECT DISTINCT product_id, order_id, order_date, COUNT(*) AS count FROM order_target GROUP BY product_id, order_id, order_date) b) ) > 3", "validity", "row count threshold", "fail", "1", ">3", None, 8, 8, "2025-02-04 01:47:35", "2025-02-04 01:47:36", None, None, None, None, None, None, None, None,None, "2025-02-03", "2025-02-04 01:47:41", "{'job': 'na_CORL_DIGITAL_source_to_o9', 'Region': 'NA', 'env': 'dev', 'Snapshot': '2024-04-15', 'data_object_name ': 'customer_order'}")
]
# Test data for custom_detailed_dataframe
custom_detailed_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("rule", StringType(), True),
    StructField("column_name", StringType(), True),
    StructField("alias", StringType(), True),
    StructField("dq_type", StringType(), True),
    StructField("source_output", StringType(), True),
    StructField("target_output", StringType(), True),
    StructField("dq_time", StringType(), True)
])

# Define the data
custom_detailed_data = [
    ("your_product_8d96eace-e26c-11ef-a07d-4240eb7a97f9", "your_product", "dq_spark_dev.customer_order", "product_missing_count_threshold", "testing_sample", "source_f1", "_source_dq", '{source_f1=[{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156","order_date":"11/8/2016","count":1}, {"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688","order_date":"6/12/2016","count":1}, {"product_id":"FUR-TA-10000577","order_id":"US-2015-108966","order_date":"10/11/2015","count":1}, {"product_id":"FUR-CH-10000454","order_id":"CA-2016-152156","order_date":"11/8/2016","count":1}, {"product_id":"OFF-ST-10000760","order_id":"US-2015-108966","order_date":"10/11/2015","count":1}]}', '{target_f1=[{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156","order_date":"11/8/2016","count":2}, {"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688","order_date":"6/12/2016","count":2}, {"product_id":"FUR-TA-10000577","order_id":"US-2015-108966","order_date":"10/11/2015","count":2}, {"product_id":"FUR-CH-10000454","order_id":"CA-2016-152156","order_date":"11/8/2016","count":2}]}', "2025-02-04 01:52:21"),
    ("your_product_8d96eace-e26c-11ef-a07d-4240eb7a97f9", "your_product", "dq_spark_dev.customer_order", "product_missing_count_threshold", "testing_sample", "target_f1", "_source_dq", '{target_f1=[{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156","order_date":"11/8/2016","count":2}, {"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688","order_date":"6/12/2016","count":2}, {"product_id":"FUR-TA-10000577","order_id":"US-2015-108966","order_date":"10/11/2015","count":2}, {"product_id":"FUR-CH-10000454","order_id":"CA-2016-152156","order_date":"11/8/2016","count":2}]}', None, "2025-02-04 01:52:21")
]

# Job metadata
job_metadata = {
    "job": "test_job",
    "Region": "test_region",
    "Snapshot": "test_snapshot",
    "data_object_name": "test_data_object"
}

# Test cases
def test_dq_obs_report_data_insert():



    # Create DataFrames
    stats_detailed_df = spark.createDataFrame(stats_detailed_data, stats_detailed_schema)
    custom_detailed_df = spark.createDataFrame(custom_detailed_data, custom_detailed_schema)




    # Mock context
    mock_context = Mock(spec=SparkExpectationsContext)
    mock_context.spark = spark
    #added
    mock_context.get_stats_detailed_dataframe.return_value = stats_detailed_df
    mock_context.get_custom_detailed_dataframe.return_value = custom_detailed_df
    # mock_context.get_job_metadata.return_value = job_metadata

    mock_context.get_stats_detailed_dataframe.return_value.show(5)
    mock_context.get_custom_detailed_dataframe.return_value.show(5)

    # Initialize SparkExpectationsReport
    report = SparkExpectationsReport(mock_context)


    # Call the method
    result, df_report_table = report.dq_obs_report_data_insert()

    # Assertions
    assert result is True
    assert df_report_table.count() > 0
    assert "job" in df_report_table.columns
    assert "Region" in df_report_table.columns
    assert "Snapshot" in df_report_table.columns
    assert "data_object_name" in df_report_table.columns

    # Check if the DataFrame has the expected columns
    expected_columns = ["run_id", "product_id", "table_name", "column_name", "rule", "total_records", "valid_records", "failed_records", "success_percentage", "status", "job", "Region", "Snapshot", "data_object_name"]
    assert all(column in df_report_table.columns for column in expected_columns)

    # Check if the success_percentage is calculated correctly
    success_percentage_row = df_report_table.filter(col("run_id") == "run1").select("success_percentage").collect()[0][0]
    assert success_percentage_row == 95.0

    # Check if the failed_records is calculated correctly
    failed_records_row = df_report_table.filter(col("run_id") == "run1").select("failed_records").collect()[0][0]
    assert failed_records_row == 5

