from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core import get_spark_session
from spark_expectations.core.expectations import DataFrame
from spark_expectations.sinks.utils.report import SparkExpectationsReport
from pyspark.sql.types import StructField, IntegerType, StringType, StructType, TimestampType, FloatType,DoubleType,LongType
import pytest
spark = get_spark_session()


@pytest.fixture(scope="module")
def test_dq_obs_report_data_insert():
    schema = StructType([
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

    # Create data
    data = [
        (
            "your_product_1aacbbd4-e7de-11ef-bac4-4240eb7a97f9",
            "your_product",
            "dq_spark_dev.customer_order",
            "query_dq",
            "product_missing_count_threshold",
            "testing_sample",
            (
                "((select count(*) from (SELECT DISTINCT product_id, order_id, order_date, COUNT(*) AS count "
                "FROM order_source GROUP BY product_id, order_id, order_date) a) - (select count(*) from "
                "(SELECT DISTINCT product_id, order_id, order_date, COUNT(*) AS count FROM order_target GROUP BY "
                "product_id, order_id, order_date) b) ) > 3"
            ),
            "validity",
            "row count threshold",
            "fail",
            "1",
            ">3",
            None,
            8,
            8,
            "2025-02-11 00:07:39",
            "2025-02-11 00:07:40",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "2025-02-10",
            "2025-02-11 00:07:46",
            (
                "{'job': 'na_CORL_DIGITAL_source_to_o9', 'Region': 'NA', 'env': 'dev', 'Snapshot': '2024-04-15', "
                "'data_object_name ': 'customer_order'}"
            )
        )
    ]
    # Create DataFrame
    df_detailed_table_test = spark.createDataFrame(data, schema)

    schema_1 = StructType([
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

    # Create data
    data_1 = [
        (
            "your_product_a67e4db2-e7de-11ef-bb70-4240eb7a97f9",
            "your_product",
            "dq_spark_dev.customer_order",
            "product_missing_count_threshold",
            "testing_sample",
            "source_f1",
            "_source_dq",
            (
                '{source_f1=[{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156","order_date":"11/8/2016","count":1}, '
                '{"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688","order_date":"6/12/2016","count":1}, '
                '{"product_id":"FUR-TA-10000577","order_id":"US-2015-108966","order_date":"10/11/2015","count":1}, '
                '{"product_id":"FUR-CH-10000454","order_id":"CA-2016-152156","order_date":"11/8/2016","count":1}, '
                '{"product_id":"OFF-ST-10000760","order_id":"US-2015-108966","order_date":"10/11/2015","count":1}]}'
            ),
            (
                '{target_f1=[{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156","order_date":"11/8/2016","count":2}, '
                '{"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688","order_date":"6/12/2016","count":2}, '
                '{"product_id":"FUR-TA-10000577","order_id":"US-2015-108966","order_date":"10/11/2015","count":2}, '
                '{"product_id":"FUR-CH-10000454","order_id":"CA-2016-152156","order_date":"11/8/2016","count":2}]}'
            ),
            "2025-02-11 00:11:41"
        ),
        (
            "your_product_a67e4db2-e7de-11ef-bb70-4240eb7a97f9",
            "your_product",
            "dq_spark_dev.customer_order",
            "product_missing_count_threshold",
            "testing_sample",
            "target_f1",
            "_source_dq",
            (
                '{target_f1=[{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156","order_date":"11/8/2016","count":2}, '
                '{"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688","order_date":"6/12/2016","count":2}, '
                '{"product_id":"FUR-TA-10000577","order_id":"US-2015-108966","order_date":"10/11/2015","count":2}, '
                '{"product_id":"FUR-CH-10000454","order_id":"CA-2016-152156","order_date":"11/8/2016","count":2}]}'
            ),
            "NULL",
            "2025-02-11 00:11:41"
        )
    ]

    # Create DataFrame
    df_custom_table_test = spark.createDataFrame(data_1, schema_1)
    # Create DataFrame


    context=SparkExpectationsContext("product_id",spark)
    context.set_stats_detailed_dataframe(df_detailed_table_test)
    context.set_custom_detailed_dataframe(df_custom_table_test)
    test_report=SparkExpectationsReport(context)
    test_result, test_df = test_report.dq_obs_report_data_insert()
    return test_result, test_df

@pytest.mark.usefixtures("test_dq_obs_report_data_insert")
def test_report_dataframe(test_dq_obs_report_data_insert):
    test_result, test_df = test_dq_obs_report_data_insert
    assert isinstance(test_result, bool), f"Expected test_result to be of type bool, but got {type(test_result)}"
    assert isinstance(test_df, DataFrame), f"Expected test_df to be of type DataFrame, but got {type(test_df)}"


@pytest.mark.usefixtures("test_dq_obs_report_data_insert")
def test_report_dataframe_columns(test_dq_obs_report_data_insert):
    test_result, test_df = test_dq_obs_report_data_insert
    expected_column_count = 15
    assert len(
        test_df.columns) == expected_column_count, f"Expected {expected_column_count} columns, but got {len(test_df.columns)}"


@pytest.mark.usefixtures("test_dq_obs_report_data_insert")
def test_dataframe_not_empty(test_dq_obs_report_data_insert):
    _, test_df = test_dq_obs_report_data_insert
    assert test_df.count() > 0, "DataFrame should not be empty"


@pytest.mark.usefixtures("test_dq_obs_report_data_insert")
def test_specific_columns_presence(test_dq_obs_report_data_insert):
    _, test_df = test_dq_obs_report_data_insert
    expected_columns = ["run_id", "product_id", "table_name", "rule", "column_name"]
    for column in expected_columns:
        assert column in test_df.columns, f"Column {column} should be present in the DataFrame"

@pytest.mark.usefixtures("test_dq_obs_report_data_insert")
def test_dataframe_schema(test_dq_obs_report_data_insert):
    _, test_df = test_dq_obs_report_data_insert
    expected_schema = StructType([
        StructField("rule", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("dq_time", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("total_records", StringType(), True),
        StructField("failed_records", LongType(), True),
        StructField("valid_records", StringType(), True),
        StructField("success_percentage", DoubleType(), True),
        StructField("run_id", StringType(), True),
        StructField("job", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("Snapshot", StringType(), True),
        StructField("data_object_name", StringType(), True)
    ])
    assert test_df.schema == expected_schema, "DataFrame schema does not match the expected schema"


@pytest.mark.usefixtures("test_dq_obs_report_data_insert")
def test_success_percentage_data(test_dq_obs_report_data_insert):
    _, test_df = test_dq_obs_report_data_insert
    assert test_df.filter(test_df["success_percentage"] > 100).count() == 0, "success_percentage should not be greater than 100"
    assert test_df.filter(test_df["success_percentage"] < 0).count() == 0, "success_percentage should not be negative"


@pytest.mark.usefixtures("test_dq_obs_report_data_insert")
def test_failed_records_greater_than_zero(test_dq_obs_report_data_insert):
    _, test_df = test_dq_obs_report_data_insert
    assert test_df.filter(test_df["failed_records"].cast("int") < 0).count() == 0, "failed_records should not  be less  than 0"















