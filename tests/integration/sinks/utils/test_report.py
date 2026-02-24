from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core import get_spark_session
from spark_expectations.core.expectations import DataFrame
from spark_expectations.sinks.utils.report import SparkExpectationsReport
from pyspark.sql.types import (
    StructField,
    IntegerType,
    StringType,
    StructType,
    TimestampType,
    FloatType,
    DoubleType,
    LongType,
)
import pytest

spark = get_spark_session()


@pytest.fixture(scope="module")
def test_dq_obs_report_data_insert():
    schema = StructType(
        [
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
            StructField("source_dq_actual_row_count", StringType(), True),
            StructField("source_dq_error_row_count", StringType(), True),
            StructField("source_dq_row_count", StringType(), True),
            StructField("source_dq_start_time", StringType(), True),
            StructField("source_dq_end_time", StringType(), True),
            StructField("target_expectations", StringType(), True),
            StructField("target_dq_status", StringType(), True),
            StructField("target_dq_actual_outcome", StringType(), True),
            StructField("target_dq_expected_outcome", StringType(), True),
            StructField("target_dq_actual_row_count", StringType(), True),
            StructField("target_dq_error_row_count", StringType(), True),
            StructField("target_dq_row_count", StringType(), True),
            StructField("target_dq_start_time", StringType(), True),
            StructField("target_dq_end_time", StringType(), True),
            StructField("dq_date", StringType(), True),
            StructField("dq_time", StringType(), True),
            StructField("dq_job_metadata_info", StringType(), True),
        ]
    )
    # when _create_schema runs to create this DataFrame, all fields are StringType

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
            "8",
            "8",
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
            ),
        ),
        (
            "your_product_2aacbbd4-e7de-11ef-bac4-4240eb7a97f9",
            "your_product",
            "dq_spark_dev.customer_order",
            "query_dq",
            "order_date_check",
            "order_date",
            ("SELECT COUNT(*) FROM order_source WHERE order_date IS NULL"),
            "completeness",
            "null check",
            "pass",
            "0",
            "0",
            "100",
            "0",
            "100",
            "2025-02-11 00:08:39",
            "2025-02-11 00:08:40",
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
            "2025-02-11 00:08:46",
            (
                "{'job': 'na_CORL_DIGITAL_source_to_o9', 'Region': 'NA', 'env': 'dev', 'Snapshot': '2024-04-15', "
                "'data_object_name ': 'customer_order'}"
            ),
        ),
        (
            "your_product_3aacbbd4-e7de-11ef-bac4-4240eb7a97f9",
            "your_product",
            "dq_spark_dev.customer_order",
            "query_dq",
            "order_id_uniqueness",
            "order_id",
            (
                "SELECT COUNT(*) FROM (SELECT order_id, COUNT(*) FROM order_source GROUP BY order_id HAVING COUNT(*) > 1)"
            ),
            "uniqueness",
            "duplicate check",
            "fail",
            "5",
            "0",
            "95",
            "5",
            "100",
            "2025-02-11 00:09:39",
            "2025-02-11 00:09:40",
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
            "2025-02-11 00:09:46",
            (
                "{'job': 'na_CORL_DIGITAL_source_to_o9', 'Region': 'NA', 'env': 'dev', 'Snapshot': '2024-04-15', "
                "'data_object_name ': 'customer_order'}"
            ),
        ),
        (
            "your_product_4aacbbd4-e7de-11ef-bac4-4240eb7a97f9",
            "your_product",
            "dq_spark_dev.customer_order",
            "query_dq",
            "product_id_format_check",
            "product_id",
            ("SELECT COUNT(*) FROM order_source WHERE product_id NOT LIKE 'PROD-%'"),
            "format",
            "pattern check",
            "pass",
            "0",
            "0",
            "100",
            "0",
            "100",
            "2025-02-11 00:10:39",
            "2025-02-11 00:10:40",
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
            "2025-02-11 00:10:46",
            (
                "{'job': 'na_CORL_DIGITAL_source_to_o9', 'Region': 'NA', 'env': 'dev', 'Snapshot': '2024-04-15', "
                "'data_object_name ': 'customer_order'}"
            ),
        ),
    ]
    # Create DataFrame
    df_detailed_table_test = spark.createDataFrame(data, schema)

    schema_1 = StructType(
        [
            StructField("run_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("rule", StringType(), True),
            StructField("column_name", StringType(), True),
            StructField("alias", StringType(), True),
            StructField("dq_type", StringType(), True),
            StructField("source_output", StringType(), True),
            StructField("target_output", StringType(), True),
            StructField("dq_time", StringType(), True),
        ]
    )

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
            "2025-02-11 00:11:41",
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
            "2025-02-11 00:11:41",
        ),
        (
            "your_product_b67e4db2-e7de-11ef-bb70-4240eb7a97f9",
            "your_product",
            "dq_spark_dev.customer_order",
            "order_date_check",
            "order_date",
            "source_f2",
            "_source_dq",
            (
                '{source_f2=[{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156","order_date":"11/8/2016","count":1}, '
                '{"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688","order_date":"6/12/2016","count":1}]}'
            ),
            (
                '{target_f2=[{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156","order_date":"11/8/2016","count":2}, '
                '{"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688","order_date":"6/12/2016","count":2}]}'
            ),
            "2025-02-11 00:12:41",
        ),
        (
            "your_product_c67e4db2-e7de-11ef-bb70-4240eb7a97f9",
            "your_product",
            "dq_spark_dev.customer_order",
            "order_id_uniqueness",
            "order_id",
            "source_f3",
            "_source_dq",
            (
                '{source_f3=[{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156","order_date":"11/8/2016","count":1}, '
                '{"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688","order_date":"6/12/2016","count":1}]}'
            ),
            (
                '{target_f3=[{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156","order_date":"11/8/2016","count":2}, '
                '{"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688","order_date":"6/12/2016","count":2}]}'
            ),
            "2025-02-11 00:13:41",
        ),
        (
            "your_product_d67e4db2-e7de-11ef-bb70-4240eb7a97f9",
            "your_product",
            "dq_spark_dev.customer_order",
            "product_id_format_check",
            "product_id",
            "source_f4",
            "_source_dq",
            (
                '{source_f4=[{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156","order_date":"11/8/2016","count":1}, '
                '{"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688","order_date":"6/12/2016","count":1}]}'
            ),
            (
                '{target_f4=[{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156","order_date":"11/8/2016","count":2}, '
                '{"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688","order_date":"6/12/2016","count":2}]}'
            ),
            "2025-02-11 00:14:41",
        ),
    ]

    # Create DataFrame
    df_custom_table_test = spark.createDataFrame(data_1, schema_1)
    # Create DataFrame

    context = SparkExpectationsContext("product_id", spark)
    context.set_stats_detailed_dataframe(df_detailed_table_test)
    context.set_custom_detailed_dataframe(df_custom_table_test)
    test_report = SparkExpectationsReport(context)
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
    assert (
        len(test_df.columns) == expected_column_count
    ), f"Expected {expected_column_count} columns, but got {len(test_df.columns)}"


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
    expected_schema = StructType(
        [
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
            StructField("data_object_name", StringType(), True),
        ]
    )
    assert test_df.schema == expected_schema, "DataFrame schema does not match the expected schema"


@pytest.mark.usefixtures("test_dq_obs_report_data_insert")
def test_success_percentage_data(test_dq_obs_report_data_insert):
    _, test_df = test_dq_obs_report_data_insert
    assert (
        test_df.filter(test_df["success_percentage"] > 100).count() == 0
    ), "success_percentage should not be greater than 100"
    assert test_df.filter(test_df["success_percentage"] < 0).count() == 0, "success_percentage should not be negative"


@pytest.mark.usefixtures("test_dq_obs_report_data_insert")
def test_failed_records_greater_than_zero(test_dq_obs_report_data_insert):
    _, test_df = test_dq_obs_report_data_insert
    assert (
        test_df.filter(test_df["failed_records"].cast("int") < 0).count() == 0
    ), "failed_records should not  be less  than 0"


# ---------------------------------------------------------------------------
# Fixtures and tests for StringType row-count columns (production schema).
#
# In production, _create_schema (writer.py) creates all detailed-stats
# columns as StringType.  The tests above use IntegerType which silently
# allows arithmetic; the tests below prove the .cast("double") fix in
# report.py works when the columns arrive as strings.
# ---------------------------------------------------------------------------


def _build_string_typed_detailed_df(extra_rows=None):
    """Helper that builds a detailed-stats DataFrame with StringType row-count
    columns, matching the production schema produced by _create_schema."""
    schema = StructType(
        [
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
            StructField("source_dq_actual_row_count", StringType(), True),
            StructField("source_dq_error_row_count", StringType(), True),
            StructField("source_dq_row_count", StringType(), True),
            StructField("source_dq_start_time", StringType(), True),
            StructField("source_dq_end_time", StringType(), True),
            StructField("target_expectations", StringType(), True),
            StructField("target_dq_status", StringType(), True),
            StructField("target_dq_actual_outcome", StringType(), True),
            StructField("target_dq_expected_outcome", StringType(), True),
            StructField("target_dq_actual_row_count", StringType(), True),
            StructField("target_dq_error_row_count", StringType(), True),
            StructField("target_dq_row_count", StringType(), True),
            StructField("target_dq_start_time", StringType(), True),
            StructField("target_dq_end_time", StringType(), True),
            StructField("dq_date", StringType(), True),
            StructField("dq_time", StringType(), True),
            StructField("dq_job_metadata_info", StringType(), True),
        ]
    )

    data = [
        (
            "your_product_1aacbbd4-e7de-11ef-bac4-4240eb7a97f9",
            "your_product",
            "dq_spark_dev.customer_order",
            "query_dq",
            "product_missing_count_threshold",
            "testing_sample",
            "SELECT 1",
            "validity",
            "row count threshold",
            "fail",
            "1",
            ">3",
            None,
            "8",
            "8",
            "2025-02-11 00:07:39",
            "2025-02-11 00:07:40",
            None, None, None, None, None, None, None, None, None,
            "2025-02-10",
            "2025-02-11 00:07:46",
            "{'job': 'test_job', 'Region': 'NA', 'Snapshot': '2024-04-15', 'data_object_name': 'customer_order'}",
        ),
        (
            "your_product_2aacbbd4-e7de-11ef-bac4-4240eb7a97f9",
            "your_product",
            "dq_spark_dev.customer_order",
            "query_dq",
            "order_date_check",
            "order_date",
            "SELECT 1",
            "completeness",
            "null check",
            "pass",
            "0",
            "0",
            "100",
            "0",
            "100",
            "2025-02-11 00:08:39",
            "2025-02-11 00:08:40",
            None, None, None, None, None, None, None, None, None,
            "2025-02-10",
            "2025-02-11 00:08:46",
            "{'job': 'test_job', 'Region': 'NA', 'Snapshot': '2024-04-15', 'data_object_name': 'customer_order'}",
        ),
        (
            "your_product_3aacbbd4-e7de-11ef-bac4-4240eb7a97f9",
            "your_product",
            "dq_spark_dev.customer_order",
            "query_dq",
            "order_id_uniqueness",
            "order_id",
            "SELECT 1",
            "uniqueness",
            "duplicate check",
            "fail",
            "5",
            "0",
            "95",
            "5",
            "100",
            "2025-02-11 00:09:39",
            "2025-02-11 00:09:40",
            None, None, None, None, None, None, None, None, None,
            "2025-02-10",
            "2025-02-11 00:09:46",
            "{'job': 'test_job', 'Region': 'NA', 'Snapshot': '2024-04-15', 'data_object_name': 'customer_order'}",
        ),
    ]

    if extra_rows:
        data.extend(extra_rows)

    return spark.createDataFrame(data, schema)


def _build_custom_detailed_df():
    """Builds the custom-detailed (query DQ) DataFrame used alongside the
    detailed-stats DataFrame in the report fixture."""
    schema = StructType(
        [
            StructField("run_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("rule", StringType(), True),
            StructField("column_name", StringType(), True),
            StructField("alias", StringType(), True),
            StructField("dq_type", StringType(), True),
            StructField("source_output", StringType(), True),
            StructField("target_output", StringType(), True),
            StructField("dq_time", StringType(), True),
        ]
    )

    data = [
        (
            "your_product_a67e4db2-e7de-11ef-bb70-4240eb7a97f9",
            "your_product",
            "dq_spark_dev.customer_order",
            "product_missing_count_threshold",
            "testing_sample",
            "source_f1",
            "_source_dq",
            (
                '{source_f1=[{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156","order_date":"11/8/2016","count":1}]}'
            ),
            (
                '{target_f1=[{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156","order_date":"11/8/2016","count":2}]}'
            ),
            "2025-02-11 00:11:41",
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
                '{target_f1=[{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156","order_date":"11/8/2016","count":2}]}'
            ),
            "NULL",
            "2025-02-11 00:11:41",
        ),
    ]

    return spark.createDataFrame(data, schema)


def _run_report(df_detailed, df_custom):
    """Set up context, run the report, return (result_bool, result_df)."""
    context = SparkExpectationsContext("product_id", spark)
    context.set_stats_detailed_dataframe(df_detailed)
    context.set_custom_detailed_dataframe(df_custom)
    report = SparkExpectationsReport(context)
    return report.dq_obs_report_data_insert()


@pytest.fixture(scope="module")
def string_typed_report():
    """Fixture that exercises dq_obs_report_data_insert with StringType
    row-count columns, matching the production schema."""
    df_detailed = _build_string_typed_detailed_df()
    df_custom = _build_custom_detailed_df()
    return _run_report(df_detailed, df_custom)


def test_report_string_typed_columns_no_error(string_typed_report):
    """Core regression test: would have raised DATATYPE_MISMATCH before the
    .cast('double') fix on (valid_records / total_records)."""
    test_result, test_df = string_typed_report
    assert test_result is True
    assert isinstance(test_df, DataFrame)
    assert test_df.count() > 0


def test_report_string_typed_success_percentage_is_numeric(string_typed_report):
    """success_percentage must be DoubleType with values in [0, 100]."""
    _, test_df = string_typed_report

    sp_field = next(f for f in test_df.schema.fields if f.name == "success_percentage")
    assert sp_field.dataType == DoubleType(), (
        f"Expected success_percentage to be DoubleType, got {sp_field.dataType}"
    )

    non_null_rows = test_df.filter(test_df["success_percentage"].isNotNull())
    assert non_null_rows.filter(non_null_rows["success_percentage"] > 100).count() == 0, (
        "success_percentage should not exceed 100"
    )
    assert non_null_rows.filter(non_null_rows["success_percentage"] < 0).count() == 0, (
        "success_percentage should not be negative"
    )


def test_report_string_typed_columns_schema(string_typed_report):
    """Verify the output has the expected columns and that the key computed
    column (success_percentage) is DoubleType regardless of string inputs."""
    _, test_df = string_typed_report

    expected_columns = [
        "rule", "column_name", "dq_time", "product_id", "table_name",
        "status", "total_records", "failed_records", "valid_records",
        "success_percentage", "run_id",
        "job", "Region", "Snapshot", "data_object_name",
    ]
    for col_name in expected_columns:
        assert col_name in test_df.columns, f"Missing expected column: {col_name}"

    sp_field = next(f for f in test_df.schema.fields if f.name == "success_percentage")
    assert sp_field.dataType == DoubleType()


def test_report_with_null_string_row_counts():
    """Null values in StringType row-count columns should produce null
    success_percentage, not crash."""
    null_row = (
        "run_null_test",
        "your_product",
        "dq_spark_dev.customer_order",
        "query_dq",
        "null_check_rule",
        "some_col",
        "SELECT 1",
        "validity",
        "desc",
        "fail",
        "0",
        "0",
        None,   # source_dq_actual_row_count = null
        "0",
        None,   # source_dq_row_count = null
        "2025-02-11 00:07:39",
        "2025-02-11 00:07:40",
        None, None, None, None, None, None, None, None, None,
        "2025-02-10",
        "2025-02-11 00:07:46",
        "{'job': 'test_job', 'Region': 'NA', 'Snapshot': '2024-04-15', 'data_object_name': 'customer_order'}",
    )

    df_detailed = _build_string_typed_detailed_df(extra_rows=[null_row])
    df_custom = _build_custom_detailed_df()

    test_result, test_df = _run_report(df_detailed, df_custom)
    assert test_result is True

    null_run = test_df.filter(test_df["run_id"] == "run_null_test")
    assert null_run.count() == 1
    row = null_run.collect()[0]
    assert row["success_percentage"] is None, (
        "success_percentage should be null when both valid_records and total_records are null"
    )


def test_report_with_zero_string_total_records():
    """Division by zero (total_records = '0') should produce null or inf,
    not raise an exception."""
    zero_row = (
        "run_zero_test",
        "your_product",
        "dq_spark_dev.customer_order",
        "query_dq",
        "zero_total_rule",
        "some_col",
        "SELECT 1",
        "validity",
        "desc",
        "fail",
        "0",
        "0",
        "50",    # source_dq_actual_row_count (valid_records)
        "50",
        "0",     # source_dq_row_count (total_records) = 0 -> division by zero
        "2025-02-11 00:07:39",
        "2025-02-11 00:07:40",
        None, None, None, None, None, None, None, None, None,
        "2025-02-10",
        "2025-02-11 00:07:46",
        "{'job': 'test_job', 'Region': 'NA', 'Snapshot': '2024-04-15', 'data_object_name': 'customer_order'}",
    )

    df_detailed = _build_string_typed_detailed_df(extra_rows=[zero_row])
    df_custom = _build_custom_detailed_df()

    test_result, test_df = _run_report(df_detailed, df_custom)
    assert test_result is True

    zero_run = test_df.filter(test_df["run_id"] == "run_zero_test")
    assert zero_run.count() == 1
