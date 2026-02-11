"""
Pytest fixtures for spark_expectations.core unit tests.
"""
import pytest
from pyspark.sql.types import StructType, StructField, StringType

from spark_expectations.core import get_spark_session
from spark_expectations.core.expectations import SparkExpectations, WrappedDataFrameWriter


# Initialize spark session for tests
_spark = get_spark_session()


@pytest.fixture
def spark():
    """Spark session fixture for tests."""
    return _spark


@pytest.fixture
def rules_df_schema():
    """Schema for rules DataFrame used in tests."""
    return StructType([
        StructField("product_id", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("rule", StringType(), True),
        StructField("rule_type", StringType(), True),
        StructField("expectation", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("action_if_failed", StringType(), True),
        StructField("tag", StringType(), True),
        StructField("description", StringType(), True),
        StructField("enable_for_source_dq_validation", StringType(), True),
        StructField("enable_for_target_dq_validation", StringType(), True),
        StructField("is_active", StringType(), True),
        StructField("enable_error_drop_alert", StringType(), True),
        StructField("error_drop_threshold", StringType(), True),
        StructField("priority", StringType(), True),
    ])


@pytest.fixture
def se_instance(spark, rules_df_schema):
    """Create a SparkExpectations instance for testing."""
    rules_data = [{
        "product_id": "test_product",
        "table_name": "test_table",
        "rule": "test_rule",
        "rule_type": "row_dq",
        "expectation": "col1 > 0",
        "column_name": "col1",
        "action_if_failed": "ignore",
        "tag": "validity",
        "description": "Test rule",
        "enable_for_source_dq_validation": "True",
        "enable_for_target_dq_validation": "False",
        "is_active": "True",
        "enable_error_drop_alert": "False",
        "error_drop_threshold": "0",
        "priority": "medium",
    }]
    rules_df = spark.createDataFrame(rules_data, schema=rules_df_schema)
    writer = WrappedDataFrameWriter().mode("append").format("parquet")
    
    return SparkExpectations(
        product_id="test_product",
        rules_df=rules_df,
        stats_table="test_stats_table",
        stats_table_writer=writer,
        target_and_error_table_writer=writer,
        debugger=False,
    )


# =============================================================================
# Test Data Fixtures
# =============================================================================

@pytest.fixture
def base_rule_data():
    """Base rule data - standard test case with all fields populated."""
    return [{
        "product_id": "product1",
        "table_name": "table1",
        "rule": "rule1",
        "rule_type": "row_dq",
        "expectation": "col1 > 0",
        "column_name": "col1",
        "action_if_failed": "ignore",
        "tag": "validity",
        "description": "Test",
        "enable_for_source_dq_validation": "True",
        "enable_for_target_dq_validation": "False",
        "is_active": "True",
        "enable_error_drop_alert": "False",
        "error_drop_threshold": "0",
        "priority": "medium",
    }]


@pytest.fixture
def rule_data_product2():
    """Rule data with different product_id."""
    return [{
        "product_id": "product2",
        "table_name": "table1",
        "rule": "rule1",
        "rule_type": "row_dq",
        "expectation": "col1 > 0",
        "column_name": "col1",
        "action_if_failed": "ignore",
        "tag": "validity",
        "description": "Test",
        "enable_for_source_dq_validation": "True",
        "enable_for_target_dq_validation": "False",
        "is_active": "True",
        "enable_error_drop_alert": "False",
        "error_drop_threshold": "0",
        "priority": "medium",
    }]


@pytest.fixture
def rule_data_expectation_100():
    """Rule data with different expectation."""
    return [{
        "product_id": "product1",
        "table_name": "table1",
        "rule": "rule1",
        "rule_type": "row_dq",
        "expectation": "col1 > 100",
        "column_name": "col1",
        "action_if_failed": "ignore",
        "tag": "validity",
        "description": "Test",
        "enable_for_source_dq_validation": "True",
        "enable_for_target_dq_validation": "False",
        "is_active": "True",
        "enable_error_drop_alert": "False",
        "error_drop_threshold": "0",
        "priority": "medium",
    }]


@pytest.fixture
def rule_data_null_product_id():
    """Rule data with null product_id."""
    return [{
        "product_id": None,
        "table_name": "table1",
        "rule": "rule1",
        "rule_type": "row_dq",
        "expectation": "col1 > 0",
        "column_name": "col1",
        "action_if_failed": "ignore",
        "tag": "validity",
        "description": "Test",
        "enable_for_source_dq_validation": "True",
        "enable_for_target_dq_validation": "False",
        "is_active": "True",
        "enable_error_drop_alert": "False",
        "error_drop_threshold": "0",
        "priority": "medium",
    }]


@pytest.fixture
def rule_data_null_table_name():
    """Rule data with null table_name."""
    return [{
        "product_id": "product1",
        "table_name": None,
        "rule": "rule1",
        "rule_type": "row_dq",
        "expectation": "col1 > 0",
        "column_name": "col1",
        "action_if_failed": "ignore",
        "tag": "validity",
        "description": "Test",
        "enable_for_source_dq_validation": "True",
        "enable_for_target_dq_validation": "False",
        "is_active": "True",
        "enable_error_drop_alert": "False",
        "error_drop_threshold": "0",
        "priority": "medium",
    }]


@pytest.fixture
def rule_data_null_rule():
    """Rule data with null rule."""
    return [{
        "product_id": "product1",
        "table_name": "table1",
        "rule": None,
        "rule_type": "row_dq",
        "expectation": "col1 > 0",
        "column_name": "col1",
        "action_if_failed": "ignore",
        "tag": "validity",
        "description": "Test",
        "enable_for_source_dq_validation": "True",
        "enable_for_target_dq_validation": "False",
        "is_active": "True",
        "enable_error_drop_alert": "False",
        "error_drop_threshold": "0",
        "priority": "medium",
    }]


@pytest.fixture
def rule_data_null_rule_type():
    """Rule data with null rule_type."""
    return [{
        "product_id": "product1",
        "table_name": "table1",
        "rule": "rule1",
        "rule_type": None,
        "expectation": "col1 > 0",
        "column_name": "col1",
        "action_if_failed": "ignore",
        "tag": "validity",
        "description": "Test",
        "enable_for_source_dq_validation": "True",
        "enable_for_target_dq_validation": "False",
        "is_active": "True",
        "enable_error_drop_alert": "False",
        "error_drop_threshold": "0",
        "priority": "medium",
    }]


@pytest.fixture
def rule_data_all_null_ids():
    """Rule data with all null id fields."""
    return [{
        "product_id": None,
        "table_name": None,
        "rule": None,
        "rule_type": None,
        "expectation": "col1 > 0",
        "column_name": "col1",
        "action_if_failed": "ignore",
        "tag": "validity",
        "description": "Test",
        "enable_for_source_dq_validation": "True",
        "enable_for_target_dq_validation": "False",
        "is_active": "True",
        "enable_error_drop_alert": "False",
        "error_drop_threshold": "0",
        "priority": "medium",
    }]


@pytest.fixture
def multiple_rows_data():
    """Multiple rows with different values."""
    return [
        {
            "product_id": "product1",
            "table_name": "table1",
            "rule": "rule1",
            "rule_type": "row_dq",
            "expectation": "col1 > 0",
            "column_name": "col1",
            "action_if_failed": "ignore",
            "tag": "validity",
            "description": "Test 1",
            "enable_for_source_dq_validation": "True",
            "enable_for_target_dq_validation": "False",
            "is_active": "True",
            "enable_error_drop_alert": "False",
            "error_drop_threshold": "0",
            "priority": "medium",
        },
        {
            "product_id": "product2",
            "table_name": "table2",
            "rule": "rule2",
            "rule_type": "agg_dq",
            "expectation": "sum(col1) > 100",
            "column_name": "col1",
            "action_if_failed": "fail",
            "tag": "accuracy",
            "description": "Test 2",
            "enable_for_source_dq_validation": "True",
            "enable_for_target_dq_validation": "True",
            "is_active": "True",
            "enable_error_drop_alert": "True",
            "error_drop_threshold": "5",
            "priority": "high",
        },
    ]


@pytest.fixture
def same_id_different_expectation_data():
    """Two rows with same id fields but different expectations."""
    return [
        {
            "product_id": "product1",
            "table_name": "table1",
            "rule": "rule1",
            "rule_type": "row_dq",
            "expectation": "col1 > 0",
            "column_name": "col1",
            "action_if_failed": "ignore",
            "tag": "validity",
            "description": "Test 1",
            "enable_for_source_dq_validation": "True",
            "enable_for_target_dq_validation": "False",
            "is_active": "True",
            "enable_error_drop_alert": "False",
            "error_drop_threshold": "0",
            "priority": "medium",
        },
        {
            "product_id": "product1",
            "table_name": "table1",
            "rule": "rule1",
            "rule_type": "row_dq",
            "expectation": "col1 > 100",
            "column_name": "col2",
            "action_if_failed": "fail",
            "tag": "accuracy",
            "description": "Test 2",
            "enable_for_source_dq_validation": "False",
            "enable_for_target_dq_validation": "True",
            "is_active": "False",
            "enable_error_drop_alert": "True",
            "error_drop_threshold": "10",
            "priority": "low",
        },
    ]


@pytest.fixture
def different_id_same_expectation_data():
    """Two rows with different id fields but same expectation."""
    return [
        {
            "product_id": "product1",
            "table_name": "table1",
            "rule": "rule1",
            "rule_type": "row_dq",
            "expectation": "col1 > 0",
            "column_name": "col1",
            "action_if_failed": "ignore",
            "tag": "validity",
            "description": "Test 1",
            "enable_for_source_dq_validation": "True",
            "enable_for_target_dq_validation": "False",
            "is_active": "True",
            "enable_error_drop_alert": "False",
            "error_drop_threshold": "0",
            "priority": "medium",
        },
        {
            "product_id": "product2",
            "table_name": "table2",
            "rule": "rule2",
            "rule_type": "agg_dq",
            "expectation": "col1 > 0",
            "column_name": "col2",
            "action_if_failed": "fail",
            "tag": "accuracy",
            "description": "Test 2",
            "enable_for_source_dq_validation": "False",
            "enable_for_target_dq_validation": "True",
            "is_active": "False",
            "enable_error_drop_alert": "True",
            "error_drop_threshold": "10",
            "priority": "low",
        },
    ]


@pytest.fixture
def rule_data_with_whitespace():
    """Rule data with leading/trailing whitespace in id fields."""
    return [{
        "product_id": "  product1  ",
        "table_name": " table1 ",
        "rule": "  rule1",
        "rule_type": "row_dq  ",
        "expectation": "col1 > 0",
        "column_name": "col1",
        "action_if_failed": "ignore",
        "tag": "validity",
        "description": "Test",
        "enable_for_source_dq_validation": "True",
        "enable_for_target_dq_validation": "False",
        "is_active": "True",
        "enable_error_drop_alert": "False",
        "error_drop_threshold": "0",
        "priority": "medium",
    }]


# =============================================================================
# Fixtures for _validate_rules tests
# =============================================================================

@pytest.fixture
def rules_df_schema_missing_product_id():
    """Schema missing product_id column."""
    return StructType([
        StructField("table_name", StringType(), True),
        StructField("rule", StringType(), True),
        StructField("rule_type", StringType(), True),
        StructField("expectation", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("action_if_failed", StringType(), True),
        StructField("tag", StringType(), True),
        StructField("description", StringType(), True),
        StructField("enable_for_source_dq_validation", StringType(), True),
        StructField("enable_for_target_dq_validation", StringType(), True),
        StructField("is_active", StringType(), True),
        StructField("enable_error_drop_alert", StringType(), True),
        StructField("error_drop_threshold", StringType(), True),
        StructField("priority", StringType(), True),
    ])


@pytest.fixture
def rules_df_schema_missing_multiple():
    """Schema missing product_id and rule_type columns."""
    return StructType([
        StructField("table_name", StringType(), True),
        StructField("rule", StringType(), True),
        StructField("expectation", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("action_if_failed", StringType(), True),
        StructField("tag", StringType(), True),
        StructField("description", StringType(), True),
        StructField("enable_for_source_dq_validation", StringType(), True),
        StructField("enable_for_target_dq_validation", StringType(), True),
        StructField("is_active", StringType(), True),
        StructField("enable_error_drop_alert", StringType(), True),
        StructField("error_drop_threshold", StringType(), True),
        StructField("priority", StringType(), True),
    ])


@pytest.fixture
def rule_data_missing_product_id():
    """Rule data without product_id field (for schema missing product_id)."""
    return [{
        "table_name": "table1",
        "rule": "rule1",
        "rule_type": "row_dq",
        "expectation": "col1 > 0",
        "column_name": "col1",
        "action_if_failed": "ignore",
        "tag": "validity",
        "description": "Test",
        "enable_for_source_dq_validation": "True",
        "enable_for_target_dq_validation": "False",
        "is_active": "True",
        "enable_error_drop_alert": "False",
        "error_drop_threshold": "0",
        "priority": "medium",
    }]


@pytest.fixture
def rule_data_missing_multiple():
    """Rule data without product_id and rule_type fields."""
    return [{
        "table_name": "table1",
        "rule": "rule1",
        "expectation": "col1 > 0",
        "column_name": "col1",
        "action_if_failed": "ignore",
        "tag": "validity",
        "description": "Test",
        "enable_for_source_dq_validation": "True",
        "enable_for_target_dq_validation": "False",
        "is_active": "True",
        "enable_error_drop_alert": "False",
        "error_drop_threshold": "0",
        "priority": "medium",
    }]


@pytest.fixture
def valid_rules_df(spark, rules_df_schema, base_rule_data):
    """Valid rules DataFrame with all required columns and no NULL values."""
    return spark.createDataFrame(base_rule_data, schema=rules_df_schema)


@pytest.fixture
def rules_df_missing_product_id_column(spark, rules_df_schema_missing_product_id, rule_data_missing_product_id):
    """Rules DataFrame missing product_id column."""
    return spark.createDataFrame(rule_data_missing_product_id, schema=rules_df_schema_missing_product_id)


@pytest.fixture
def rules_df_missing_multiple_columns(spark, rules_df_schema_missing_multiple, rule_data_missing_multiple):
    """Rules DataFrame missing product_id and rule_type columns."""
    return spark.createDataFrame(rule_data_missing_multiple, schema=rules_df_schema_missing_multiple)


@pytest.fixture
def rules_df_with_null_product_id(spark, rules_df_schema, rule_data_null_product_id):
    """Rules DataFrame with NULL product_id value."""
    return spark.createDataFrame(rule_data_null_product_id, schema=rules_df_schema)


@pytest.fixture
def rules_df_with_null_table_name(spark, rules_df_schema, rule_data_null_table_name):
    """Rules DataFrame with NULL table_name value."""
    return spark.createDataFrame(rule_data_null_table_name, schema=rules_df_schema)


@pytest.fixture
def rules_df_with_all_null_ids(spark, rules_df_schema, rule_data_all_null_ids):
    """Rules DataFrame with all NULL values in required columns."""
    return spark.createDataFrame(rule_data_all_null_ids, schema=rules_df_schema)


@pytest.fixture
def rules_df_with_partial_null_rows(spark, rules_df_schema):
    """Rules DataFrame with some rows having NULL values."""
    data = [
        {
            "product_id": "product1",
            "table_name": "table1",
            "rule": "rule1",
            "rule_type": "row_dq",
            "expectation": "col1 > 0",
            "column_name": "col1",
            "action_if_failed": "ignore",
            "tag": "validity",
            "description": "Test 1",
            "enable_for_source_dq_validation": "True",
            "enable_for_target_dq_validation": "False",
            "is_active": "True",
            "enable_error_drop_alert": "False",
            "error_drop_threshold": "0",
            "priority": "medium",
        },
        {
            "product_id": None,  # NULL in second row
            "table_name": "table2",
            "rule": "rule2",
            "rule_type": "row_dq",
            "expectation": "col2 > 0",
            "column_name": "col2",
            "action_if_failed": "ignore",
            "tag": "validity",
            "description": "Test 2",
            "enable_for_source_dq_validation": "True",
            "enable_for_target_dq_validation": "False",
            "is_active": "True",
            "enable_error_drop_alert": "False",
            "error_drop_threshold": "0",
            "priority": "medium",
        },
    ]
    return spark.createDataFrame(data, schema=rules_df_schema)


@pytest.fixture
def empty_rules_df(spark, rules_df_schema):
    """Empty rules DataFrame with correct schema but no rows."""
    return spark.createDataFrame([], schema=rules_df_schema)


@pytest.fixture
def rule_data_empty_string_product_id():
    """Rule data with empty string product_id."""
    return [{
        "product_id": "",
        "table_name": "table1",
        "rule": "rule1",
        "rule_type": "row_dq",
        "expectation": "col1 > 0",
        "column_name": "col1",
        "action_if_failed": "ignore",
        "tag": "validity",
        "description": "Test",
        "enable_for_source_dq_validation": "True",
        "enable_for_target_dq_validation": "False",
        "is_active": "True",
        "enable_error_drop_alert": "False",
        "error_drop_threshold": "0",
        "priority": "medium",
    }]


@pytest.fixture
def rule_data_whitespace_only_product_id():
    """Rule data with whitespace-only product_id."""
    return [{
        "product_id": "   ",
        "table_name": "table1",
        "rule": "rule1",
        "rule_type": "row_dq",
        "expectation": "col1 > 0",
        "column_name": "col1",
        "action_if_failed": "ignore",
        "tag": "validity",
        "description": "Test",
        "enable_for_source_dq_validation": "True",
        "enable_for_target_dq_validation": "False",
        "is_active": "True",
        "enable_error_drop_alert": "False",
        "error_drop_threshold": "0",
        "priority": "medium",
    }]


@pytest.fixture
def rule_data_multiple_empty_columns():
    """Rule data with multiple empty string columns."""
    return [{
        "product_id": "",
        "table_name": "   ",
        "rule": "rule1",
        "rule_type": "",
        "expectation": "col1 > 0",
        "column_name": "col1",
        "action_if_failed": "ignore",
        "tag": "validity",
        "description": "Test",
        "enable_for_source_dq_validation": "True",
        "enable_for_target_dq_validation": "False",
        "is_active": "True",
        "enable_error_drop_alert": "False",
        "error_drop_threshold": "0",
        "priority": "medium",
    }]
