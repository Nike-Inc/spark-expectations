import os
from unittest.mock import patch, Mock
import pytest

# from pytest_mock import mocker // this will be automatically used while running using py-test
from spark_expectations.core import get_spark_session
from spark_expectations.utils.reader import SparkExpectationsReader
from spark_expectations.core.context import SparkExpectationsContext
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import BooleanType
from spark_expectations.core.exceptions import (
    SparkExpectationsUserInputOrConfigInvalidException,
    SparkExpectationsMiscException,
)

spark = get_spark_session()


@pytest.fixture(name="_fixture_product_rules_view")
def fixture_product_rules():
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "../resources/product_rules.csv"))
    )

    # Set up the mock dataframe as a temporary table
    df.createOrReplaceTempView("product_rules")
    yield "product_rules_view"
    spark.catalog.dropTempView("product_rules")


@pytest.fixture(name="_fixture_product_rules_schema_view")
def fixture_product_rules_schema():
    csvSchema = StructType(
        [
            StructField("product_id", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("rule_type", StringType(), False),
            StructField("rule", StringType(), False),
            StructField("column_name", StringType(), False),
            StructField("expectation", StringType(), False),
            StructField("action_if_failed", StringType(), False),
            StructField("tag", StringType(), False),
            StructField("description", StringType(), False),
            StructField("enable_for_source_dq_validation", BooleanType(), False),
            StructField("enable_for_target_dq_validation", BooleanType(), False),
            StructField("is_active", BooleanType(), False),
            StructField("enable_error_drop_alert", BooleanType(), False),
            StructField("error_drop_threshold", IntegerType(), False),
            StructField("query_dq_delimiter", StringType(), True),
            StructField("enable_querydq_custom_output", BooleanType(), True),
        ]
    )

    df_with_schema = (
        spark.read.option("header", "true")
        .option("delimiter", "|")
        .schema(csvSchema)
        .csv(os.path.join(os.path.dirname(__file__), "../resources/product_rules_pipe.csv"))
    )

    # Set up the mock dataframe as a temporary table
    df_with_schema.createOrReplaceTempView("product_rules_schema")
    yield "product_rules_schema_view"
    spark.catalog.dropTempView("product_rules_schema")


@pytest.fixture(name="_fixture_product_rules_view_pipecsv")
def fixture_product_rules_pipe():
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", "|")
        .csv(os.path.join(os.path.dirname(__file__), "../resources/product_rules_pipe.csv"))
    )

    # Set up the mock dataframe as a temporary table
    df.createOrReplaceTempView("product_rules_pipe")
    yield "product_rules_view"
    spark.catalog.dropTempView("product_rules_pipe")




@pytest.mark.usefixtures("_fixture_product_rules_view")
@pytest.mark.parametrize(
    "product_id, table_name, tag, expected_output",
    [
        ("product1", "table1", "tag2", {"rule2": "expectation2"}),
        ("product2", "table1", None, {"rule5": "expectation5", "rule7": "expectation7", "rule12": "expectation12"}),
        (
            "product1",
            "table1",
            None,
            {
                "rule1": "expectation1",
                "rule2": "expectation2",
                "rule3": "expectation3",
                "rule6": "expectation6",
                "rule10": "expectation10",
                "rule13": "expectation13",
            },
        ),
        ("product2", "table2", "tag7", {}),
    ],
)
def test_get_rules_dlt(product_id, table_name, tag, expected_output, mocker, _fixture_product_rules_view):
    # create mock _context object
    mock_context = mocker.MagicMock()

    mock_context.spark = spark
    mock_context.product_id = product_id
    # Create an instance of the class and set the product_id
    reade_handler = SparkExpectationsReader(mock_context)
    dq_queries, rules_dlt, rules_settings = reade_handler.get_rules_from_df(
        spark.sql("select * from product_rules"), table_name, True, tag
    )

    # Assert
    assert rules_dlt == expected_output


@pytest.mark.usefixtures("_fixture_product_rules_view_pipecsv")
@pytest.mark.usefixtures("_fixture_product_rules_schema_view")
@pytest.mark.parametrize(
    "product_id, table_name, expected_expectations, expected_rule_execution_settings",
    [
        (
            "product1",
            "table1",
            {
                "target_table_name": "table1",
                "row_dq_rules": [
                    {
                        "product_id": "product1",
                        "table_name": "table1",
                        "rule_type": "row_dq",
                        "rule": "rule1",
                        "column_name": "column1",
                        "expectation": "expectation1",
                        "action_if_failed": "fail",
                        "enable_for_source_dq_validation": True,
                        "enable_for_target_dq_validation": True,
                        "tag": "tag1",
                        "description": "description1",
                        "enable_error_drop_alert": True,
                        "error_drop_threshold": 10,
                    },
                    {
                        "product_id": "product1",
                        "table_name": "table1",
                        "rule_type": "row_dq",
                        "rule": "rule2",
                        "column_name": "column2",
                        "expectation": "expectation2",
                        "action_if_failed": "drop",
                        "enable_for_source_dq_validation": True,
                        "enable_for_target_dq_validation": True,
                        "tag": "tag2",
                        "description": "description2",
                        "enable_error_drop_alert": False,
                        "error_drop_threshold": 0,
                    },
                    {
                        "action_if_failed": "ignore",
                        "column_name": "column3",
                        "description": "description3",
                        "enable_error_drop_alert": False,
                        "enable_for_source_dq_validation": True,
                        "enable_for_target_dq_validation": True,
                        "error_drop_threshold": 0,
                        "expectation": "expectation3",
                        "product_id": "product1",
                        "rule": "rule3",
                        "rule_type": "row_dq",
                        "table_name": "table1",
                        "tag": "tag3",
                    },
                ],
                "agg_dq_rules": [
                    {
                        "product_id": "product1",
                        "table_name": "table1",
                        "rule_type": "agg_dq",
                        "rule": "rule6",
                        "column_name": "column3",
                        "expectation": "expectation6",
                        "action_if_failed": "fail",
                        "enable_for_source_dq_validation": True,
                        "enable_for_target_dq_validation": True,
                        "tag": "tag6",
                        "description": "description6",
                        "enable_error_drop_alert": False,
                        "error_drop_threshold": 0,
                    },
                    {
                        "action_if_failed": "ignore",
                        "column_name": "column7",
                        "description": "description10",
                        "enable_error_drop_alert": False,
                        "enable_for_source_dq_validation": False,
                        "enable_for_target_dq_validation": True,
                        "error_drop_threshold": 0,
                        "expectation": "expectation10",
                        "product_id": "product1",
                        "rule": "rule10",
                        "rule_type": "agg_dq",
                        "table_name": "table1",
                        "tag": "tag10",
                    },
                ],
                "query_dq_rules": [
                    {
                        "product_id": "product1",
                        "table_name": "table1",
                        "rule_type": "query_dq",
                        "rule": "rule13",
                        "column_name": "column10",
                        "expectation": "expectation13expectation13a",
                        "action_if_failed": "fail",
                        "enable_for_source_dq_validation": True,
                        "enable_for_target_dq_validation": False,
                        "tag": "tag13",
                        "description": "description13",
                        "enable_error_drop_alert": False,
                        "enable_querydq_custom_output": True,
                        "expectation_source_f1": "expectation13a",
                        "error_drop_threshold": 0,
                    },
                    {
                        "product_id": "product1",
                        "table_name": "table1",
                        "rule_type": "query_dq",
                        "rule": "rule13",
                        "column_name": "column10",
                        "expectation": "expectation13expectation13a",
                        "action_if_failed": "fail",
                        "enable_for_source_dq_validation": True,
                        "enable_for_target_dq_validation": False,
                        "tag": "tag13",
                        "description": "description13",
                        "enable_error_drop_alert": False,
                        "enable_querydq_custom_output": False,
                        "expectation_source_f1": "expectation13a",
                        "error_drop_threshold": 0,
                    },
                    {
                        "product_id": "product1",
                        "table_name": "table1",
                        "rule_type": "query_dq",
                        "rule": "rule13",
                        "column_name": "column10",
                        "expectation": "expectation13expectation13a",
                        "action_if_failed": "fail",
                        "enable_for_source_dq_validation": True,
                        "enable_for_target_dq_validation": False,
                        "tag": "tag13",
                        "description": "description13",
                        "enable_error_drop_alert": False,
                        "enable_querydq_custom_output": False,
                        "expectation_source_f1": "expectation13a",
                        "error_drop_threshold": 0,
                    },
                    {
                        "product_id": "product1",
                        "table_name": "table1",
                        "rule_type": "query_dq",
                        "rule": "rule13",
                        "column_name": "column10",
                        "expectation": "expectation13expectation13a",
                        "action_if_failed": "fail",
                        "enable_for_source_dq_validation": True,
                        "enable_for_target_dq_validation": False,
                        "tag": "tag13",
                        "description": "description13",
                        "enable_error_drop_alert": False,
                        "enable_querydq_custom_output": False,
                        "expectation_source_f1": "expectation13a",
                        "error_drop_threshold": 0,
                    },
                ],
            },
            {
                # should be the output of the _get_rules_execution_settings from reader.py
                "row_dq": True,
                "source_agg_dq": True,
                "target_agg_dq": True,
                "source_query_dq": True,
                "target_query_dq": False,
            },
        )
    ],
)
def test_get_rules_from_table(
    product_id, table_name, expected_expectations, expected_rule_execution_settings, _fixture_product_rules_view_pipecsv
):
    # Create an instance of the class and set the product_id

    mock_context = Mock(spec=SparkExpectationsContext)
    setattr(mock_context, "get_row_dq_rule_type_name", "row_dq")
    setattr(mock_context, "get_agg_dq_rule_type_name", "agg_dq")
    setattr(mock_context, "get_query_dq_rule_type_name", "query_dq")
    mock_context.spark = spark
    mock_context.product_id = product_id

    reader_handler = SparkExpectationsReader(mock_context)

    dq_queries_dict, expectations, rule_execution_settings = reader_handler.get_rules_from_df(
        spark.sql(" select * from product_rules_pipe"), table_name, is_dlt=False
    )

    # Assert
    assert expectations == expected_expectations
    assert rule_execution_settings == expected_rule_execution_settings

    dq_queries_dict, expectations, rule_execution_settings = reader_handler.get_rules_from_df(
        spark.sql(" select * from product_rules_schema"), table_name, is_dlt=False
    )

    # Assert
    assert expectations == expected_expectations
    assert rule_execution_settings == expected_rule_execution_settings


@pytest.mark.usefixtures("_fixture_product_rules_view")
@pytest.mark.parametrize("product_id, table_name", [("product1", "table1")])
def test_get_rules_detailed_result_exception(product_id, table_name):
    _mock_context = Mock(spec=SparkExpectationsContext)
    _mock_context.spark = spark
    product_id = product_id
    setattr(_mock_context, "get_row_dq_rule_type_name", "row_dq")
    setattr(_mock_context, "get_agg_dq_rule_type_name", "agg_dq")
    setattr(_mock_context, "get_query_dq_rule_type_name", "query_dq")
    setattr(_mock_context, "get_query_dq_detailed_stats_status", True)
    _reader_handler = SparkExpectationsReader(_mock_context)

    with pytest.raises(SparkExpectationsMiscException, match=r"error occurred while retrieving rules list .*"):
        _reader_handler.get_rules_from_df(spark.sql(" select * from product_rules"), table_name)

