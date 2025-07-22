import re

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

from spark_expectations.core.exceptions import (
    SparkExpectationsInvalidAggDQExpectationException,
    SparkExpectationsInvalidQueryDQExpectationException,
    SparkExpectationsInvalidRowDQExpectationException,
    SparkExpectationsInvalidRuleTypeException)
from spark_expectations.utils.validate_rules import \
    SparkExpectationsValidateRules


@pytest.fixture(scope="session")
def spark():
    """
    Returns a Spark session
    """
    return SparkSession.builder.appName("DQValidation").getOrCreate()


@pytest.fixture
def sample_df(spark):
    """
    Returns a spark dataframe
    """
    return spark.createDataFrame([{"col1": 1, "col2": 10}, {"col1": 2, "col2": 20}, {"col1": 3, "col2": 30}])


@pytest.mark.parametrize(
    "expectation",
    [
        "col1 is null",
        "col1 is not null",
        "col1 > 10",
        "col2 < 25",
        "(col1 % 2) = 0",
    ],
)
def test_valid_row_dq(sample_df, expectation, spark):
    """
    Test with valid row data quality rules
    """
    rule = {
        "rule_type": "row_dq",
        "expectation": expectation,
        "rule": "valid_row_dq",
    }
    SparkExpectationsValidateRules.validate_expectation(sample_df, rule, spark)


@pytest.mark.parametrize(
    "expectation",
    [
        "sum(col1) > 20",
        "avg(col2) < 25",
        "min(col1) > 10",
        "stddev(col2) > 10",
        "count(distinct col1) > 4",
        "avg(col1) > 4",
        "avg(col2) > 18 and avg(col2) < 2",
        "avg(col1) between 18 and 25",
    ],
)
def test_valid_agg_dq(sample_df, expectation, spark):
    """
    Test with valid aggregate data quality rules
    """
    rule = {
        "rule_type": "agg_dq",
        "expectation": expectation,
        "rule": "valid_agg_dq",
    }
    SparkExpectationsValidateRules.validate_expectation(sample_df, rule, spark)


@pytest.mark.parametrize(
    "expectation",
    [
        "(select sum(col1) from test_table) > 10",
        "(select stddev(col2) from test_table) > 0",
        "(select max(col1) from test_final_table_view) > 10",
        "(select min(col1) from test_final_table_view) > 0",
        "(select count(col1) from test_final_table_view) > 3",
        "(select count(case when col2>0 then 1 else 0 end) from test_final_table_view) > 10",
        "(select sum(col1) from {table}) > 10",
        "(select count(*) from test_table) > 10",
    ],
)
def test_valid_query_dq(sample_df, expectation, spark):
    """
    Test with valid query data quality rules
    """
    rule = {
        "rule_type": "query_dq",
        "expectation": expectation,
        "rule": "valid_query_dq",
    }
    SparkExpectationsValidateRules.validate_expectation(sample_df, rule, spark)


@pytest.mark.parametrize(
    "expectation",
    [
        "sum(col1) > 20",  # This is an agg_dq
        "(select stddev(col2) from test_table) > 0",  # This is a query_dq
        "non_existing_col > 20",  # non_existing_col does not exist
    ],
)
def test_invalid_row_dq(sample_df, expectation, spark):
    """
    Test with invalid row data quality rules
    """
    rule = {
        "rule_type": "row_dq",
        "expectation": expectation,
        "rule": "invalid_row_dq",
    }
    with pytest.raises(SparkExpectationsInvalidRowDQExpectationException):
        SparkExpectationsValidateRules.validate_expectation(sample_df, rule, spark)


@pytest.mark.parametrize(
    "expectation",
    [
        "col1 > 20",  # This is a row_dq
        "(select stddev(col2) from test_table) > 0",  # This is a query_dq
        "sum(non_existing_col) > 20",  # non_existing_col does not exist
    ],
)
def test_invalid_agg_dq(sample_df, expectation, spark):
    """
    Test with invalid aggregate data quality rules
    """
    rule = {
        "rule_type": "agg_dq",
        "expectation": expectation,
        "rule": "invalid_agg_dq",
    }
    with pytest.raises(SparkExpectationsInvalidAggDQExpectationException):
        SparkExpectationsValidateRules.validate_expectation(sample_df, rule, spark)


@pytest.mark.parametrize(
    "expectation",
    [
        "col1 > 20",  # This is a row_dq
        "avg(col1) < 100",  # This is a agg_dq
        "SELECT SUM(col1) > 5 AS result",  # Syntax error
    ],
)
def test_invalid_query_dq(sample_df, expectation, spark):
    """
    Test with invalid query data quality rules
    """
    rule = {
        "rule_type": "query_dq",
        "expectation": expectation,
        "rule": "invalid_query_dq",
    }
    with pytest.raises(SparkExpectationsInvalidQueryDQExpectationException):
        SparkExpectationsValidateRules.validate_expectation(sample_df, rule, spark)


@pytest.mark.parametrize(
    "expectation",
    [
        "col1 > 20",
    ],
)
def test_invalid_rule_type_exception(sample_df, expectation, spark):
    """
    Test with invalid rule type
    """
    rule = {
        "rule_type": "foo_dq",
        "expectation": expectation,
        "rule": "invalid_rule_type",
    }
    with pytest.raises(SparkExpectationsInvalidRuleTypeException):
        SparkExpectationsValidateRules.validate_expectation(sample_df, rule, spark)

