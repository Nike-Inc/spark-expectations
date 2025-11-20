from unittest.mock import Mock
from unittest.mock import patch

import pytest
from pyspark.sql.functions import lit, struct, array, udf, create_map

from spark_expectations.core import get_spark_session
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.utils.actions import SparkExpectationsActions

spark = get_spark_session()


@pytest.fixture(name="_fixture_df")
def fixture_df():
    # Create a sample input dataframe
    _fixture_df = spark.createDataFrame(
        [
            {"row_id": 0, "col1": 1, "col2": "a"},
            {"row_id": 1, "col1": 2, "col2": "b"},
            {"row_id": 2, "col1": 3, "col2": "c"},
        ]
    )

    return _fixture_df


@pytest.fixture(name="_fixture_mock_context")
def fixture_mock_context():
    # fixture for mock context
    mock_object = Mock(spec=SparkExpectationsContext)
    mock_object.product_id = "product1"
    mock_object.spark = spark
    mock_object.get_row_dq_rule_type_name = "row_dq"
    mock_object.get_agg_dq_rule_type_name = "agg_dq"
    mock_object.get_query_dq_rule_type_name = "query_dq"
    mock_object.get_agg_dq_detailed_stats_status = True
    mock_object.get_query_dq_detailed_stats_status = True
    mock_object.get_querydq_secondary_queries = {
        "product_1|test_table|table_row_count_gt_1": {
            "source_f1": "select count(*) from query_test_table",
            "target_f1": "select count(*) from query_test_table_target",
        },
        "product_1|test_table|table_distinct_count": {
            "source_f1": "select distinct col1, col2 from query_test_table",
            "target_f1": "select distinct col1, col2 from query_test_table_target",
        },
    }

    mock_object.get_supported_df_query_dq = spark.createDataFrame([{"spark_expectations_test": "se_query_dq"}])
    return mock_object


@pytest.fixture(name="_fixture_query_dq_rule")
def fixture_query_dq_rule():
    # Define the expectations for the data quality rules
    return {
        "product_id": "product_1",
        "rule_type": "query_dq",
        "rule": "table_row_count_gt_1",
        "column_name": "col1",
        "expectation": "((select count(*) from query_test_table)-(select count(*) from query_test_table_target))>1",
        "action_if_failed": "ignore",
        "table_name": "test_table",
        "tag": "validity",
        "enable_for_target_dq_validation": True,
        "enable_for_source_dq_validation": True,
        "enable_querydq_custom_output": True,
        "expectation_source_f1": "select count(*) from query_test_table",
        "expectation_target_f1": "select count(*) from query_test_table_target",
        "description": "table count should be greater than 1",
    }


@pytest.fixture(name="_fixture_agg_dq_rule")
def fixture_agg_dq_rule():
    # Define the expectations for the data quality rules
    return {
        "rule_type": "agg_dq",
        "rule": "col1_sum_gt_eq_6",
        "column_name": "col1",
        "expectation": "sum(col1)>=6",
        "action_if_failed": "ignore",
        "table_name": "test_table",
        "tag": "validity",
        "enable_for_source_dq_validation": True,
        "description": "col1 sum gt 1",
        "product_id": "product_1",
    }


@pytest.fixture(name="_fixture_agg_dq_rule_type_range")
def _fixture_agg_dq_rule_type_range():
    # Define the expectations for the data quality rules
    return {
        "rule_type": "agg_dq",
        "rule": "col1_sum_gt_6_and_lt_10",
        "column_name": "col1",
        "expectation": "sum(col1)>6 and sum(col1)<10",
        "action_if_failed": "ignore",
        "table_name": "test_table",
        "tag": "validity",
        "enable_for_source_dq_validation": True,
        "description": "sum of col1 is greater than 6 and sum of col1 is less than 10",
        "product_id": "product_1",
    }

@pytest.fixture(name="_fixture_agg_dq_rule_type_range_upper_lower")
def _fixture_agg_dq_rule_type_range_upper_lower():
    return {
        "product_id": "product1",
        "rule_type": "agg_dq",
        "rule": "expect_row_count_to_be_in_range",
        "column_name": "",
        "expectation": "count(*) > 1 and count(*) < 10",
        "action_if_failed": "fail",
        "table_name": "test_table",
        "tag": "accuracy",
        "status": "fail",
        "enable_for_target_dq_validation": True,
        "enable_for_source_dq_validation": True,
        "enable_querydq_custom_output": True,
        "actual_value": 0,
        "expected_value": None,
        "description": "rule to check if row count is within upper and lower bounds",
    }



@pytest.fixture(name="_fixture_mock_context_without_detailed_stats")
def fixture_mock_context_without_detailed_stats():
    # fixture for mock context without_detailed_stats
    mock_object = Mock(spec=SparkExpectationsContext)
    mock_object.product_id = "product1"
    mock_object.spark = spark
    mock_object.get_row_dq_rule_type_name = "row_dq"
    mock_object.get_agg_dq_rule_type_name = "agg_dq"
    mock_object.get_query_dq_rule_type_name = "query_dq"
    mock_object.get_agg_dq_detailed_stats_status = False
    mock_object.get_query_dq_detailed_stats_status = False
    mock_object.get_supported_df_query_dq = spark.createDataFrame([{"spark_expectations_test": "se_query_dq"}])
    return mock_object


@pytest.fixture(name="_fixture_expectations")
def fixture_expectations():
    # Define the expectations for the data quality rules
    return {
        "row_dq_rules": [
            {
                "product_id": "product_1",
                "rule_type": "row_dq",
                "rule": "col1_gt_eq_1",
                "column_name": "col1",
                "expectation": "col1 >=1",
                "action_if_failed": "ignore",
                "table_name": "test_table",
                "tag": "validity",
                "description": "col1 gt or eq 1",
                "priority": "medium"
            },
            {
                "product_id": "product_1",
                "rule_type": "row_dq",
                "rule": "col1_gt_eq_2",
                "column_name": "col1",
                "expectation": "col1 >= 2",
                "action_if_failed": "drop",
                "table_name": "test_table",
                "tag": "accuracy",
                "description": "col1 gt or eq 2",
                "priority": "medium"
            },
            {
                "product_id": "product_1",
                "rule_type": "row_dq",
                "rule": "col1_gt_eq_3",
                "column_name": "col1",
                "expectation": "col1 >= 3",
                "action_if_failed": "fail",
                "table_name": "test_table",
                "tag": "completeness",
                "description": "col1 gt or eq 3",
                "priority": "medium"
            },
        ],
        "agg_dq_rules": [
            {
                "product_id": "product_1",
                "rule_type": "agg_dq",
                "rule": "col1_sum_gt_eq_6",
                "column_name": "col1",
                "expectation": "sum(col1)>=6",
                "action_if_failed": "ignore",
                "table_name": "test_table",
                "tag": "validity",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": True,
                "description": "col1 sum gt 1",
                "priority": "medium"
            },
            {
                "product_id": "product_1",
                "rule_type": "agg_dq",
                "rule": "col2_unique_value_gt_3",
                "column_name": "col2",
                "expectation": "count(distinct col2)>3",
                "action_if_failed": "fail",
                "table_name": "test_table",
                "tag": "accuracy",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": True,
                "description": "col2 unique value grater than 3",
                "priority": "medium"
            },
            {
                "product_id": "product_1",
                "rule_type": "agg_dq",
                "rule": "col1_sum_gt_6_and_lt_10",
                "column_name": "col1",
                "expectation": "sum(col1)>6 and sum(col1)<10",
                "action_if_failed": "fail",
                "table_name": "test_table",
                "tag": "accuracy",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": True,
                "description": "sum of col1 value grater than 6 and less than 10",
                "priority": "medium"
            },
        ],
        "query_dq_rules": [
            {
                "product_id": "product_1",
                "rule_type": "query_dq",
                "rule": "table_row_count_gt_1",
                "column_name": "col1",
                "expectation": "((select count(*) from query_test_table)-(select count(*) from query_test_table_target))>1",
                "enable_querydq_custom_output": True,
                "action_if_failed": "ignore",
                "table_name": "test_table",
                "tag": "validity",
                "enable_for_target_dq_validation": True,
                "enable_for_source_dq_validation": True,
                "description": "table count should be greater than 1",
                "expectation_source_f1": "select count(*) from query_test_table",
                "expectation_target_f1": "select count(*) from query_test_table_target",
                "priority": "medium"
            },
            {
                "product_id": "product_1",
                "rule_type": "query_dq",
                "rule": "table_distinct_count",
                "column_name": "col1",
                "expectation": "((select count(*) from (select distinct col1, col2 from query_test_table))-(select count(*) from (select distinct col1, col2 from query_test_table_target)))>3",
                "enable_querydq_custom_output": False,
                "action_if_failed": "fail",
                "table_name": "test_table",
                "tag": "accuracy",
                "enable_for_target_dq_validation": True,
                "enable_for_source_dq_validation": True,
                "description": "table distinct row count should be greater than 3",
                "expectation_source_f1": "select count(*) from (select distinct col1, col2 from query_test_table)",
                "expectation_target_f1": "select count(*) from (select distinct col1, col2 from query_test_table_target)",
                "priority": "medium"
            },
        ],
    }


@pytest.fixture(name="_fixture_agg_dq_detailed_expected_result")
def fixture_agg_dq_detailed_expected_result():
    # define the expected result for row dq operations
    return {
        "result": {
            "product_id": "product_1",
            "table_name": "test_table",
            "rule_type": "agg_dq",
            "rule": "col1_sum_gt_eq_6",
            "column_name": "col1",
            "expectation": "sum(col1)>=6",
            "tag": "validity",
            "status": "pass",
            "description": "col1 sum gt 1",
            "actual_value": 6,
            "expected_value": ">=6",
        },
        "result_query_dq": {
            "product_id": "product_1",
            "table_name": "test_table",
            "rule_type": "query_dq",
            "rule": "table_row_count_gt_1",
            "column_name": "col1",
            "expectation": "((select count(*) from query_test_table)-(select count(*) from query_test_table_target))>1",
            "tag": "validity",
            "status": "fail",
            "description": "table count should be greater than 1",
            "actual_value": 0,
            "expected_value": ">1",
        },
        "result_agg_query_dq_detailed_upper_lower_bound": {
            "product_id": "product1",
            "table_name": "test_table",
            "rule_type": "agg_dq",
            "rule": "expect_row_count_to_be_in_range",
            "column_name": "",
            "expectation": "count(*) > 1 and count(*) < 10",
            "tag": "accuracy",
            "status": "pass",
            "description": "rule to check if row count is within upper and lower bounds",
            "actual_value": 3,
            "expected_value": "3 > 1 and 3 < 10",
        },
        "result_without_context": {
            "product_id": "product_1",
            "table_name": "test_table",
            "rule_type": "agg_dq",
            "rule": "col1_sum_gt_eq_6",
            "column_name": "col1",
            "expectation": "sum(col1)>=6",
            "tag": "validity",
            "status": None,
            "description": "col1 sum gt 1",
            "actual_value": None,
            "expected_value": None,
        },
        "result_without_context1": {
            "product_id": "product_1",
            "table_name": "test_table",
            "rule_type": "agg_dq",
            "rule": "col1_sum_gt_6_and_lt_10",
            "column_name": "col1",
            "expectation": "sum(col1)>6 and sum(col1)<10",
            "tag": "validity",
            "status": "fail",
            "description": "sum of col1 is greater than 6 and sum of col1 is less than 10",
            "actual_value": 6,
            "expected_value": "6>6 and 6<10",
        },
    }


@pytest.fixture(name="_fixture_row_dq_expected_result")
def fixture_row_dq_expected_result():
    # define the expected result for row dq operations
    return {
        "result": [
            {
                "row_dq_col1_gt_eq_1": {
                    "action_if_failed": "ignore",
                    "description": "col1 gt or eq 1",
                    "rule": "col1_gt_eq_1",
                    "rule_type": "row_dq",
                    'column_name': 'col1',
                    "status": "pass",
                    "tag": "validity",
                    "priority": "medium"
                },
                "row_dq_col1_gt_eq_2": {
                    "rule_type": "row_dq",
                    "rule": "col1_gt_eq_2",
                    'column_name': 'col1',
                    "action_if_failed": "drop",
                    "status": "fail",
                    "tag": "accuracy",
                    "description": "col1 gt or eq 2",
                    "priority": "medium"
                },
                "row_dq_col1_gt_eq_3": {
                    "rule_type": "row_dq",
                    "rule": "col1_gt_eq_3",
                    'column_name': 'col1',
                    "action_if_failed": "fail",
                    "status": "fail",
                    "tag": "completeness",
                    "description": "col1 gt or eq 3",
                    "priority": "medium"
                },
            },
            {
                "row_dq_col1_gt_eq_1": {
                    "action_if_failed": "ignore",
                    "description": "col1 gt or eq 1",
                    'column_name': 'col1',
                    "rule": "col1_gt_eq_1",
                    "rule_type": "row_dq",
                    "status": "pass",
                    "tag": "validity",
                    "priority": "medium"
                },
                "row_dq_col1_gt_eq_2": {
                    "rule_type": "row_dq",
                    'column_name': 'col1',
                    "rule": "col1_gt_eq_2",
                    "action_if_failed": "drop",
                    "status": "pass",
                    "tag": "accuracy",
                    "description": "col1 gt or eq 2",
                    "priority": "medium"
                },
                "row_dq_col1_gt_eq_3": {
                    "rule_type": "row_dq",
                    'column_name': 'col1',
                    "rule": "col1_gt_eq_3",
                    "action_if_failed": "fail",
                    "status": "fail",
                    "tag": "completeness",
                    "description": "col1 gt or eq 3",
                    "priority": "medium"
                },
            },
            {
                "row_dq_col1_gt_eq_1": {
                    "action_if_failed": "ignore",
                    "description": "col1 gt or eq 1",
                    "rule": "col1_gt_eq_1",
                    "rule_type": "row_dq",
                    'column_name': 'col1',
                    "status": "pass",
                    "tag": "validity",
                    "priority": "medium"
                },
                "row_dq_col1_gt_eq_2": {
                    "rule_type": "row_dq",
                    "rule": "col1_gt_eq_2",
                    "action_if_failed": "drop",
                    "status": "pass",
                    'column_name': 'col1',
                    "tag": "accuracy",
                    "description": "col1 gt or eq 2",
                    "priority": "medium"
                },
                "row_dq_col1_gt_eq_3": {
                    "rule_type": "row_dq",
                    "rule": "col1_gt_eq_3",
                    'column_name': 'col1',
                    "action_if_failed": "fail",
                    "status": "pass",
                    "tag": "completeness",
                    "description": "col1 gt or eq 3",
                    "priority": "medium"
                },
            },
        ]
    }


@pytest.fixture(name="_fixture_agg_dq_expected_result")
def fixture_agg_dq_expected_result():
    # define the expected result for agg dq operations
    return {
        "result": [
            {
                "action_if_failed": "ignore",
                "description": "col1 sum gt 1",
                "rule": "col1_sum_gt_eq_6",
                "rule_type": "agg_dq",
                'column_name': 'col1',
                "status": "pass",
                "tag": "validity",
                "priority": "medium"
            },
            {
                "rule_type": "agg_dq",
                "rule": "col2_unique_value_gt_3",
                'column_name': 'col2',
                "action_if_failed": "fail",
                "status": "fail",
                "tag": "accuracy",
                "description": "col2 unique value grater than 3",
                "priority": "medium"
            },
            {
                "rule_type": "agg_dq",
                "rule": "col1_sum_gt_6_and_lt_10",
                'column_name': 'col1',
                "action_if_failed": "fail",
                "status": "fail",
                "tag": "accuracy",
                "description": "sum of col1 value grater than 6 and less than 10",
                "priority": "medium"
            },
        ]
    }


@pytest.fixture(name="_fixture_query_dq_expected_result")
def fixture_query_dq_expected_result():
    # define the expected result for agg dq operations
    return {
        "result": [
            {
                "rule": "table_row_count_gt_1",
                "description": "table count should be greater than 1",
                "rule_type": "query_dq",
                "column_name": "col1",
                "status": "fail",
                "tag": "validity",
                "action_if_failed": "ignore",
                "priority": "medium"
            },
            {
                "rule": "table_distinct_count",
                "description": "table distinct row count should be greater than 3",
                "rule_type": "query_dq",
                "column_name": "col1",
                "status": "fail",
                "tag": "accuracy",
                "action_if_failed": "fail",
                "priority": "medium"
            },
        ]
    }


@pytest.mark.parametrize(
    "rule, rule_type_name, source_dq_enabled, target_dq_enabled, expected",
    [
        (
            {"enable_for_source_dq_validation": True},
            "query_dq",
            True,
            False,
            True,
        ),
        (
            {"enable_for_target_dq_validation": False},
            "agg_dq",
            False,
            True,
            False,
        ),
        (
            {"enable_for_source_dq_validation": True},
            "query_dq",
            False,
            False,
            False,
        ),
    ],
)
def test_get_rule_is_active(
    rule, rule_type_name, source_dq_enabled, target_dq_enabled, expected, _fixture_mock_context
):
    assert (
        SparkExpectationsActions.get_rule_is_active(
            _fixture_mock_context, rule, rule_type_name, source_dq_enabled, target_dq_enabled
        )
        == expected
    )


@pytest.mark.parametrize(
    "_query_dq_rule, query_dq_detailed_expected_result, _source_dq_status,_target_dq_status",
    [
        # expectations rule 1
        (
            {
                "product_id": "product_1",
                "rule_type": "query_dq",
                "rule": "table_row_count_gt_1",
                "column_name": "col1",
                "expectation": "((select count(*) from query_test_table)-(select count(*) from query_test_table_target))>(select count(*) from query_test_table)",
                "enable_querydq_custom_output": True,
                "action_if_failed": "ignore",
                "table_name": "test_table",
                "tag": "validity",
                "enable_for_target_dq_validation": True,
                "enable_for_source_dq_validation": True,
                "description": "table count should be greater than 1",
                "expectation_source_f1": "select count(*) from query_test_table",
                "expectation_target_f1": "select count(*) from query_test_table_target",
            },
            # result in spark col object 1
            {
                "product_id": "product_1",
                "table_name": "test_table",
                "rule_type": "query_dq",
                "rule": "table_row_count_gt_1",
                "column_name": "col1",
                "expectation": "((select count(*) from query_test_table)-(select count(*) from query_test_table_target))>(select count(*) from query_test_table)",
                "tag": "validity",
                "status": "fail",
                "description": "table count should be greater than 1",
                "actual_value": 0,
                "expected_value": ">3",
            },
            True,
            False,
        ),
        # expectations rule 2
        (
            {
                "product_id": "product_1",
                "rule_type": "query_dq",
                "rule": "table_distinct_count",
                "column_name": "col1",
                "expectation": "((select count(*) from (select distinct col1, col2 from query_test_table))-(select count(*) from (select distinct col1, col2 from query_test_table_target)))>(select count(*) from (select distinct col1, col2 from query_test_table_target))",
                "enable_querydq_custom_output": False,
                "action_if_failed": "fail",
                "table_name": "test_table",
                "tag": "accuracy",
                "enable_for_target_dq_validation": True,
                "enable_for_source_dq_validation": True,
                "description": "table distinct row count should be greater than 3",
                "expectation_source_f1": "select count(*) from (select distinct col1, col2 from query_test_table)",
                "expectation_target_f1": "select count(*) from (select distinct col1, col2 from query_test_table_target)",
            },
            # result in spark col object 2
            {
                "product_id": "product_1",
                "table_name": "test_table",
                "rule_type": "query_dq",
                "rule": "table_distinct_count",
                "column_name": "col1",
                "expectation": "((select count(*) from (select distinct col1, col2 from query_test_table))-(select count(*) from (select distinct col1, col2 from query_test_table_target)))>(select count(*) from (select distinct col1, col2 from query_test_table_target))",
                "tag": "accuracy",
                "status": "fail",
                "description": "table distinct row count should be greater than 3",
                "actual_value": 0,
                "expected_value": ">3",
            },
            False,
            True,
        ),
         # expectations rule 3 - float retuned value
        (
            {
                "product_id": "product_1",
                "rule_type": "query_dq",
                "rule": "table_distinct_count_float",
                "column_name": "col1",
                "expectation": "(select count(col1) + 0.5 from query_test_table) > 2.78",
                "enable_querydq_custom_output": False,
                "action_if_failed": "fail",
                "table_name": "test_table",
                "tag": "accuracy",
                "enable_for_target_dq_validation": True,
                "enable_for_source_dq_validation": True,
                "description": "query results should be more than 2.78",
                "expectation_source_f1": "select count(col1) + 0.5 from query_test_table",
                "expectation_target_f1": "",
            },
            # result in spark col object 3
            {
                "product_id": "product_1",
                "table_name": "test_table",
                "rule_type": "query_dq",
                "rule": "table_distinct_count_float",
                "column_name": "col1",
                "expectation": "(select count(col1) + 0.5 from query_test_table) > 2.78",
                "tag": "accuracy",
                "status": "pass",
                "description": "query results should be more than 2.78",
                "actual_value": 3.5,
                "expected_value": ">2.78",
            },
            True,
            False,
        ),
         # expectations rule 4 - string retuned value
        (
            {
                "product_id": "product_1",
                "rule_type": "query_dq",
                "rule": "table_max_string",
                "column_name": "col2",
                "expectation": "(select max(col2) from query_test_table) > 'a'",
                "enable_querydq_custom_output": False,
                "action_if_failed": "fail",
                "table_name": "test_table",
                "tag": "accuracy",
                "enable_for_target_dq_validation": True,
                "enable_for_source_dq_validation": True,
                "description": "table max col2 bigger then 'a'",
                "expectation_source_f1": "select max(col2) from query_test_table",
                "expectation_target_f1": "select max(col2) from query_test_table_target",
            },
            # result in spark col object 4
            {
                "product_id": "product_1",
                "table_name": "test_table",
                "rule_type": "query_dq",
                "rule": "table_max_string",
                "column_name": "col2",
                "expectation": "(select max(col2) from query_test_table) > 'a'",
                "tag": "accuracy",
                "status": "pass",
                "description": "table max col2 bigger then 'a'",
                "actual_value": 'c',
                "expected_value": ">'a'",
            },
            True,
            False,
        ),
    ],
)
def test_agg_query_dq_detailed_result_with_querdq_v2(
    _fixture_df,
    _query_dq_rule,
    query_dq_detailed_expected_result,
    _fixture_mock_context,
    _source_dq_status,
    _target_dq_status,
):
    _fixture_df.createOrReplaceTempView("query_test_table")
    _fixture_df.createOrReplaceTempView("query_test_table_target")
    result_out, result_output = SparkExpectationsActions.agg_query_dq_detailed_result(
        _fixture_mock_context,
        _query_dq_rule,
        _fixture_df,
        [],
        _source_dq_status=_source_dq_status,
        _target_dq_status=_target_dq_status,
    )
    print("result_df:", result_output)
    print("query_dq_detailed_expected_result:", query_dq_detailed_expected_result)

    assert result_output[1] == query_dq_detailed_expected_result.get("product_id")
    assert result_output[2] == query_dq_detailed_expected_result.get("table_name")
    assert result_output[3] == query_dq_detailed_expected_result.get("rule_type")
    assert result_output[4] == query_dq_detailed_expected_result.get("rule")
    assert result_output[5] == query_dq_detailed_expected_result.get("column_name")
    assert result_output[6] == query_dq_detailed_expected_result.get("expectation")
    assert result_output[7] == query_dq_detailed_expected_result.get("tag")
    assert result_output[8] == query_dq_detailed_expected_result.get("description")
    assert result_output[9] == query_dq_detailed_expected_result.get("status")

    assert result_output[10] == query_dq_detailed_expected_result.get("actual_value")
    assert result_output[11] == query_dq_detailed_expected_result.get("expected_value")


@pytest.mark.parametrize(
    "_query_dq_rule_exception",
    [
        # expectations rule
        (
            {
                "product_id": "product_1",
                "rule_type": "query_dq",
                "rule": "table_row_count_gt_1",
                "column_name": "col1",
                "expectation": "(select count(*) from query_test_table)-(select count(*) from query_test_table_target))>(select count(*) from query_test_table)",
                "enable_querydq_custom_output": True,
                "action_if_failed": "ignore",
                "table_name": "test_table",
                "tag": "validity",
                "enable_for_target_dq_validation": True,
                "description": "table count should be greater than 1",
                "expectation_source_f1": "select count(*) from query_test_table",
                "expectation_target_f1": "select count(*) from query_test_table_target",
            }
        ),
        # expectations rule
        (
            {
                "product_id": "product_1",
                "rule_type": "query_dq",
                "rule": "table_distinct_count",
                "column_name": "col1",
                "expectation": "(select count(*) from (select distinct col1, col2 from query_test_table))-(select count(*) from (select distinct col1, col2 from query_test_table_target)))>(select count(*) from (select distinct col1, col2 from query_test_table_target))",
                "enable_querydq_custom_output": False,
                "action_if_failed": "fail",
                "table_name": "test_table",
                "tag": "accuracy",
                "enable_for_target_dq_validation": True,
                "enable_for_source_dq_validation": True,
                "description": "table distinct row count should be greater than 3",
                "expectation_source_f1": "select count(*) from (select distinct col1, col2 from query_test_table)",
                "expectation_target_f1": "select count(*) from (select distinct col1, col2 from query_test_table_target)",
            }
        ),
    ],
)
def test_agg_query_dq_detailed_result_exception_v2(_fixture_df, _query_dq_rule_exception, _fixture_mock_context):
    # faulty user input is given to test the exception functionality of the agg_query_dq_detailed_result
    _fixture_df.createOrReplaceTempView("query_test_table")
    _fixture_df.createOrReplaceTempView("query_test_table_target")
    with pytest.raises(
        SparkExpectationsMiscException,
        match=r"(error occurred while running agg_query_dq_detailed_result Sql query is invalid. *)|(error occurred while running agg_query_dq_detailed_result Regex match not found. *)",
    ):
        SparkExpectationsActions().agg_query_dq_detailed_result(
            _fixture_mock_context, _query_dq_rule_exception, _fixture_df, []
        )


def test_agg_query_dq_detailed_result(
    _fixture_df, _fixture_agg_dq_rule, _fixture_agg_dq_detailed_expected_result, _fixture_mock_context
):
    result_out, result_df = SparkExpectationsActions.agg_query_dq_detailed_result(
        _fixture_mock_context, _fixture_agg_dq_rule, _fixture_df, []
    )

    assert result_df[1] == _fixture_agg_dq_detailed_expected_result.get("result").get("product_id")
    assert result_df[2] == _fixture_agg_dq_detailed_expected_result.get("result").get("table_name")
    assert result_df[3] == _fixture_agg_dq_detailed_expected_result.get("result").get("rule_type")
    assert result_df[4] == _fixture_agg_dq_detailed_expected_result.get("result").get("rule")
    assert result_df[5] == _fixture_agg_dq_detailed_expected_result.get("result").get("column_name")
    assert result_df[6] == _fixture_agg_dq_detailed_expected_result.get("result").get("expectation")
    assert result_df[7] == _fixture_agg_dq_detailed_expected_result.get("result").get("tag")
    assert result_df[8] == _fixture_agg_dq_detailed_expected_result.get("result").get("description")
    assert result_df[9] == _fixture_agg_dq_detailed_expected_result.get("result").get("status")

    assert result_df[10] == _fixture_agg_dq_detailed_expected_result.get("result").get("actual_value")
    assert result_df[11] == _fixture_agg_dq_detailed_expected_result.get("result").get("expected_value")


def test_agg_query_dq_detailed_result_with_range_rule_type(
    _fixture_df, _fixture_agg_dq_rule_type_range, _fixture_agg_dq_detailed_expected_result, _fixture_mock_context
):
    result_out, result_df = SparkExpectationsActions.agg_query_dq_detailed_result(
        _fixture_mock_context, _fixture_agg_dq_rule_type_range, _fixture_df, []
    )

    assert result_df[1] == _fixture_agg_dq_detailed_expected_result.get("result_without_context1").get("product_id")
    assert result_df[2] == _fixture_agg_dq_detailed_expected_result.get("result_without_context1").get("table_name")
    assert result_df[3] == _fixture_agg_dq_detailed_expected_result.get("result_without_context1").get("rule_type")
    assert result_df[4] == _fixture_agg_dq_detailed_expected_result.get("result_without_context1").get("rule")
    assert result_df[5] == _fixture_agg_dq_detailed_expected_result.get("result_without_context1").get("column_name")
    assert result_df[6] == _fixture_agg_dq_detailed_expected_result.get("result_without_context1").get("expectation")
    assert result_df[7] == _fixture_agg_dq_detailed_expected_result.get("result_without_context1").get("tag")
    assert result_df[8] == _fixture_agg_dq_detailed_expected_result.get("result_without_context1").get("description")
    assert result_df[9] == _fixture_agg_dq_detailed_expected_result.get("result_without_context1").get("status")

    assert result_df[10] == _fixture_agg_dq_detailed_expected_result.get("result_without_context1").get("actual_value")
    assert result_df[11] == _fixture_agg_dq_detailed_expected_result.get("result_without_context1").get(
        "expected_value"
    )

def test_agg_query_dq_detailed_result_with_upper_lower_rule(
    _fixture_df, _fixture_agg_dq_rule_type_range_upper_lower, _fixture_agg_dq_detailed_expected_result, _fixture_mock_context
):
    result_out, result_df = SparkExpectationsActions.agg_query_dq_detailed_result(
        _fixture_mock_context, _fixture_agg_dq_rule_type_range_upper_lower, _fixture_df, []
    )

    assert result_df[1] == _fixture_agg_dq_detailed_expected_result.get("result_agg_query_dq_detailed_upper_lower_bound").get("product_id")
    assert result_df[2] == _fixture_agg_dq_detailed_expected_result.get("result_agg_query_dq_detailed_upper_lower_bound").get("table_name")
    assert result_df[3] == _fixture_agg_dq_detailed_expected_result.get("result_agg_query_dq_detailed_upper_lower_bound").get("rule_type")
    assert result_df[4] == _fixture_agg_dq_detailed_expected_result.get("result_agg_query_dq_detailed_upper_lower_bound").get("rule")
    assert result_df[5] == _fixture_agg_dq_detailed_expected_result.get("result_agg_query_dq_detailed_upper_lower_bound").get("column_name")
    assert result_df[6] == _fixture_agg_dq_detailed_expected_result.get("result_agg_query_dq_detailed_upper_lower_bound").get("expectation")
    assert result_df[7] == _fixture_agg_dq_detailed_expected_result.get("result_agg_query_dq_detailed_upper_lower_bound").get("tag")
    assert result_df[8] == _fixture_agg_dq_detailed_expected_result.get("result_agg_query_dq_detailed_upper_lower_bound").get("description")
    assert result_df[9] == _fixture_agg_dq_detailed_expected_result.get("result_agg_query_dq_detailed_upper_lower_bound").get("status")

    assert result_df[10] == _fixture_agg_dq_detailed_expected_result.get("result_agg_query_dq_detailed_upper_lower_bound").get("actual_value")
    assert result_df[11] == _fixture_agg_dq_detailed_expected_result.get("result_agg_query_dq_detailed_upper_lower_bound").get(
        "expected_value"
    )


def test_agg_query_dq_detailed_result_with_querdq(
    _fixture_df, _fixture_query_dq_rule, _fixture_agg_dq_detailed_expected_result, _fixture_mock_context
):
    _fixture_df.createOrReplaceTempView("query_test_table")
    _fixture_df.createOrReplaceTempView("query_test_table_target")
    result_out, result_df = SparkExpectationsActions.agg_query_dq_detailed_result(
        _fixture_mock_context, _fixture_query_dq_rule, _fixture_df, []
    )

    assert result_df[1] == _fixture_agg_dq_detailed_expected_result.get("result_query_dq").get("product_id")
    assert result_df[2] == _fixture_agg_dq_detailed_expected_result.get("result_query_dq").get("table_name")
    assert result_df[3] == _fixture_agg_dq_detailed_expected_result.get("result_query_dq").get("rule_type")
    assert result_df[4] == _fixture_agg_dq_detailed_expected_result.get("result_query_dq").get("rule")
    assert result_df[5] == _fixture_agg_dq_detailed_expected_result.get("result_query_dq").get("column_name")
    assert result_df[6] == _fixture_agg_dq_detailed_expected_result.get("result_query_dq").get("expectation")
    assert result_df[7] == _fixture_agg_dq_detailed_expected_result.get("result_query_dq").get("tag")
    assert result_df[8] == _fixture_agg_dq_detailed_expected_result.get("result_query_dq").get("description")
    assert result_df[9] == _fixture_agg_dq_detailed_expected_result.get("result_query_dq").get("status")

    assert result_df[10] == _fixture_agg_dq_detailed_expected_result.get("result_query_dq").get("actual_value")
    assert result_df[11] == _fixture_agg_dq_detailed_expected_result.get("result_query_dq").get("expected_value")


def test_agg_query_dq_detailed_result_without_detailed_context(
    _fixture_df,
    _fixture_agg_dq_rule,
    _fixture_agg_dq_detailed_expected_result,
    _fixture_mock_context_without_detailed_stats,
):
    result_out, result_df = SparkExpectationsActions.agg_query_dq_detailed_result(
        _fixture_mock_context_without_detailed_stats, _fixture_agg_dq_rule, _fixture_df, []
    )

    assert result_df[1] == _fixture_agg_dq_detailed_expected_result.get("result_without_context").get("product_id")
    assert result_df[2] == _fixture_agg_dq_detailed_expected_result.get("result_without_context").get("table_name")
    assert result_df[3] == _fixture_agg_dq_detailed_expected_result.get("result_without_context").get("rule_type")
    assert result_df[4] == _fixture_agg_dq_detailed_expected_result.get("result_without_context").get("rule")
    assert result_df[5] == _fixture_agg_dq_detailed_expected_result.get("result_without_context").get("column_name")
    assert result_df[6] == _fixture_agg_dq_detailed_expected_result.get("result_without_context").get("expectation")
    assert result_df[7] == _fixture_agg_dq_detailed_expected_result.get("result_without_context").get("tag")
    assert result_df[8] == _fixture_agg_dq_detailed_expected_result.get("result_without_context").get("description")
    assert result_df[9] == _fixture_agg_dq_detailed_expected_result.get("result_without_context").get("status")

    assert result_df[10] == _fixture_agg_dq_detailed_expected_result.get("result_without_context").get("actual_value")
    assert result_df[11] == _fixture_agg_dq_detailed_expected_result.get("result_without_context").get("expected_value")


def test_run_dq_rules_row(_fixture_df, _fixture_expectations, _fixture_row_dq_expected_result, _fixture_mock_context):
    # Apply the data quality rules
    result_df = SparkExpectationsActions.run_dq_rules(
        _fixture_mock_context, _fixture_df, _fixture_expectations, "row_dq"
    )

    # Assert that the result dataframe has the expected number of columns
    assert len(result_df.columns) == 6

    result_df.show(truncate=False)

    # Assert that the result dataframe has the expected values for each rule
    for row in result_df.collect():
        row_id = row["row_id"]
        assert row.row_dq_col1_gt_eq_1 == _fixture_row_dq_expected_result.get("result")[row_id].get(
            "row_dq_col1_gt_eq_1"
        )
        assert row.row_dq_col1_gt_eq_2 == _fixture_row_dq_expected_result.get("result")[row_id].get(
            "row_dq_col1_gt_eq_2"
        )
        assert row.row_dq_col1_gt_eq_3 == _fixture_row_dq_expected_result.get("result")[row_id].get(
            "row_dq_col1_gt_eq_3"
        )


@pytest.mark.parametrize(
    "agg_dq_source_dq_status,agg_dq_target_dq_status",
    [
        (True, False),
        (False, True),
    ],
)
def test_run_dq_rules_agg(
    _fixture_df,
    _fixture_expectations,
    _fixture_agg_dq_expected_result,
    _fixture_mock_context,
    agg_dq_source_dq_status,
    agg_dq_target_dq_status,
):
    # Apply the data quality rules

    result_df = SparkExpectationsActions.run_dq_rules(
        _fixture_mock_context,
        _fixture_df,
        _fixture_expectations,
        "agg_dq",
        agg_dq_source_dq_status,
        agg_dq_target_dq_status,
    )

    # Assert that the result dataframe has the expected number of columns
    assert len(result_df.columns) == 1

    result_df.show(truncate=False)

    # Assert that the result dataframe has the expected values for each rule
    row = result_df.collect()[0]
    assert row.meta_agg_dq_results == _fixture_agg_dq_expected_result.get("result")


@pytest.mark.parametrize(
    "query_dq_source_dq_status,query_dq_target_dq_status",
    [
        (True, False),
        (False, True),
    ],
)
def test_run_dq_rules_query(
    _fixture_df,
    _fixture_expectations,
    _fixture_query_dq_expected_result,
    _fixture_mock_context,
    query_dq_source_dq_status,
    query_dq_target_dq_status,
):
    # Apply the data quality rules
    _fixture_df.createOrReplaceTempView("query_test_table")
    _fixture_df.createOrReplaceTempView("query_test_table_target")

    result_df = SparkExpectationsActions.run_dq_rules(
        _fixture_mock_context,
        _fixture_df,
        _fixture_expectations,
        "query_dq",
        query_dq_source_dq_status,
        query_dq_target_dq_status,
    )

    # Assert that the result dataframe has the expected number of columns
    assert len(result_df.columns) == 1

    result_df.show(truncate=False)

    # Assert that the result dataframe has the expected values for each rule
    row = result_df.collect()[0]
    assert row.meta_query_dq_results == _fixture_query_dq_expected_result.get("result")


def test_run_dq_rules_negative_case(_fixture_df, _fixture_mock_context):
    expectations = {"row_dq_rules": []}

    with pytest.raises(SparkExpectationsMiscException, match=r"error occurred while running expectations .*"):
        SparkExpectationsActions.run_dq_rules(_fixture_mock_context, _fixture_df, expectations, "row_dq")

@pytest.mark.parametrize(
    "expectations, expected_exception",
    [
        ({"rules": [{}]}, SparkExpectationsMiscException),
        ({}, SparkExpectationsMiscException),
        (
            {
                "row_dq_rules": [
                    {
                        "rule": "col1_gt_eq_1",
                        "expectation": "col1 equals or greater than 1",
                        "table_name": "test_table",
                        "tag": "col1 gt or eq 1",
                    }
                ]
            },
            SparkExpectationsMiscException,
        ),
        ({"row_dq_rules": [{}]}, SparkExpectationsMiscException),
    ],
)
def test_run_dq_rules_exception(_fixture_df, _fixture_mock_context, expectations, expected_exception):
    # test the exception functionality in run_dq_rules with faulty user input
    with pytest.raises(expected_exception, match=r"error occurred while running expectations .*"):
        SparkExpectationsActions.run_dq_rules(_fixture_mock_context, _fixture_df, expectations, "row_dq")



def test_run_dq_rules_condition_expression_exception(
    _fixture_df, _fixture_query_dq_expected_result, _fixture_mock_context
):
    # Apply the data quality rules
    _expectations = {
        "query_dq_rules": [
            {
                "rule_type": "query_dq",
                "rule": "table_row_count_gt_1",
                "column_name": "col1",
                "expectation": "(select count(*) from query_test_table)>1",
                "action_if_failed": "ignore",
                "table_name": "test_table",
                "tag": "validity",
                "enable_for_target_dq_validation": False,
                "description": "table count should be greater than 1",
            },
        ],
    }
    _fixture_df.createOrReplaceTempView("query_test_table")

    with pytest.raises(SparkExpectationsMiscException, match=r"error occurred while running expectations .*"):
        SparkExpectationsActions.run_dq_rules(
            _fixture_mock_context, _fixture_df, _expectations, "query_dq", False, True
        )


@pytest.mark.parametrize(
    "_rule_test",
    [
        ({"rule_type": "query"}),
    ],
)
def test_run_dq_rules_condition_expression_dynamic_exception(
    _fixture_df, _fixture_query_dq_expected_result, _fixture_mock_context, _rule_test
):
    # Apply the data quality rules
    _expectations = {
        "query_rules": [
            {
                "rule_type": "query",
                "rule": "table_row_count_gt_1",
                "expectation": "(select count(*) from query_test_table)>1",
                "column_name": "*",
                "action_if_failed": "ignore",
                "table_name": "test_table",
                "tag": "validity",
                "enable_for_target_dq_validation": False,
                "description": "table count should be greater than 1",
            },
        ],
    }
    _fixture_df.createOrReplaceTempView("query_test_table")

    with pytest.raises(SparkExpectationsMiscException, match=r"error occurred while running expectations .*"):
        _rule_type = _rule_test.get("rule_type")
        SparkExpectationsActions.run_dq_rules(
            _fixture_mock_context, _fixture_df, _expectations, _rule_type, False, True
        )


def test_agg_query_dq_detailed_result_type_error(_fixture_agg_dq_rule, _fixture_mock_context):
    # Patch DataFrame.agg to return a value of an unexpected type (e.g., list)
    class DummyDF:
        def agg(self, *args, **kwargs):
            class DummyRow:
                def collect(self):
                    return [[["unexpected", "list"]]]  # Not int, float, str, or date
            return DummyRow()
    dummy_df = DummyDF()
    with pytest.raises(SparkExpectationsMiscException, match="error occurred while running agg_query_dq_detailed_result .*"):
        SparkExpectationsActions.agg_query_dq_detailed_result(
            _fixture_mock_context, _fixture_agg_dq_rule, dummy_df, []
        )

@pytest.mark.parametrize(
    "input_df, rule_type_name, expected_output",
    # input_df
    [
        (
            spark.createDataFrame(
                [
                    {
                        "meta_agg_dq_results": [
                            {"action_if_failed": "ignore", "description": "desc1"},
                            {"action_if_failed": "ignore", "description": "desc2"},
                            {"action_if_failed": "ignore", "description": "desc3"},
                        ]
                    }
                ]
            ),
            "agg_dq",  # rule_type_name
            # expected_output
            [
                {"action_if_failed": "ignore", "description": "desc1"},
                {"action_if_failed": "ignore", "description": "desc2"},
                {"action_if_failed": "ignore", "description": "desc3"},
            ],
        ),  # input_df
        # input_df
        (
            spark.createDataFrame(
                [
                    {
                        "meta_query_dq_results": [
                            {"action_if_failed": "ignore", "description": "desc1"},
                        ]
                    }
                ]
            ),
            # expected_output
            "query_dq",  # rule_type_name
            [{"action_if_failed": "ignore", "description": "desc1"}],
        ),
        (
            spark.createDataFrame(
                [
                    {
                        "meta_query_dq_results": [
                            {"action_if_failed": "ignore", "description": "desc1"},
                        ]
                    }
                ]
            ),
            # expected_output
            "row_dq",  # rule_type_name
            None,
        ),
    ],
)
def test_create_agg_dq_results(input_df, rule_type_name, expected_output, _fixture_mock_context):
    # unit test case on create_agg_dq_results
    assert (
        SparkExpectationsActions().create_agg_dq_results(
            _fixture_mock_context,
            input_df,
            rule_type_name,
        )
        == expected_output
    )


@pytest.mark.parametrize(
    "input_df",
    [
        (spark.createDataFrame([{"agg_dq_results": ""}]),),
        (
            None,
            "agg_dq",  # rule_type_name
            # expected_output
            None,
        ),
    ],
)
def test_create_agg_dq_results_exception(input_df, _fixture_mock_context):
    # faulty user input is given to test the exception functionality of the agg_dq_result
    with pytest.raises(SparkExpectationsMiscException, match=r"error occurred while running create agg dq results .*"):
        SparkExpectationsActions().create_agg_dq_results(
            _fixture_mock_context,
            input_df,
            "<test>",
        )


def test_create_agg_dq_results_streaming_skip(_fixture_mock_context):
    """Test line 449-454: streaming DataFrame skips aggregation results collection"""
    streaming_df = spark.readStream.format("rate").option("rowsPerSecond", "1").load()
    assert SparkExpectationsActions().create_agg_dq_results(_fixture_mock_context, streaming_df, "agg_dq") is None

@pytest.mark.parametrize(
    "input_df, table_name, input_count, error_count, output_count, "
    "rule_type, row_dq_flag, source_agg_flag, final_agg_flag, source_query_dq_flag, final_query_dq_flag, expected_output",
    [
        (
            # test case 1
            # In this test case, action_if_failed consist "ignore"
            # function should return dataframe with all the rows
            spark.createDataFrame(
                [  # log into err & final
                    {
                        "col1": 1,
                        "col2": "a",
                        "meta_row_dq_results": [
                            {"action_if_failed": "ignore"},
                            {"action_if_failed": "ignore"},
                            {"action_if_failed": "ignore"},
                        ],
                    },
                    # log into err & final
                    {"col1": 2, "col2": "b", "meta_row_dq_results": [{"action_if_failed": "ignore"}]},
                    # log into err & final
                    {"col1": 3, "col2": "c", "meta_row_dq_results": [{"action_if_failed": "ignore"}]},
                ]
            ),
            "test_dq_stats_table",  # table name
            3,  # input count
            3,  # error count
            3,  # output count
            "row_dq",  # rule type
            True,  # row_dq_flag
            False,  # source_agg_dq_flag
            False,  # final_dg_flag
            False,  # source_query_dq_flag
            False,  # final_query_dq_flag
            # expected dataframe
            spark.createDataFrame([{"col1": 1, "col2": "a"}, {"col1": 2, "col2": "b"}, {"col1": 3, "col2": "c"}]),
        ),
        (
            # In this test case, action_if_failed consist "drop"
            # function should return dataframe with all the rows
            spark.createDataFrame(
                [  # drop & log into error
                    {
                        "col1": 1,
                        "col2": "a",
                        "meta_row_dq_results": [
                            {"action_if_failed": "drop", "status": "fail"},
                            {"action_if_failed": "drop", "status": "fail"},
                            {"action_if_failed": "drop", "status": "fail"},
                        ],
                    },
                    # drop & log into error
                    {"col1": 2, "col2": "b", "meta_row_dq_results": [{"action_if_failed": "drop", "status": "fail"}]},
                    # log into final
                    {"col1": 3, "col2": "c", "meta_row_dq_results": []},
                ]
            ),
            "test_dq_stats_table",  # table name
            3,  # input count
            2,  # error count
            1,  # output count
            "row_dq",  # rule type
            True,  # row_dq_flag
            False,  # source_agg_dq_flag
            False,  # final_agg_dq_flag
            False,  # source_query_dq_flag
            False,  # final_query_dq_flag
            # expected df
            spark.createDataFrame([{"col1": 3, "col2": "c"}]),
        ),
        (
            # test case 3
            # In this test case, action_if_failed consist "fail"
            # spark expectations expected to fail
            spark.createDataFrame(
                [  # log into error and raise error
                    {
                        "col1": 1,
                        "col2": "a",
                        "meta_row_dq_results": [
                            {"action_if_failed": "fail", "status": "fail"},
                            {"action_if_failed": "fail", "status": "fail"},
                            {"action_if_failed": "fail", "status": "fail"},
                        ],
                    },
                    {"col1": 2, "col2": "b", "meta_row_dq_results": []},
                    {"col1": 3, "col2": "c", "meta_row_dq_results": []},
                ]
            ),
            "test_dq_stats_table",  # table name
            3,  # input count
            1,  # error count
            0,  # output count
            "row_dq",  # rule tye
            True,  # row_dq_flag
            False,  # source_agg_dq_flag
            False,  # final_agg_dq_flag
            False,  # source_query_dq_flag
            False,  # final_query_dq_flag
            SparkExpectationsMiscException,  # expected res
        ),
        (
            # test case 4
            # In this test case, action_if_failed consist "ignore" & "drop"
            # function should return dataframe with 2 the rows
            spark.createDataFrame(
                [
                    # drop, log into error
                    {
                        "col1": 1,
                        "col2": "a",
                        "meta_row_dq_results": [
                            {"action_if_failed": "ignore", "status": "fail"},
                            {"action_if_failed": "ignore", "status": "fail"},
                            {"action_if_failed": "drop", "status": "fail"},
                        ],
                    },
                    # log into error and final
                    {"col1": 2, "col2": "b", "meta_row_dq_results": [{"action_if_failed": "ignore", "status": "fail"}]},
                    # log into error and final
                    {"col1": 2, "col2": "c", "meta_row_dq_results": [{"action_if_failed": "ignore", "status": "fail"}]},
                ]
            ),
            "test_dq_stats_table",  # table name
            3,  # input count
            3,  # error count
            2,  # output count
            "row_dq",  # rule type
            True,  # row_dq_flag
            False,  # source_agg_dq_flag
            False,  # final_agg_dq_flag
            False,  # source_query_dq_flag
            False,  # final_query_dq_flag
            # expected df
            spark.createDataFrame([{"col1": 2, "col2": "b"}, {"col1": 2, "col2": "c"}]),
        ),
        (
            # test case 5
            # In this test case, action_if_failed consist "ignore" & "fail"
            # spark expectations expected to fail
            spark.createDataFrame(
                [
                    # log into to error & fail program
                    {
                        "col1": 1,
                        "col2": "a",
                        "meta_row_dq_results": [
                            {"action_if_failed": "ignore", "status": "fail"},
                            {"action_if_failed": "ignore", "status": "fail"},
                            {"action_if_failed": "fail", "status": "fail"},
                        ],
                    },
                    # log into error
                    {"col1": 2, "col2": "b", "meta_row_dq_results": [{"action_if_failed": "ignore", "status": "fail"}]},
                    {"col1": 3, "col2": "c", "meta_row_dq_results": []},
                ]
            ),
            "test_dq_stats_table",  # table name
            3,  # input count
            2,  # error count
            0,  # output count
            "row_dq",  # rule type
            True,  # row_dq_flag
            False,  # source_agg_dq_flag
            False,  # final_agg_dq_flag
            False,  # source_query_dq_flag
            False,  # final_query_dq_flag
            SparkExpectationsMiscException,  # expected res
        ),
        # test case 6
        # In this test case, action_if_failed consist "drop" & "fail"
        # spark expectations expected to fail
        (
            spark.createDataFrame(
                [
                    # log into error & fail
                    {
                        "col1": 1,
                        "col2": "a",
                        "meta_row_dq_results": [
                            {"action_if_failed": "drop", "status": "fail"},
                            {"action_if_failed": "drop", "status": "fail"},
                            {"action_if_failed": "fail", "status": "fail"},
                        ],
                    },
                    # log into error & drop
                    {"col1": 2, "col2": "b", "meta_row_dq_results": [{"action_if_failed": "drop", "status": "fail"}]},
                    {"col1": 3, "col2": "c", "meta_row_dq_results": []},
                ]
            ),
            "test_dq_stats_table",  # table name
            3,  # input count
            2,  # error count
            0,  # output count
            "row_dq",  # rule type
            True,  # row_dq_flag
            False,  # source_agg_dq_flag
            False,  # final_agg_dq_flag
            False,  # source_query_dq_flag
            False,  # final_query_dq_flag
            SparkExpectationsMiscException,  # expected res
        ),
        (
            # test case 7
            # In this test case, action_if_failed consist "ignore" & "drop"
            # function returns dataframe with 2 rows
            spark.createDataFrame(
                [
                    # drop and log into error
                    {
                        "col1": 1,
                        "col2": "a",
                        "meta_row_dq_results": [
                            {"action_if_failed": "drop", "status": "fail"},
                            {"action_if_failed": "ignore", "status": "fail"},
                            {"action_if_failed": "ignore", "status": "fail"},
                        ],
                    },
                    # log into final
                    {"col1": 2, "col2": "b", "meta_row_dq_results": [{"action_if_failed": "test", "status": "fail"}]},
                    # log into final
                    {"col1": 3, "col2": "c", "meta_row_dq_results": []},
                ]
            ),
            "test_dq_stats_table",  # table name
            3,  # input count
            1,  # error count
            2,  # output count
            "row_dq",  # rule type
            True,  # row_dq_flag
            False,  # source_agg_dq_flag
            False,  # final_agg_dq_flag
            False,  # source_query_dq_flag
            False,  # final_query_dq_flag
            # expected df
            spark.createDataFrame([{"col1": 2, "col2": "b"}, {"col1": 3, "col2": "c"}]),
        ),
        # (
        #             # test case 8
        #             spark.createDataFrame(
        #                 [
        #                     {"col1": 1, "col2": "a"},
        #                     {"col1": 2, "col2": "b"},
        #                     {"col1": 3, "col2": "c"},
        #
        #                 ]
        #             ).withColumn("dq_rule_col_gt_eq_1", create_map()),
        #             'test_dq_stats_table',
        #             3,
        #             0,
        #             spark.createDataFrame([
        #                 {"col1": 1, "col2": "a"},
        #                 {"col1": 2, "col2": "b"},
        #                 {"col1": 3, "col2": "c"}
        #             ])
        #     )
        (
            # test case 9
            # In this test case, action_if_failed consist "ignore"  for agg dq on source_agg_dq
            # function returns empty dataframe
            spark.createDataFrame(
                [
                    {"meta_agg_dq_results": [{"action_if_failed": "ignore", "status": "fail"}]},
                ]
            ),
            "test_dq_stats_table",  # table name
            100,  # input count
            30,  # output count
            0,  # error count
            "agg_dq",  # rule type
            False,  # row_dq_flag
            True,  # source_agg_dq_flag
            False,  # final_agg_dq_flag
            False,  # source_query_dq_flag
            False,  # final_query_dq_flag
            # expected df
            spark.createDataFrame([{"meta_agg_dq_results": [{"action_if_failed": "ignore", "status": "fail"}]}]).drop(
                "meta_agg_dq_results"
            ),
        ),
        (
            # test case 10
            # In this test case, action_if_failed consist "fail" for agg dq on source_agg_dq
            # function returns empty dataframe
            spark.createDataFrame(
                [
                    {"meta_agg_dq_results": [{"action_if_failed": "fail", "status": "fail"}]},
                ]
            ),
            "test_dq_stats_table",  # table name
            100,  # input count
            10,  # output count
            0,  # error count
            "agg_dq",  # rule type
            False,  # row_dq_flag
            True,  # source_agg_dq_flag
            False,  # final_agg_dq_flag
            False,  # source_query_dq_flag
            False,  # final_query_dq_flag
            SparkExpectationsMiscException,  # expected res
        ),
        (
            # test case 11
            # In this test case, action_if_failed consist "ignore" for agg dq on final_agg_dq
            # function returns empty dataframe
            spark.createDataFrame(
                [
                    {"meta_agg_dq_results": [{"action_if_failed": "ignore", "status": "fail"}]},
                ]
            ),
            "test_dq_stats_table",  # table name
            100,  # input count
            20,  # error count
            0,  # output count
            "agg_dq",  # rule type
            False,  # row_dq_flag
            False,  # source_agg_flag
            True,  # final_agg_flag
            False,  # source_query_dq_flag
            False,  # final_query_dq_flag
            # expected df
            spark.createDataFrame(
                [
                    {"meta_agg_dq_results": [{"action_if_failed": "ignore", "status": "fail"}]},
                ]
            ).drop("meta_agg_dq_results"),
        ),
        (
            # test case 12
            # In this test case, action_if_failed consist "fail" for agg dq on final_agg_aq
            # spark expectations set to fail for agg_dq on final_agg-dq
            spark.createDataFrame(
                [
                    {"meta_agg_dq_results": [{"action_if_failed": "fail", "status": "fail"}]},
                ]
            ),
            "test_dq_stats_table",  # table name
            100,  # input count
            30,  # error count
            0,  # output count
            "agg_dq",  # rule type
            False,  # row_dq_flag
            False,  # source_agg_flag
            True,  # final_agg_flag
            False,  # source_query_dq_flag
            False,  # final_query_dq_flag
            SparkExpectationsMiscException,  # expected res
        ),
        (
            # test case 13
            # In this test case, action_if_failed consist "fail" for agg dq on final_agg_dq
            # spark expectations set to fail for agg_dq on final_agg_dq
            spark.createDataFrame(
                [
                    {"meta_agg_dq_results": [{"action_if_failed": "fail", "status": "fail"}]},
                ]
            ),
            "test_dq_stats_table",  # table name
            100,  # input count
            0,  # error count
            0,  # output count
            "agg_dq",  # rule type
            False,  # row_dq_flag
            False,  # source_agg_flag
            True,  # final_agg_flag
            False,  # source_query_dq_flag
            False,  # final_query_dq_flag
            SparkExpectationsMiscException,  # expected res
        ),
        (
            # test case 14
            # In this test case, action_if_failed consist "ignore", "fail"  for agg dq on final_agg_dq
            # spark expectations set to fail for agg_dq on final_agg_dq
            spark.createDataFrame(
                [
                    {
                        "meta_agg_dq_results": [
                            {"action_if_failed": "ignore", "status": "fail"},
                            {"action_if_failed": "fail", "status": "fail"},
                        ]
                    },
                ]
            ),
            "test_dq_stats_table",  # table name
            100,  # input count
            0,  # error count
            0,  # output count
            "agg_dq",  # rule type
            False,  # row_dq_flag
            False,  # source_agg_flag
            True,  # final_agg_flag
            False,  # source_query_dq_flag
            False,  # final_query_dq_flag
            SparkExpectationsMiscException,  # expected res
        ),
        (
            # test case 15
            # In this test case, action_if_failed consist "ignore" for agg dq on source_agg_dq
            # function returns a empty datatset
            spark.createDataFrame(
                [
                    {
                        "meta_agg_dq_results": [
                            {"action_if_failed": "ignore", "status": "fail"},
                            {"action_if_failed": "ignore", "status": "fail"},
                        ]
                    },
                ]
            ),
            "test_dq_stats_table",  # table name
            100,  # input count
            0,  # error count
            0,  # output count
            "agg_dq",  # rule type
            False,  # row_dq_flag
            True,  # source_agg_flag
            False,  # final_agg_flag
            False,  # source_query_dq_flag
            False,  # final_query_dq_flag
            spark.createDataFrame(
                [
                    {"meta_agg_dq_results": [{"action_if_failed": "ignore", "status": "fail"}]},
                ]
            ).drop("meta_agg_dq_results"),  # expected df
        ),
        (
            # test case 16
            # In this test case, action_if_failed consist "ignore" for agg dq on final_agg_dq
            # function returns a empty datatset
            spark.createDataFrame(
                [
                    {
                        "meta_agg_dq_results": [
                            {"action_if_failed": "ignore", "status": "fail"},
                            {"action_if_failed": "ignore", "status": "fail"},
                            {"action_if_failed": "ignore", "status": "fail"},
                        ]
                    },
                ]
            ),
            "test_dq_stats_table",  # table name
            100,  # input count
            15,  # error count
            0,  # output count
            "agg_dq",  # rule type
            False,  # row_dq_flag
            False,  # source_agg_flag
            True,  # final_agg_flag
            False,  # source_query_dq_flag
            False,  # final_query_dq_flag
            spark.createDataFrame(
                [
                    {"meta_agg_dq_results": [{"action_if_failed": "ignore", "status": "fail"}]},
                ]
            ).drop("meta_agg_dq_results"),  # expected df
        ),
        (
            # test case 17
            # In this test case, action_if_failed consist "ignore" for query dq on source_query_dq
            # function returns a empty datatset
            spark.createDataFrame(
                [
                    {
                        "meta_query_dq_results": [
                            {"action_if_failed": "ignore", "status": "fail"},
                            {"action_if_failed": "ignore", "status": "fail"},
                            {"action_if_failed": "ignore", "status": "fail"},
                        ]
                    },
                ]
            ),
            "test_dq_stats_table",  # table name
            100,  # input count
            0,  # error count
            0,  # output count
            "query_dq",  # rule type
            False,  # row_dq_flag
            False,  # source_agg_flag
            True,  # final_agg_flag
            True,  # source_query_dq_flag
            False,  # final_query_dq_flag
            spark.createDataFrame(
                [
                    {"meta_query_dq_results": [{"action_if_failed": "ignore", "status": "fail"}]},
                ]
            ).drop("meta_query_dq_results"),  # expected df
        ),
        (
            # test case 18
            # In this test case, action_if_failed consist "ignore" & "fail" for query dq on source_query_dq
            # function set to fail with exceptions
            spark.createDataFrame(
                [
                    {
                        "meta_query_dq_results": [
                            {"action_if_failed": "ignore", "status": "fail"},
                            {"action_if_failed": "fail", "status": "fail"},
                            {"action_if_failed": "ignore", "status": "fail"},
                            {"action_if_failed": "ignore", "status": "fail"},
                            {"action_if_failed": "ignore", "status": "fail"},
                        ]
                    },
                ]
            ),
            "test_dq_stats_table",  # table name
            100,  # input count
            0,  # error count
            0,  # output count
            "query_dq",  # rule type
            False,  # row_dq_flag
            False,  # source_agg_flag
            False,  # final_agg_flag
            True,  # source_query_dq_flag
            False,  # final_query_dq_flag
            SparkExpectationsMiscException,  # expected df
        ),
        (
            # test case 19
            # In this test case, action_if_failed consist "ignore" & "fail" for query dq on final_query_dq
            # function set to fail with exceptions
            spark.createDataFrame(
                [
                    {
                        "meta_query_dq_results": [
                            {"action_if_failed": "fail", "status": "fail"},
                        ]
                    },
                ]
            ),
            "test_dq_stats_table",  # table name
            100,  # input count
            25,  # error count
            0,  # output count
            "query_dq",  # rule type
            False,  # row_dq_flag
            False,  # source_agg_flag
            False,  # final_agg_flag
            False,  # source_query_dq_flag
            True,  # final_query_dq_flag
            SparkExpectationsMiscException,  # expected df
        ),
        (
            # test case 20
            # In this test case, action_if_failed consist "ignore" for query dq on final_query_dq
            # function returns empty dataset
            spark.createDataFrame(
                [
                    {
                        "meta_query_dq_results": [
                            {"action_if_failed": "ignore", "status": "fail"},
                        ]
                    },
                ]
            ),
            "test_dq_stats_table",  # table name
            100,  # input count
            10,  # error count
            0,  # output count
            "query_dq",  # rule type
            False,  # row_dq_flag
            False,  # source_agg_flag
            False,  # final_agg_flag
            False,  # source_query_dq_flag
            True,  # final_query_dq_flag
            spark.createDataFrame(
                [
                    {"meta_query_dq_results": [{"action_if_failed": "ignore", "status": "fail"}]},
                ]
            ).drop("meta_query_dq_results"),  # expected df
        ),
        (
            # test case 20
            # In this test case, action_if_failed consist "ignore" for query dq on final_query_dq
            # function returns empty dataset
            spark.createDataFrame(
                [
                    {
                        "meta_query_dq_results": [
                            {"action_if_failed": "ignore", "status": "fail"},
                            {"action_if_failed": "ignore", "status": "fail"},
                            {"action_if_failed": "ignore", "status": "fail"},
                            {"action_if_failed": "ignore", "status": "fail"},
                            {"action_if_failed": "ignore", "status": "fail"},
                            {"action_if_failed": "ignore", "status": "fail"},
                        ]
                    },
                ]
            ),
            "test_dq_stats_table",  # table name
            100,  # input count
            10,  # error count
            0,  # output count
            "query_dq",  # rule type
            False,  # row_dq_flag
            False,  # source_agg_flag
            False,  # final_agg_flag
            False,  # source_query_dq_flag
            True,  # final_query_dq_flag
            spark.createDataFrame(
                [
                    {"meta_query_dq_results": [{"action_if_failed": "ignore", "status": "fail"}]},
                ]
            ).drop("meta_query_dq_results"),  # expected df
        ),
    ],
)
@patch(
    "spark_expectations.utils.actions.SparkExpectationsContext.set_final_agg_dq_status", autospec=True, spec_set=True
)
@patch(
    "spark_expectations.utils.actions.SparkExpectationsContext.set_source_agg_dq_status", autospec=True, spec_set=True
)
@patch("spark_expectations.utils.actions.SparkExpectationsContext.set_row_dq_status", autospec=True, spec_set=True)
def test_action_on_dq_rules(
    _mock_set_row_dq_status,
    _mock_set_source_agg_dq_status,
    _mock_set_final_agg_dq_status,
    input_df,
    table_name,
    input_count,
    error_count,
    output_count,
    rule_type,
    row_dq_flag,
    source_agg_flag,
    final_agg_flag,
    source_query_dq_flag,
    final_query_dq_flag,
    expected_output,
    _fixture_mock_context,
):
    input_df.show(truncate=False)

    # assert for exception when spark expectations set to fail
    if isinstance(expected_output, type) and issubclass(expected_output, Exception):
        with pytest.raises(expected_output, match=r"error occurred while taking action on given rules .*"):
            SparkExpectationsActions.action_on_rules(
                _fixture_mock_context,
                input_df,
                input_count,
                error_count,
                output_count,
                rule_type,
                row_dq_flag,
                source_agg_flag,
                final_agg_flag,
                source_query_dq_flag,
                final_query_dq_flag,
            )

    else:
        # assert when all condition passes without action_if_failed "fail"
        df = SparkExpectationsActions.action_on_rules(
            _fixture_mock_context,
            input_df,
            input_count,
            error_count,
            output_count,
            rule_type,
            row_dq_flag,
            source_agg_flag,
            final_agg_flag,
        )
        if row_dq_flag is True:
            # assert for row dq expectations
            assert df.orderBy("col2").collect() == expected_output.orderBy("col2").collect()
        else:
            # assert for agg dq expectations
            assert df.collect() == expected_output.collect()




@pytest.mark.parametrize(
    "input_df, table_name, input_count, error_count, output_count, rule_type, row_dq_flag",
    [
        (  # test case 1
            spark.createDataFrame(
                [
                    {
                        "col1": 1,
                        "col2": "a",
                        "row_dq_results": [
                            {"action_if_failed": "fail"},
                            {"action_if_failed": "ignore"},
                            {"action_if_failed": "ignore"},
                        ],
                    },
                    {"col1": 2, "col2": "b", "row_dq_results": [{"action_if_failed": "test"}]},
                    {"col1": 3, "col2": "c", "row_dq_results": []},
                ]
            ),
            "test_dq_stats",
            3,
            2,
            0,
            "row_dq",
            True,
        ),
        # test case 2
        (spark.createDataFrame([(1, "a")], ["dq_rule_1", "dq_rule_2"]), "test_dq_stats", 3, 2, 0, "row_dq", True),
    ],
)
def test_action_on_rules_exception(
    input_df, table_name, input_count, error_count, output_count, rule_type, row_dq_flag, _fixture_mock_context
):
    # test exception functionality with faulty user input
    with pytest.raises(SparkExpectationsMiscException, match=r"error occurred while taking action on given rules .*"):
        SparkExpectationsActions.action_on_rules(
            _fixture_mock_context, input_df, table_name, input_count, error_count, output_count, rule_type, row_dq_flag
        )


def test_action_on_rules_streaming_skip(_fixture_mock_context):
    """Test line 663: streaming DataFrame handles streaming mode safely"""
    streaming_df = spark.readStream.format("rate").option("rowsPerSecond", "1").load()
    streaming_df = streaming_df.withColumn(
        "meta_row_dq_results", array(create_map(lit("status"), lit("pass"), lit("action_if_failed"), lit("ignore")))
    ).withColumn("col1", lit(1))
    result_df = SparkExpectationsActions.action_on_rules(
        _fixture_mock_context, streaming_df, 10, 0, 0, "row_dq", True
    )
    assert result_df.isStreaming is True


def test_agg_query_dq_detailed_result_type_error_line_210(_fixture_agg_dq_rule, _fixture_mock_context):
    """Test line 210: TypeError for unexpected aggregation result type"""
    df = spark.createDataFrame([{"col1": 1}])
    with patch.object(df, 'agg') as mock_agg:
        mock_result = Mock()
        mock_result.collect.return_value = [[[]]]  # Return list to trigger TypeError
        mock_agg.return_value = mock_result
        with pytest.raises(SparkExpectationsMiscException, match="error occurred while running agg_query_dq_detailed_result .*"):
            SparkExpectationsActions.agg_query_dq_detailed_result(
                _fixture_mock_context, _fixture_agg_dq_rule, df, []
            )

