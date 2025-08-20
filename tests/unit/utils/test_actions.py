from unittest.mock import Mock
from unittest.mock import patch

import pytest
from pyspark.sql.functions import lit, struct, array, udf

from spark_expectations.core import get_spark_session
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.utils.actions import SparkExpectationsActions

spark = get_spark_session()


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



import pytest


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
    "_rule_map, expected_output",
    [
        # expectations rule
        (
            {"rule_type": "type1", "rule": "rule1", "action_if_failed": "action1", "description": "desc1"},
            # result in spark col object
            array(
                [
                    struct(lit("rule_type"), lit("type1")),
                    struct(lit("rule"), lit("rule1")),
                    struct(lit("action_if_failed"), lit("action1")),
                    struct(lit("description"), lit("desc1")),
                ]
            ),
        ),
        # expectations rule
        (
            {"rule_type": "type2", "rule": "rule2", "action_if_failed": "action2", "description": "desc2"},
            # result in spark col object
            array(
                [
                    struct(lit("rule_type"), lit("type2")),
                    struct(lit("rule"), lit("rule2")),
                    struct(lit("action_if_failed"), lit("action2")),
                    struct(lit("description"), lit("desc2")),
                ]
            ),
        ),
    ],
)
def test_create_rules_map(_rule_map, expected_output):
    # test function which creates the dq_results based on the expectations
    actual_output = SparkExpectationsActions.create_rules_map(_rule_map)

    @udf
    def compare_result(_actual_output, _expected_output):
        for itr in (0, 4):
            assert _actual_output[itr] == _expected_output[itr]

    # assert dataframe column object using udf
    compare_result(actual_output, expected_output)





