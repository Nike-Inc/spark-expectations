from unittest.mock import Mock
from unittest.mock import patch

import pytest
from pyspark.sql.functions import lit, struct, array, udf

from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.utils.actions import SparkExpectationsActions


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


def test_agg_query_dq_detailed_result_exception():
    _mock_object_context = Mock(spec=SparkExpectationsContext)
    # faulty user input is given to test the exception functionality of the agg_query_dq_detailed_result

    with pytest.raises(
        SparkExpectationsMiscException, match=r"error occurred while running agg_query_dq_detailed_result .*"
    ):
        SparkExpectationsActions().agg_query_dq_detailed_result(
            _mock_object_context, "_fixture_query_dq_rule", "<df>", []
        )
