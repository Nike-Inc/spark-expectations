from unittest.mock import Mock, MagicMock
from unittest.mock import patch

import pytest
from pyspark.sql.functions import lit, struct, array, udf

from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.utils.actions import SparkExpectationsActions
from spark_expectations.utils.reader import SparkExpectationsReader
from spark_expectations.utils.regulate_flow import SparkExpectationsRegulateFlow


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


def _mock_rules_df_empty():
    """Used by test_default_error_table_naming to allow get_rules_from_df to run without real Spark DataFrame ops."""
    rules_df = MagicMock()
    filtered_df = MagicMock()
    rules_df.filter.return_value = filtered_df
    filtered_df.collect.return_value = []
    return rules_df


def test_default_error_table_naming():
    """Expectation: The default behavior of appending _error to the table name still works"""
    ctx = SparkExpectationsContext(product_id="p1", spark=Mock())

    reader = SparkExpectationsReader(_context=ctx)

    reader._get_rules_execution_settings = Mock(return_value={}) # avoid spark transformations inside _get_rules_execution_settings

    reader.get_rules_from_df(rules_df=_mock_rules_df_empty(), target_table="my_db.target_table") # call get_rules_from_df which calls sets self._context.set_error_table_name(f"{target_table}_error")

    assert ctx.get_error_table_name == "my_db.target_table_error" # validate the default error table name for 'target_table'
    assert ctx.get_error_table_name_user_specified is False


def test_error_table_override():
    """ Expectation: different_catalog.my_override_error is used as the error table instead of the default my_db.target_table_error"""
    target_table_override= "different_catalog.my_override_error" # Define the error table name we want
    ctx = SparkExpectationsContext(product_id="p1", spark=Mock())
    ctx.set_error_table_name(target_table_override) #  set_error_table_name as different_catalog.my_override_error

    actions = Mock()
    writer = Mock()
    notification = Mock()

    dq_df = MagicMock(name="dq_df")
    error_df = MagicMock(name="error_df")

    actions.run_dq_rules.return_value = dq_df
    actions.action_on_rules.return_value = MagicMock(name="out_df")
    writer.write_error_records_final.return_value = (0, error_df)

    process = SparkExpectationsRegulateFlow.execute_dq_process(
        _context=ctx,
        _actions=actions,
        _writer=writer,
        _notification=notification,
        expectations={},
        _input_count=1,
    )

    process(df=MagicMock(name="input_df"), _rule_type="row_dq", row_dq_flag=True)

    # Assert that write_error_records_final was called with target_table_override instead of "my_db.target_table_error"
    writer.write_error_records_final.assert_called_once_with(
        dq_df,
        target_table_override,
        ctx.get_row_dq_rule_type_name,
    )

    # Assert that the get_error_table method returns the override value
    assert ctx.get_error_table_name == target_table_override
    assert ctx.get_error_table_name_user_specified is True


def test_multi_decorator_default_updates():
    """Verify default error table updates across multiple decorator calls"""
    ctx = SparkExpectationsContext(product_id="p1", spark=Mock())
    reader = SparkExpectationsReader(_context=ctx)
    reader._get_rules_execution_settings = Mock(return_value={})

    # First call
    reader.get_rules_from_df(rules_df=_mock_rules_df_empty(), target_table="db.table_a")
    assert ctx.get_error_table_name == "db.table_a_error"
    assert ctx.get_error_table_name_user_specified is False

    # Second call - should update, not stick
    reader.get_rules_from_df(rules_df=_mock_rules_df_empty(), target_table="db.table_b")
    assert ctx.get_error_table_name == "db.table_b_error"  # Not "db.table_a_error"
    assert ctx.get_error_table_name_user_specified is False
