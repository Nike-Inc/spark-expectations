from unittest.mock import patch
import pytest
from spark_expectations.notifications.push.spark_expectations_notify import (
    SparkExpectationsNotify,
)
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.core.context import SparkExpectationsContext
from unittest.mock import Mock


@pytest.fixture(name="_fixture_mock_context")
def fixture_mock_context():
    _context_mock = Mock(spec=SparkExpectationsContext)
    _context_mock.get_table_name = "test_table"
    _context_mock.get_run_id = "test_run_id"
    _context_mock.get_run_date = "test_run_date"
    _context_mock.get_input_count = 1000
    _context_mock.get_error_percentage = 10.0
    _context_mock.get_output_percentage = 90.0
    _context_mock.get_error_drop_threshold = 15
    _context_mock.get_error_drop_percentage = 20.0
    _context_mock.get_success_percentage = 100.0
    _context_mock.get_source_agg_dq_status = "pass"
    _context_mock.get_source_query_dq_status = "pass"
    _context_mock.get_row_dq_status = "fail"
    _context_mock.get_final_agg_dq_status = "skipped"
    _context_mock.get_final_query_dq_status = "skipped"
    _context_mock.get_dq_run_status = "fail"
    _context_mock.get_row_dq_rule_type_name = "row_dq"
    _context_mock.product_id = "product_id1"
    _context_mock.get_enable_custom_email_body = False
    _context_mock.get_kafka_write_status = "Success"
    _context_mock.get_kafka_write_error_message = ""

    return _context_mock


@pytest.fixture(name="_fixture_notify_start_expected_result")
def fixture_notify_start_expected_result():
    return (
        "Spark expectations job has started \n\n"
        "table_name: test_table\n"
        "run_id: test_run_id\n"
        "run_date: test_run_date"
    )


@pytest.fixture(name="_fixture_notify_completion_expected_result")
def fixture_notify_completion_expected_result():
    return (
        "Spark expectations job has been completed  \n\n"
        "product_id: product_id1\n"
        "table_name: test_table\n"
        "run_id: test_run_id\n"
        "run_date: test_run_date\n"
        "input_count: 1000\n"
        "error_percentage: 10.0\n"
        "output_percentage: 90.0\n"
        "success_percentage: 100.0\n"
        "kafka_write_status: Success\n"
        "status: source_agg_dq_status = pass\n"
        "            source_query_dq_status = pass\n"
        "            row_dq_status = fail\n"
        "            final_agg_dq_status = skipped\n"
        "            final_query_dq_status = skipped\n"
        "            run_status = fail"
    )


@pytest.fixture(name="_fixture_notify_error_threshold_expected_result")
def fixture_notify_error_threshold_expected_result():
    return (
        "Spark expectations - dropped error percentage has been exceeded above the threshold "
        "value(15%) for `row_data` quality validation  \n\n"
        "product_id: product_id1\n"
        "table_name: test_table\n"
        "run_id: test_run_id\n"
        "run_date: test_run_date\n"
        "input_count: 1000\n"
        "error_percentage: 10.0\n"
        "error_drop_percentage: 20.0\n"
        "output_percentage: 90.0\n"
        "success_percentage: 100.0"
    )


@pytest.fixture(name="_fixture_notify_on_ignore_rules_expected_result")
def fixture_notify_on_ignore_rules_expected_result(_fixture_mock_context):
    return (
        "Spark expectations notification on rules which action_if_failed are set to ignore \n\n"
        "product_id: product_id1\n"
        "table_name: test_table\n"
        "run_id: test_run_id\n"
        "run_date: test_run_date\n"
        "input_count: 1000\n"
        "ignored_rules_run_results: "
        + str(
            (
                SparkExpectationsNotify(_fixture_mock_context),
                [
                    {
                        "rule": "value_positive_threshold",
                        "description": "count of value positive value must be greater than 10",
                        "rule_type": "query_dq",
                        "tag": "strict",
                        "action_if_failed": "ignore",
                    },
                    {
                        "rule": "sum_of_value_should_be_less_than_60",
                        "description": "desc_sum_of_value_should_be_less_than_60",
                        "rule_type": "agg_dq",
                        "tag": "strict",
                        "action_if_failed": "ignore",
                    },
                    {
                        "rule": "value_positive_threshold",
                        "description": "count of value positive value must be greater than 10",
                        "rule_type": "query_dq",
                        "tag": "strict",
                        "action_if_failed": "ignore",
                    },
                    {
                        "rule": "sum_of_value_should_be_less_than_60",
                        "description": "desc_sum_of_value_should_be_less_than_60",
                        "rule_type": "agg_dq",
                        "tag": "strict",
                        "action_if_failed": "ignore",
                    },
                ],
            )
        )
    )


@pytest.fixture(name="_fixture_notify_fail_expected_result")
def fixture_notify_fail_expected_result():
    return (
        "Spark expectations job has been failed  \n\n"
        "product_id: product_id1\n"
        "table_name: test_table\n"
        "run_id: test_run_id\n"
        "run_date: test_run_date\n"
        "input_count: 1000\n"
        "error_percentage: 10.0\n"
        "output_percentage: 90.0\n"
        "kafka_write_status: Success\n"
        "status: source_agg_dq_status = pass\n"
        "            source_query_dq_status = pass\n"
        "            row_dq_status = fail\n"
        "            final_agg_dq_status = skipped\n"
        "            final_query_dq_status = skipped\n"
        "            run_status = fail"
    )


def test_notify_on_start_completion_failure(
    _fixture_mock_context,
):
    _fixture_mock_context.get_notification_on_start = False
    _fixture_mock_context.get_notification_on_completion = False
    _fixture_mock_context.get_notification_on_fail = False

    notify_handler = SparkExpectationsNotify(_fixture_mock_context)

    @notify_handler.notify_on_start_completion_failure(
        lambda: print("start notification sent"),
        lambda: print("completion notification sent"),
        lambda e: print(f"failure notification sent: {e}"),
    )
    def dummy_function(raise_exception=False):
        if raise_exception:
            raise SparkExpectationsMiscException("Test Exception")
        return "Success"

    # Test on successful execution
    assert dummy_function() == "Success"


@patch(
    "spark_expectations.notifications.push.spark_expectations_notify._notification_hook",
    autospec=True,
    spec_set=True,
)
def test_notify_on_start(
    _mock_notification_hook,
    _fixture_mock_context,
    _fixture_notify_start_expected_result,
):
    notify_handler = SparkExpectationsNotify(_fixture_mock_context)

    # Call the function to be tested
    notify_handler.notify_on_start()

    # assert
    _mock_notification_hook.send_notification.assert_called_once_with(
        _context=_fixture_mock_context,
        _config_args={"message": _fixture_notify_start_expected_result, "content_type": "plain"},
    )


@patch(
    "spark_expectations.notifications.push.spark_expectations_notify._notification_hook",
    autospec=True,
    spec_set=True,
)
def test_notify_on_completion(
    _mock_notification_hook,
    _fixture_mock_context,
    _fixture_notify_completion_expected_result,
):
    notify_handler = SparkExpectationsNotify(_fixture_mock_context)

    # Call the function to be tested
    notify_handler.notify_on_completion()

    # assert for expected result
    _mock_notification_hook.send_notification.assert_called_once_with(
        _context=_fixture_mock_context,
        _config_args={"message": _fixture_notify_completion_expected_result, "content_type": "plain"},
    )


@patch(
    "spark_expectations.notifications.push.spark_expectations_notify._notification_hook",
    autospec=True,
    spec_set=True,
)
def test_notify_on_exceeds_of_error_threshold(
    _mock_notification_hook,
    _fixture_mock_context,
    _fixture_notify_error_threshold_expected_result,
):
    notify_handler = SparkExpectationsNotify(_fixture_mock_context)

    # Call the function to be tested
    notify_handler.notify_on_exceeds_of_error_threshold()

    # assert for expected result
    _mock_notification_hook.send_notification.assert_called_once_with(
        _context=_fixture_mock_context,
        _config_args={"message": _fixture_notify_error_threshold_expected_result, "content_type": "plain"},
    )


@patch(
    "spark_expectations.notifications.push.spark_expectations_notify._notification_hook",
    autospec=True,
    spec_set=True,
)
def test_notify_on_ignore_rules(
    _mock_notification_hook,
    _fixture_mock_context,
    _fixture_notify_on_ignore_rules_expected_result,
):
    notify_handler = SparkExpectationsNotify(_fixture_mock_context)

    ignored_rules_run_results = (
        SparkExpectationsNotify(_fixture_mock_context),
        [
            {
                "rule": "value_positive_threshold",
                "description": "count of value positive value must be greater than 10",
                "rule_type": "query_dq",
                "tag": "strict",
                "action_if_failed": "ignore",
            },
            {
                "rule": "sum_of_value_should_be_less_than_60",
                "description": "desc_sum_of_value_should_be_less_than_60",
                "rule_type": "agg_dq",
                "tag": "strict",
                "action_if_failed": "ignore",
            },
            {
                "rule": "value_positive_threshold",
                "description": "count of value positive value must be greater than 10",
                "rule_type": "query_dq",
                "tag": "strict",
                "action_if_failed": "ignore",
            },
            {
                "rule": "sum_of_value_should_be_less_than_60",
                "description": "desc_sum_of_value_should_be_less_than_60",
                "rule_type": "agg_dq",
                "tag": "strict",
                "action_if_failed": "ignore",
            },
        ],
    )

    # Call the function to be tested
    notify_handler.notify_on_ignore_rules(ignored_rules_run_results)

    # assert for expected result
    _mock_notification_hook.send_notification.assert_called_once_with(
        _context=_fixture_mock_context,
        _config_args={"message": _fixture_notify_on_ignore_rules_expected_result, "content_type": "plain"},
    )


@patch(
    "spark_expectations.notifications.push.spark_expectations_notify._notification_hook",
    autospec=True,
    spec_set=True,
)
def test_notify_on_failure(_mock_notification_hook, _fixture_mock_context, _fixture_notify_fail_expected_result):
    notify_handler = SparkExpectationsNotify(_fixture_mock_context)

    # Call the function to be tested
    notify_handler.notify_on_failure("exception")

    # assert for expected result
    _mock_notification_hook.send_notification.assert_called_once_with(
        _context=_fixture_mock_context,
        _config_args={"message": _fixture_notify_fail_expected_result, "content_type": "plain"},
    )


@patch(
    "spark_expectations.notifications.push.spark_expectations_notify._notification_hook",
    autospec=True,
    spec_set=True,
)
def test_notify_on_start_success(
    _mock_notification_hook,
    _fixture_mock_context,
    _fixture_notify_start_expected_result,
):
    _fixture_mock_context.get_notification_on_start = True
    _fixture_mock_context.get_notification_on_completion = False
    _fixture_mock_context.get_notification_on_fail = False

    notify_handler = SparkExpectationsNotify(_fixture_mock_context)

    @notify_handler.notify_on_start_completion_failure(
        notify_handler.notify_on_start,
        lambda: print("completion notification sent"),
        lambda e: print(f"failure notification sent: {e}"),
    )
    def dummy_function():
        return "Success"

    dummy_function()

    # assert for expected result
    _mock_notification_hook.send_notification.assert_called_once_with(
        _context=_fixture_mock_context,
        _config_args={"message": _fixture_notify_start_expected_result, "content_type": "plain"},
    )


@patch(
    "spark_expectations.notifications.push.spark_expectations_notify._notification_hook",
    autospec=True,
    spec_set=True,
)
def test_notify_on_completion_success(
    _mock_notification_hook,
    _fixture_mock_context,
    _fixture_notify_completion_expected_result,
):
    _fixture_mock_context.get_notification_on_start = False
    _fixture_mock_context.get_notification_on_completion = True
    _fixture_mock_context.get_notification_on_fail = False

    notify_handler = SparkExpectationsNotify(_fixture_mock_context)

    @notify_handler.notify_on_start_completion_failure(
        lambda: print("start notification sent"),
        notify_handler.notify_on_completion,
        lambda e: print(f"failure notification sent: {e}"),
    )
    def dummy_function():
        return "Success"

    dummy_function()

    # assert for expected result
    _mock_notification_hook.send_notification.assert_called_once_with(
        _context=_fixture_mock_context,
        _config_args={"message": _fixture_notify_completion_expected_result, "content_type": "plain"},
    )


@patch(
    "spark_expectations.notifications.push.spark_expectations_notify._notification_hook",
    autospec=True,
    spec_set=True,
)
def test_notify_on_failure_success(
    _mock_notification_hook, _fixture_mock_context, _fixture_notify_fail_expected_result
):
    _fixture_mock_context.get_notification_on_start = False
    _fixture_mock_context.get_notification_on_completion = False
    _fixture_mock_context.get_notification_on_fail = True

    notify_handler = SparkExpectationsNotify(_fixture_mock_context)

    @notify_handler.notify_on_start_completion_failure(
        lambda: print("start notification sent"),
        lambda: print("completion notification sent"),
        notify_handler.notify_on_failure,
    )
    def dummy_function(raise_exception=False):
        if raise_exception:
            raise SparkExpectationsMiscException("Test Exception")
        return "Success"

    with pytest.raises(SparkExpectationsMiscException, match="Test Exception"):
        dummy_function(raise_exception=True)

        # assert for expected result
        assert _fixture_mock_context.set_dq_run_status.assert_called_once_with("Failed")
        _mock_notification_hook.send_notification.assert_called_once_with(
            _context=_fixture_mock_context,
            _config_args={"message": _fixture_notify_fail_expected_result},
        )


@patch(
    "spark_expectations.notifications.push.spark_expectations_notify._notification_hook",
    autospec=True,
    spec_set=True,
)
def test_notify_on_failure_with_kafka_error(_mock_notification_hook, _fixture_mock_context):
    # Set up context with Kafka failure
    _fixture_mock_context.get_kafka_write_status = "Failed"
    _fixture_mock_context.get_kafka_write_error_message = "Connection timeout to Kafka broker"
    
    expected_message = (
        "Spark expectations job has been failed  \n\n"
        "product_id: product_id1\n"
        "table_name: test_table\n"
        "run_id: test_run_id\n"
        "run_date: test_run_date\n"
        "input_count: 1000\n"
        "error_percentage: 10.0\n"
        "output_percentage: 90.0\n"
        "kafka_write_status: Failed\n"
        "kafka_write_error: Connection timeout to Kafka broker\n"
        "status: source_agg_dq_status = pass\n"
        "            source_query_dq_status = pass\n"
        "            row_dq_status = fail\n"
        "            final_agg_dq_status = skipped\n"
        "            final_query_dq_status = skipped\n"
        "            run_status = fail"
    )

    notify_handler = SparkExpectationsNotify(_fixture_mock_context)
    notify_handler.notify_on_failure("exception")

    _mock_notification_hook.send_notification.assert_called_once_with(
        _context=_fixture_mock_context,
        _config_args={"message": expected_message, "content_type": "plain"},
    )


def test_notify_on_start_completion_failure_exception(_fixture_mock_context):
    _fixture_mock_context.get_notification_on_start = False
    _fixture_mock_context.get_notification_on_completion = False
    _fixture_mock_context.get_notification_on_fail = False

    notify_handler = SparkExpectationsNotify(_fixture_mock_context)

    @notify_handler.notify_on_start_completion_failure(
        lambda: print("start notification sent"),
        lambda: print("completion notification sent"),
        lambda e: print(f"failure notification sent: {e}"),
    )
    def dummy_function(raise_exception=False):
        if raise_exception:
            raise SparkExpectationsMiscException("Test Exception")
        return "Success"

    with pytest.raises(SparkExpectationsMiscException, match="Test Exception"):
        dummy_function(raise_exception=True)
        assert _fixture_mock_context.set_dq_run_status.assert_called_once_with("Failed")


def test_construct_message_for_each_rules(_fixture_mock_context):
    # Create an instance of the class under test
    notify_handler = SparkExpectationsNotify(_fixture_mock_context)

    # Set up the test input
    rule_name = "Rule 1"
    failed_row_count = 10
    error_drop_percentage = 5.6
    set_error_drop_threshold = 2.5
    action = "Notify"
    description = "Testing custom message for rule 1"

    # Call the method under test
    result = notify_handler.construct_message_for_each_rules(
        rule_name,
        failed_row_count,
        error_drop_percentage,
        action,
        description
    )

    # Assert the constructed notification message
    expected_message = (
        f"{description} \n"
        "product_id: product_id1\n"
        f"table_name: {_fixture_mock_context.get_table_name}\n"
        f"run_id: {_fixture_mock_context.get_run_id}\n"
        f"run_date: {_fixture_mock_context.get_run_date}\n"
        f"input_count: {_fixture_mock_context.get_input_count}\n"
        "rule_name: Rule 1\n"
        "action: Notify\n"
        "failed_row_count: 10\n"
        "error_drop_percentage: 5.6\n\n\n"
    )

    assert result == expected_message


@patch(
    "spark_expectations.notifications.push.spark_expectations_notify._notification_hook",
    autospec=True,
    spec_set=True,
)
def test_notify_on_exceeds_of_error_threshold_each_rules(_notification_hook, _fixture_mock_context):
    from unittest import mock

    notify_handler = SparkExpectationsNotify(_fixture_mock_context)

    # Define test data
    message = "Test message"

    # Call the function under test
    notify_handler.notify_on_exceeds_of_error_threshold_each_rules(message)

    # Assertions
    _notification_hook.send_notification.assert_called_once_with(
        _context=_fixture_mock_context,
        _config_args={
            "message": "Spark expectations - The number of notifications for rules being followed has surpassed the specified threshold \n\n\nTest message",
            "content_type": "plain"
        },
    )


@pytest.mark.parametrize(
    "failed_row_count, enable_error_drop_alert, set_error_drop_threshold, expected_notification_called",
    [
        (10, True, 5, True),  # Exceeds threshold, notification should be called
        (3, True, 5, False),  # Below threshold, notification should not be called
        (
            8,
            False,
            5,
            False,
        ),  # Disabled error drop alert, notification should not be called
    ],
)
@patch(
    "spark_expectations.notifications.push.spark_expectations_notify.SparkExpectationsNotify.notify_on_exceeds_of_error_threshold_each_rules",
    autospec=True,
    spec_set=True,
)
def test_notify_rules_exceeds_threshold(
    _mock_notification_hook,
    _fixture_mock_context,
    failed_row_count,
    enable_error_drop_alert,
    set_error_drop_threshold,
    expected_notification_called,
):
    # Create an instance of the class and initialize necessary variables
    _fixture_mock_context.get_input_count = 10
    # _fixture_mock_context._row_dq_rule_type_name = "row_dq"

    _fixture_mock_context.get_summarized_row_dq_res = [{"rule": "rule1", "failed_row_count": failed_row_count}]

    notify_handler = SparkExpectationsNotify(_fixture_mock_context)

    rules = {
        "row_dq_rules": [
            {
                "rule": "rule1",
                "enable_error_drop_alert": enable_error_drop_alert,
                "action_if_failed": "skip",
                "error_drop_threshold": set_error_drop_threshold,
            }
        ]
    }

    # Mock the notification method
    notification_called = False

    # Call the function to test
    notify_handler.notify_rules_exceeds_threshold(rules)

    # Assertions
    if expected_notification_called:
        _mock_notification_hook.assert_called_once()
    else:
        assert notification_called == expected_notification_called


@patch(
    "spark_expectations.notifications.push.spark_expectations_notify.SparkExpectationsNotify.notify_on_exceeds_of_error_threshold_each_rules",
    autospec=True,
    spec_set=True,
)
def test_notify_rules_exceeds_threshold_return_none(
    _mock_notification_hook,
    _fixture_mock_context,
):
    # Create an instance of the class and initialize necessary variables
    _fixture_mock_context.get_input_count = 10
    # _fixture_mock_context._row_dq_rule_type_name = "row_dq"

    _fixture_mock_context.get_summarized_row_dq_res = None

    notify_handler = SparkExpectationsNotify(_fixture_mock_context)

    # Call the function to test
    assert notify_handler.notify_rules_exceeds_threshold({}) == None


def test_notify_rules_exceeds_threshold_exception(_fixture_mock_context):
    # Simulate the case where get_summarized_row_dq_res is None
    _fixture_mock_context.get_summarized_row_dq_res = [{"rule": "rule1", "failed_row_count": 10}]

    rules = {
        "dq_rules": [
            {
                "rule": "rule1",
                "action_if_failed": "skip",
            }
        ]
    }

    notify_handler = SparkExpectationsNotify(_fixture_mock_context)

    # Expecting a SparkExpectationsMiscException to be raised
    with pytest.raises(
        SparkExpectationsMiscException,
        match="An error occurred while sending notification " r"when the error threshold is breached: *",
    ):
        notify_handler.notify_rules_exceeds_threshold(rules)


def test_get_custom_notification(_fixture_mock_context):
    _fixture_mock_context.get_email_custom_body = """'product_id': {}, 'table_name': {}"""
    _fixture_mock_context.get_stats_dict = [{"product_id": "product_id1", "table_name": "test_table", "input_count": 5}]

    result = SparkExpectationsNotify(_fixture_mock_context).get_custom_notification()
    expected_result = 'CUSTOM EMAIL\n{"product_id": "product_id1", "table_name": "test_table"}'
    assert result == expected_result


def test_get_custom_notification_no_dict_exception(_fixture_mock_context):
    _fixture_mock_context.get_email_custom_body = """Custom statistics for dq run:
        'product_id': {},
        'table_name': {}"""
    _fixture_mock_context.get_stats_dict = None

    with pytest.raises(SparkExpectationsMiscException, match="Stats dictionary list is not available or not a list."):
        SparkExpectationsNotify(_fixture_mock_context).get_custom_notification()


def test_get_custom_notification_no_keys_exception(_fixture_mock_context):
    _fixture_mock_context.get_email_custom_body = """Custom statistics for dq run:
        'product_id': ,
        'table_name': """
    _fixture_mock_context.get_stats_dict = [{"product_id": "product_id1", "table_name": "test_table", "input_count": 5}]

    with pytest.raises(SparkExpectationsMiscException, match="No key words for statistics were provided."):
        SparkExpectationsNotify(_fixture_mock_context).get_custom_notification()


def test_get_custom_notification_exception(_fixture_mock_context):
    _fixture_mock_context.get_stats_dict = "Not a list"
    _fixture_mock_context.get_email_custom_body = Mock(return_value="")

    # Create an instance of SparkExpectationsNotify with the mocked context
    notify = SparkExpectationsNotify(_context=_fixture_mock_context)

    # Assert that the exception is raised with the correct message
    with pytest.raises(
        SparkExpectationsMiscException,
        match=r"An error occurred while getting dictionary list with stats from dq run: Stats dictionary list is not available or not a list.",
    ):
        notify.get_custom_notification()


@patch(
    "spark_expectations.notifications.push.spark_expectations_notify._notification_hook",
    autospec=True,
    spec_set=True,
)
@patch.object(SparkExpectationsNotify, "get_custom_notification", return_value="Custom notification message")
def test_notify_on_completion_with_custom_email_body(
    mock_get_custom_notification,
    _mock_notification_hook,
    _fixture_mock_context,
):
    _fixture_mock_context.get_enable_custom_email_body = True

    notify_handler = SparkExpectationsNotify(_fixture_mock_context)

    # Call the function to be tested
    notify_handler.notify_on_completion()

    # assert for expected result
    _mock_notification_hook.send_notification.assert_called_once_with(
        _context=_fixture_mock_context,
        _config_args={"message": "Custom notification message", "content_type": "plain"},
    )


@patch(
    "spark_expectations.notifications.push.spark_expectations_notify._notification_hook",
    autospec=True,
    spec_set=True,
)
@patch.object(SparkExpectationsNotify, "get_custom_notification", return_value="Custom notification message")
def test_notify_on_failure_with_custom_email_body(
    mock_get_custom_notification,
    _mock_notification_hook,
    _fixture_mock_context,
):
    _fixture_mock_context.get_enable_custom_email_body = True

    notify_handler = SparkExpectationsNotify(_fixture_mock_context)

    # Call the function to be tested
    notify_handler.notify_on_failure("exception")

    # assert for expected result
    _mock_notification_hook.send_notification.assert_called_once_with(
        _context=_fixture_mock_context,
        _config_args={"message": "Custom notification message", "content_type": "plain"},
    )

@patch("spark_expectations.notifications.push.spark_expectations_notify._log")
def test_get_custom_notification(mock_log,_fixture_mock_context):
    _fixture_mock_context.get_email_custom_body = """'product_id': {}, 'table_name': {}, 'unmatched_key': {}"""
    _fixture_mock_context.get_stats_dict = [{"product_id": "product_id1", "table_name": "test_table", "input_count": 5}]

    result = SparkExpectationsNotify(_fixture_mock_context).get_custom_notification()
    expected_result = 'CUSTOM EMAIL\n{"product_id": "product_id1", "table_name": "test_table"}'
    assert result == expected_result

    assert mock_log.warning.call_count >= 1
    assert "unmatched_key" in mock_log.warning.call_args[0][0]

@patch(
    "spark_expectations.notifications.push.spark_expectations_notify._notification_hook",
    autospec=True,
    spec_set=True,
)
@patch.object(SparkExpectationsNotify, "construct_message_for_each_rules")
def test_notify_on_failed_dq_medium_priority(
    mock_construct_message,
    mock_notification_hook,
    _fixture_mock_context_with_priority_data
):
    # Setup
    mock_construct_message.side_effect = [
        "High priority rule message",
        "Medium priority rule message"
    ]
    
    notify_handler = SparkExpectationsNotify(_fixture_mock_context_with_priority_data)
    
    # Call the function to be tested
    notify_handler.notify_on_failed_dq()
    
    # Verify construct_message_for_each_rules was called for medium and high priority rules
    assert mock_construct_message.call_count == 2
    # Verify notification hook was called twice
    assert mock_notification_hook.send_notification.call_count == 2


@patch(
    "spark_expectations.notifications.push.spark_expectations_notify._notification_hook",
    autospec=True,
    spec_set=True,
)
@patch.object(SparkExpectationsNotify, "construct_message_for_each_rules")
def test_notify_on_failed_dq_high_priority(
    mock_construct_message,
    mock_notification_hook,
    _fixture_mock_context_with_priority_data
):
    # Setup - change priority to high
    _fixture_mock_context_with_priority_data.get_min_priority_slack = "high"
    mock_construct_message.return_value = "High priority rule message"
    
    notify_handler = SparkExpectationsNotify(_fixture_mock_context_with_priority_data)
    
    # Call the function to be tested
    notify_handler.notify_on_failed_dq()
    
    # Verify construct_message_for_each_rules was called only for high priority rule
    assert mock_construct_message.call_count == 1
    mock_construct_message.assert_called_with(
        rule_name="rule_high_priority",
        failed_row_count=50,
        error_drop_percentage=5.0,
        action="fail",
        description="rule_high_priority with priority high has  failed the row dq check"
    )
    
    # Verify notification hook was called once
    assert mock_notification_hook.send_notification.call_count == 1


@patch(
    "spark_expectations.notifications.push.spark_expectations_notify._notification_hook",
    autospec=True,
    spec_set=True,
)
@patch.object(SparkExpectationsNotify, "construct_message_for_each_rules")
def test_notify_on_failed_dq_low_priority(
    mock_construct_message,
    mock_notification_hook,
    _fixture_mock_context_with_priority_data
):
    # Setup - change priority to low
    _fixture_mock_context_with_priority_data.get_min_priority_slack = "low"
    mock_construct_message.side_effect = [
        "High priority rule message",
        "Medium priority rule message",
        "Low priority rule message"
    ]
    
    notify_handler = SparkExpectationsNotify(_fixture_mock_context_with_priority_data)
    
    # Call the function to be tested
    notify_handler.notify_on_failed_dq()
    
    # Verify construct_message_for_each_rules was called for all priority rules (excluding zero failures)
    assert mock_construct_message.call_count == 3
    
    # Verify notification hook was called three times
    assert mock_notification_hook.send_notification.call_count == 3


@patch(
    "spark_expectations.notifications.push.spark_expectations_notify._notification_hook",
    autospec=True,
    spec_set=True,
)
def test_notify_on_failed_dq_no_failed_rules(
    mock_notification_hook,
    _fixture_mock_context
):
    # Setup context with no failed rules
    _fixture_mock_context.get_min_priority_slack = "low"
    _fixture_mock_context.get_summarized_row_dq_res = [
        {
            "rule": "rule_no_failures_1",
            "priority": "high",
            "failed_row_count": 0,
            "action_if_failed": "fail"
        },
        {
            "rule": "rule_no_failures_2",
            "priority": "medium",
            "failed_row_count": 0,
            "action_if_failed": "ignore"
        }
    ]
    
    notify_handler = SparkExpectationsNotify(_fixture_mock_context)
    
    # Call the function to be tested
    notify_handler.notify_on_failed_dq()
    
    # Verify no notifications were sent
    assert mock_notification_hook.send_notification.call_count == 0


@patch(
    "spark_expectations.notifications.push.spark_expectations_notify._notification_hook",
    autospec=True,
    spec_set=True,
)
def test_notify_on_failed_dq_empty_rules(
    mock_notification_hook,
    _fixture_mock_context
):
    # Setup context with empty rules list
    _fixture_mock_context.get_min_priority_slack = "low"
    _fixture_mock_context.get_summarized_row_dq_res = []
    
    notify_handler = SparkExpectationsNotify(_fixture_mock_context)
    
    # Call the function to be tested
    notify_handler.notify_on_failed_dq()
    
    # Verify no notifications were sent
    assert mock_notification_hook.send_notification.call_count == 0


def test_get_rules_for_notification_filters_by_priority(_fixture_mock_context):
    # Setup test data
    _fixture_mock_context.get_summarized_row_dq_res = [
        {"rule": "rule1", "priority": "high", "failed_row_count": 10},
        {"rule": "rule2", "priority": "medium", "failed_row_count": 5},
        {"rule": "rule3", "priority": "low", "failed_row_count": 3},
        {"rule": "rule4", "priority": "high", "failed_row_count": 0}  # Should be filtered out
    ]
    
    notify_handler = SparkExpectationsNotify(_fixture_mock_context)
    
    # Test with high priority filter
    result = notify_handler._get_rules_for_notification(["high"])
    assert len(result) == 1
    assert result[0]["rule"] == "rule1"
    
    # Test with medium and high priority filter
    result = notify_handler._get_rules_for_notification(["medium", "high"])
    assert len(result) == 2
    assert any(rule["rule"] == "rule1" for rule in result)
    assert any(rule["rule"] == "rule2" for rule in result)
    
    # Test with all priorities
    result = notify_handler._get_rules_for_notification(["low", "medium", "high"])
    assert len(result) == 3


def test_get_rules_for_notification_filters_by_failed_count(_fixture_mock_context):
    # Setup test data with zero and non-zero failed counts
    _fixture_mock_context.get_summarized_row_dq_res = [
        {"rule": "rule1", "priority": "high", "failed_row_count": 10},
        {"rule": "rule2", "priority": "high", "failed_row_count": 0},
        {"rule": "rule3", "priority": "high", "failed_row_count": "0"},  # String zero
        {"rule": "rule4", "priority": "high", "failed_row_count": 5}
    ]
    
    notify_handler = SparkExpectationsNotify(_fixture_mock_context)
    
    # Test filtering - should only return rules with failed_row_count > 0
    result = notify_handler._get_rules_for_notification(["high"])
    assert len(result) == 2
    assert any(rule["rule"] == "rule1" for rule in result)
    assert any(rule["rule"] == "rule4" for rule in result)


@patch(
    "spark_expectations.notifications.push.spark_expectations_notify._notification_hook",
    autospec=True,
    spec_set=True,
)
def test_notify_on_failed_dq_message_content(
    mock_notification_hook,
    _fixture_mock_context_with_priority_data
):
    # Setup
    notify_handler = SparkExpectationsNotify(_fixture_mock_context_with_priority_data)
    
    # Call the function to be tested
    notify_handler.notify_on_failed_dq()
    
    # Verify the message content structure by checking calls to send_notification
    calls = mock_notification_hook.send_notification.call_args_list
    
    for call in calls:
        args, kwargs = call
        # Verify the call structure
        assert "_context" in kwargs or len(args) >= 1
        assert "_config_args" in kwargs or len(args) >= 2
        
        # Extract config_args
        config_args = kwargs.get("_config_args", args[1] if len(args) > 1 else {})
        assert "message" in config_args
        assert "content_type" in config_args
        assert config_args["content_type"] == "plain"


@pytest.mark.parametrize(
    "min_priority, expected_rule_count",
    [
        ("high", 1),    # Only high priority rules
        ("medium", 2),  # Medium and high priority rules
        ("low", 3)      # All priority rules (excluding zero failures)
    ]
)
@patch(
    "spark_expectations.notifications.push.spark_expectations_notify._notification_hook",
    autospec=True,
    spec_set=True,
)
def test_notify_on_failed_dq_priority_filtering_parametrized(
    mock_notification_hook,
    _fixture_mock_context_with_priority_data,
    min_priority,
    expected_rule_count
):
    # Setup
    _fixture_mock_context_with_priority_data.get_min_priority_slack = min_priority
    notify_handler = SparkExpectationsNotify(_fixture_mock_context_with_priority_data)
    
    # Call the function to be tested
    notify_handler.notify_on_failed_dq()
    
    # Verify the expected number of notifications were sent
    assert mock_notification_hook.send_notification.call_count == expected_rule_count
