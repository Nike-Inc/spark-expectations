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
        _config_args={"message": _fixture_notify_start_expected_result},
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
        _config_args={"message": _fixture_notify_completion_expected_result},
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
        _config_args={"message": _fixture_notify_error_threshold_expected_result},
    )


@patch(
    "spark_expectations.notifications.push.spark_expectations_notify._notification_hook",
    autospec=True,
    spec_set=True,
)
def test_notify_on_failure(
    _mock_notification_hook, _fixture_mock_context, _fixture_notify_fail_expected_result
):

    notify_handler = SparkExpectationsNotify(_fixture_mock_context)

    # Call the function to be tested
    notify_handler.notify_on_failure("exception")

    # assert for expected result
    _mock_notification_hook.send_notification.assert_called_once_with(
        _context=_fixture_mock_context,
        _config_args={"message": _fixture_notify_fail_expected_result},
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
        _config_args={"message": _fixture_notify_start_expected_result},
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
        _config_args={"message": _fixture_notify_completion_expected_result},
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

    # Call the method under test
    result = notify_handler.construct_message_for_each_rules(
        rule_name,
        failed_row_count,
        error_drop_percentage,
        set_error_drop_threshold,
        action,
    )

    # Assert the constructed notification message
    expected_message = (
        "Rule 1 has been exceeded above the threshold value(2.5%) for `row_data` quality validation\n"
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
def test_notify_on_exceeds_of_error_threshold_each_rules(
    _notification_hook, _fixture_mock_context
):
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
            "message": "Spark expectations - The number of notifications for rules being followed has surpassed the specified threshold \n\n\nTest message"
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

    _fixture_mock_context.get_summarized_row_dq_res = [
        {"rule": "rule1", "failed_row_count": failed_row_count}
    ]

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
    _fixture_mock_context.get_summarized_row_dq_res = [
        {"rule": "rule1", "failed_row_count": 10}
    ]

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
        match="An error occurred while sending notification "
        r"when the error threshold is breached: *",
    ):
        notify_handler.notify_rules_exceeds_threshold(rules)
