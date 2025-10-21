import pytest
from unittest.mock import Mock
from spark_expectations.core.context import SparkExpectationsContext

@pytest.fixture(name="_fixture_mock_context_with_priority_data")
def fixture_mock_context_with_priority_data():
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
    _context_mock.product_id = "product_id1"
    _context_mock.get_min_priority_slack = "medium"
    _context_mock.get_summarized_row_dq_res = [
        {
            "rule": "rule_high_priority",
            "priority": "high",
            "failed_row_count": 50,
            "action_if_failed": "fail"
        },
        {
            "rule": "rule_medium_priority",
            "priority": "medium",
            "failed_row_count": 30,
            "action_if_failed": "ignore"
        },
        {
            "rule": "rule_low_priority",
            "priority": "low",
            "failed_row_count": 20,
            "action_if_failed": "drop"
        },
        {
            "rule": "rule_no_failures",
            "priority": "high",
            "failed_row_count": 0,
            "action_if_failed": "fail"
        }
    ]
    return _context_mock