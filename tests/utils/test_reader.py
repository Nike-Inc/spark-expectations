import os
from unittest.mock import patch
import pytest

# from pytest_mock import mocker // this will be automatically used while running using py-test
from spark_expectations.core import get_spark_session
from spark_expectations.utils.reader import SparkExpectationsReader
from spark_expectations.core.exceptions \
    import (
    SparkExpectationsUserInputOrConfigInvalidException,
    SparkExpectationsMiscException)
from pyspark.sql.functions import col

spark = get_spark_session()


@pytest.fixture(name="_fixture_reader")
@patch("spark_expectations.utils.reader.SparkExpectationsContext")
def fixture_reader(_mocker_context):
    product_id = 'product1'
    return SparkExpectationsReader(
        product_id, _mocker_context
    )


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


@pytest.mark.parametrize("notification, expected_result", [
    ({}, None),
    ({
         "spark.expectations.notifications.email.enabled": True,
         "spark.expectations.notifications.email.smtp_host": "smtp.mail.com",
         "spark.expectations.notifications.email.smtp_port": 587,
         "spark.expectations.notifications.email.from": "sender@mail.com",
         "spark.expectations.notifications.email.to.other.mail.com": "recipient@mail.com",
         "spark.expectations.notifications.email.subject": "Test email",
         "spark.expectations.notifications.slack.enabled": False,
         "spark.expectations.notifications.slack.webhook_url": "",
     }, None),
    ({
         "spark.expectations.notifications.email.enabled": True,
         "spark.expectations.notifications.email.smtp_host": "",
         "spark.expectations.notifications.email.smtp_port": 25,
         "spark.expectations.notifications.email.from": "",
         "spark.expectations.notifications.email.to.other.mail.com": "",
         "spark.expectations.notifications.email.subject": "",
     }, SparkExpectationsMiscException),

    ({
         "spark.expectations.notifications.email.enabled": False,
         "spark.expectations.notifications.email.smtp_host": "smtp.mail.com",
         "spark.expectations.notifications.email.smtp_port": 25,
         "spark.expectations.notifications.email.from": "sender@mail.com",
         "spark.expectations.notifications.email.to.other.mail.com": "recipient@mail.com",
         "spark.expectations.notifications.email.subject": "Test email"
     }, None),
    ({"spark.expectations.notifications.slack.enabled": True,
      "spark.expectations.notifications.slack.webhook_url": "https://hooks.slack.com/services/..."},
     None),
    ({
         "spark.expectations.notifications.slack.enabled": True,
         "spark.expectations.notifications.slack.webhook_url": "",
     }, SparkExpectationsMiscException),
])
@patch("spark_expectations.utils.reader.SparkExpectationsContext", autospec=True, spec_set=True)
def test_set_notification_param(mock_context, notification, expected_result):
    # This function helps/implements test cases for while setting notification
    # configurations

    # Create an instance of the class and set the product_id
    reader_handler = SparkExpectationsReader("product1", mock_context)

    if expected_result is None:
        assert reader_handler.set_notification_param(notification) == expected_result

        if notification.get("spark.expectations.notifications.email.enabled"):
            # assert parameter are context class respective method's are called correctly
            mock_context.set_enable_mail.assert_called_once_with(
                notification.get("spark.expectations.notifications.email.enabled"))
            mock_context.set_mail_subject.assert_called_once_with(
                notification.get("spark.expectations.notifications.email.subject"))
            mock_context.set_mail_smtp_server.assert_called_once_with(
                notification.get("spark.expectations.notifications.email.smtp_host"))
            mock_context.set_mail_smtp_port.assert_called_once_with(
                notification.get("spark.expectations.notifications.email.smtp_port"))
            mock_context.set_mail_from.assert_called_once_with(
                notification.get("spark.expectations.notifications.email.from"))
            mock_context.set_to_mail.assert_called_once_with(
                notification.get("spark.expectations.notifications.email.to.other.mail.com"))
        if notification.get("spark.expectations.notifications.slack.enabled"):
            mock_context.set_enable_slack.assert_called_once_with(
                notification.get("spark.expectations.notifications.slack.enabled"))
            mock_context.set_slack_webhook_url.assert_called_once_with(
                notification.get("spark.expectations.notifications.slack.webhook_url"))
    else:
        with pytest.raises(expected_result, match=r"All params/variables required for [a-z]+ notification "
                                                  "is not configured or supplied"):
            reader_handler.set_notification_param(notification)


@pytest.mark.usefixtures("_fixture_product_rules_view")
@pytest.mark.parametrize("product_id, table_name, action, tag, expected_output", [
    ("product1", "table1", ["fail", "drop"], "tag2", {"rule2": "expectation2"}),
    ("product2", "table1", ["drop", "ignore"], None, {"rule5": "expectation5", 'rule12': 'expectation12'}),
    ("product1", "table1", ["fail", "drop", "ignore"], None,
     {"rule1": "expectation1", "rule2": "expectation2", "rule3": "expectation3", "rule6": "expectation6",
      'rule10': 'expectation10', 'rule13': 'expectation13'}),
    ("product2", "table2", ["fail", "drop", "ignore"], "tag7", {})
])
def test_get_rules_dlt(product_id, table_name, action, tag, expected_output, mocker, _fixture_product_rules_view):
    # create mock _context object
    mock_context = mocker.MagicMock()

    # Create an instance of the class and set the product_id
    reade_handler = SparkExpectationsReader(product_id, mock_context)
    rules_dlt = reade_handler.get_rules_dlt("product_rules", table_name, action, tag)

    # Assert
    assert rules_dlt == expected_output


@pytest.mark.usefixtures("_fixture_product_rules_view")
@pytest.mark.parametrize("product_id, table_name, action, expected_expectations, expected_rule_execution_settings", [
    ("product1", "table1", ["fail", "drop"], {
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
                "error_drop_threshold": 10
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
                "error_drop_threshold": 0
            }
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
                "error_drop_threshold": 0
            }
        ],
        "query_dq_rules": [
            {
                "product_id": "product1",
                "table_name": "table1",
                "rule_type": "query_dq",
                "rule": "rule13",
                "column_name": "column10",
                "expectation": "expectation13",
                "action_if_failed": "fail",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": False,
                "tag": "tag13",
                "description": "description13",
                "enable_error_drop_alert": False,
                "error_drop_threshold": 0
            }
        ]
    }, {
        # should be the output of the _get_rules_execution_settings from reader.py
        "row_dq": True,
        "source_agg_dq": True,
        "target_agg_dq": True,
        "source_query_dq": True,
        "target_query_dq": False,
    })
])
@patch("spark_expectations.utils.reader.SparkExpectationsContext", autospec=True, spec_set=True)
def test_get_rules_from_table(mock_context, product_id, table_name,
                              action, expected_expectations, expected_rule_execution_settings,
                              _fixture_product_rules_view):
    # Create an instance of the class and set the product_id

    setattr(mock_context, "get_row_dq_rule_type_name", "row_dq")
    setattr(mock_context, "get_agg_dq_rule_type_name", "agg_dq")
    setattr(mock_context, "get_query_dq_rule_type_name", "query_dq")

    reader_handler = SparkExpectationsReader(product_id, mock_context)

    expectations, rule_execution_settings = reader_handler.get_rules_from_df("product_rules",
                                                                                "test_dq_stats_table",
                                                                             table_name,
                                                                             action)

    # Assert
    assert expectations == expected_expectations
    assert rule_execution_settings == expected_rule_execution_settings

    mock_context.set_dq_stats_table_name.assert_called_once_with("test_dq_stats_table")
    mock_context.set_final_table_name.assert_called_once_with(table_name)
    mock_context.set_error_table_name.assert_called_once_with(f"{table_name}_error")


def test_set_notification_param_exception(_fixture_reader):
    with pytest.raises(SparkExpectationsMiscException,
                       match=r"error occurred while reading notification configurations .*"):
        _fixture_reader.set_notification_param(['a', 'b', 'c'])


def test_get_rules_dlt_exception(_fixture_reader):
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException,
                       match=r"error occurred while reading or getting rules from the rules table .*"):
        _fixture_reader.get_rules_dlt("product_rules_1", "table1", ["fail", "drop"])


def test_get_rules_from_table_exception(_fixture_reader):
    with pytest.raises(SparkExpectationsMiscException,
                       match=r"error occurred while retrieving rules list from the table .*"):
        _fixture_reader.get_rules_from_df("mock_rules_table_1", "mock_dq_stats_table", "table1", ["fail", "drop"])