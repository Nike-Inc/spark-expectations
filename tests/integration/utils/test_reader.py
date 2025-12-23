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


@pytest.fixture(name="_fixture_reader")
@patch("spark_expectations.utils.reader.SparkExpectationsContext")
def fixture_reader(_mocker_context):
    product_id = "product1"
    _mocker_context.spark = spark
    return SparkExpectationsReader(_mocker_context)


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
            StructField("priority", StringType(), False),
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


@pytest.mark.parametrize(
    "notification, expected_result",
    [
        ({}, None),
        (
            {
                "spark.expectations.notifications.email.enabled": True,
                "spark.expectations.notifications.email.custom.body.enable": False,
                "spark.expectations.notifications.email.smtp.host": "smtp.mail.com",
                "spark.expectations.notifications.email.smtp.port": 587,
                "spark.expectations.notifications.email.from": "sender@mail.com",
                "spark.expectations.notifications.email.to.other.mail.com": "recipient@mail.com",
                "spark.expectations.notifications.email.subject": "Test email",
                "spark.expectations.notifications.slack.enabled": False,
                "spark.expectations.notifications.slack.webhook.url": "",
                "spark.expectations.notifications.teams.enabled": False,
                "spark.expectations.notifications.teams.webhook.url": "",
                "spark.expectations.notifications.pagerduty.enabled": False,
                "spark.expectations.notifications.pagerduty.integration.key": "",
                "spark.expectations.notifications.pagerduty.webhook.url": "",
            },
            None,
        ),
        (
            {
                "spark.expectations.notifications.email.enabled": True,
                "spark.expectations.notifications.email.smtp.host": "",
                "spark.expectations.notifications.email.smtp.port": 25,
                "spark.expectations.notifications.email.from": "",
                "spark.expectations.notifications.email.to.other.mail.com": "",
                "spark.expectations.notifications.email.subject": "",
            },
            SparkExpectationsMiscException,
        ),
        (
            {
                "spark.expectations.notifications.email.enabled": False,
                "spark.expectations.notifications.email.smtp.host": "smtp.mail.com",
                "spark.expectations.notifications.email.smtp.port": 25,
                "spark.expectations.notifications.email.from": "sender@mail.com",
                "spark.expectations.notifications.email.to.other.mail.com": "recipient@mail.com",
                "spark.expectations.notifications.email.subject": "Test email",
            },
            None,
        ),
        (
            {
                "spark.expectations.notifications.slack.enabled": True,
                "spark.expectations.notifications.slack.webhook.url": "https://hooks.slack.com/services/...",
            },
            None,
        ),
        (
            {
                "spark.expectations.notifications.slack.enabled": True,
                "spark.expectations.notifications.slack.webhook.url": "",
            },
            SparkExpectationsMiscException,
        ),
        (
            {
                "spark.expectations.notifications.teams.enabled": True,
                "spark.expectations.notifications.teams.webhook.url": "https://hooks.teams.com/services/...",
            },
            None,
        ),
        (
            {
                "spark.expectations.notifications.teams.enabled": True,
                "spark.expectations.notifications.teams.webhook.url": "",
            },
            SparkExpectationsMiscException,
        ),
        (
            {
                "spark.expectations.notifications.email.custom.body.enable": True,
                "spark.expectations.notifications.email.enabled": True,
                "spark.expectations.notifications.email.smtp.host": "smtp.mail.com",
                "spark.expectations.notifications.email.smtp.port": 587,
                "spark.expectations.notifications.email.from": "sender@mail.com",
                "spark.expectations.notifications.email.to.other.mail.com": "recipient@mail.com",
                "spark.expectations.notifications.email.subject": "Test email",
                "spark.expectations.notifications.email.custom.body": "Test email body",
                "spark.expectations.notifications.slack.enabled": False,
                "spark.expectations.notifications.slack.webhook.url": "",
                "spark.expectations.notifications.teams.enabled": False,
                "spark.expectations.notifications.teams.webhook.url": "",
                "spark.expectations.notifications.pagerduty.enabled": False,
                "spark.expectations.notifications.pagerduty.integration.key": "",
                "spark.expectations.notifications.pagerduty.webhook.url": "",
            },
            None,
        ),
        (
            {
                "spark.expectations.notifications.email.smtp.server.auth": True,
                "spark.expectations.notifications.email.enabled": True,
                "spark.expectations.notifications.email.smtp.host": "smtp.mail.com",
                "spark.expectations.notifications.email.smtp.port": 587,
                "spark.expectations.notifications.email.smtp.password": "password",
                "spark.expectations.notifications.email.from": "sender@mail.com",
                "spark.expectations.notifications.email.to.other.mail.com": "recipient@mail.com",
                "spark.expectations.notifications.email.subject": "Test email",
                "spark.expectations.notifications.slack.enabled": False,
                "spark.expectations.notifications.slack.webhook.url": "",
                "spark.expectations.notifications.teams.enabled": False,
                "spark.expectations.notifications.teams.webhook.url": "",
                "spark.expectations.notifications.pagerduty.enabled": False,
                "spark.expectations.notifications.pagerduty.integration.key": "",
                "spark.expectations.notifications.pagerduty.webhook.url": "",
            },
            None,
        ),
        (
            {
                "spark.expectations.notifications.email.smtp.server.auth": True,
                "spark.expectations.notifications.email.enabled": True,
                "spark.expectations.notifications.email.smtp.host": "smtp.mail.com",
                "spark.expectations.notifications.email.smtp.port": 587,
                "spark.expectations.notifications.smtp.creds.dict": {
                    "se.streaming.secret.type": "cerberus",
                    "se.streaming.cerberus.url": "https://xyz.com",
                    "se.streaming.cerberus.sdb.path": "abc",
                    "spark.expectations.notifications.cerberus.smtp.password": "def",
                },
                "spark.expectations.notifications.email.from": "sender@mail.com",
                "spark.expectations.notifications.email.to.other.mail.com": "recipient@mail.com",
                "spark.expectations.notifications.email.subject": "Test email",
                "spark.expectations.notifications.slack.enabled": False,
                "spark.expectations.notifications.slack.webhook.url": "",
                "spark.expectations.notifications.teams.enabled": False,
                "spark.expectations.notifications.teams.webhook.url": "",
                "spark.expectations.notifications.pagerduty.enabled": False,
                "spark.expectations.notifications.pagerduty.integration.key": "",
                "spark.expectations.notifications.pagerduty.webhook.url": "",
            },
            None,
        ),
        (
            {
                "spark.expectations.notifications.email.smtp.server.auth": True,
                "spark.expectations.notifications.email.enabled": True,
                "spark.expectations.notifications.email.smtp.host": "smtp.mail.com",
                "spark.expectations.notifications.email.smtp.port": 587,
                "spark.expectations.notifications.email.from": "sender@mail.com",
                "spark.expectations.notifications.email.to.other.mail.com": "recipient@mail.com",
                "spark.expectations.notifications.email.subject": "Test email",
                "spark.expectations.notifications.slack.enabled": False,
                "spark.expectations.notifications.slack.webhook.url": "",
                "spark.expectations.notifications.teams.enabled": False,
                "spark.expectations.notifications.teams.webhook.url": "",
                "spark.expectations.notifications.pagerduty.enabled": False,
                "spark.expectations.notifications.pagerduty.integration.key": "",
                "spark.expectations.notifications.pagerduty.webhook.url": "",
            },
            SparkExpectationsMiscException,
        ),
        (
            {
                "spark.expectations.notifications.email.smtp.server.auth": True,
                "spark.expectations.notifications.email.enabled": True,
                "spark.expectations.notifications.email.smtp.host": "smtp.mail.com",
                "spark.expectations.notifications.email.smtp.port": 587,
                "spark.expectations.notifications.smtp.creds.dict": {
                    "se.streaming.secret.type": 1,
                    "se.streaming.cerberus.url": True,
                },
                "spark.expectations.notifications.email.from": "sender@mail.com",
                "spark.expectations.notifications.email.to.other.mail.com": "recipient@mail.com",
                "spark.expectations.notifications.email.subject": "Test email",
                "spark.expectations.notifications.slack.enabled": False,
                "spark.expectations.notifications.slack.webhook.url": "",
                "spark.expectations.notifications.teams.enabled": False,
                "spark.expectations.notifications.teams.webhook.url": "",
                "spark.expectations.notifications.pagerduty.enabled": True,
                "spark.expectations.notifications.pagerduty.integration.key": "",
                "spark.expectations.notifications.pagerduty.webhook.url": "",
            },
            SparkExpectationsMiscException,
        ),
        (
            {
                "spark.expectations.notifications.pagerduty.enabled": True,
                "spark.expectations.notifications.pagerduty.integration.key": "",
                "spark.expectations.notifications.pagerduty.webhook.url": "",
            },
            SparkExpectationsMiscException,
        ),
        (
            {
                "spark.expectations.notifications.pagerduty.enabled": True,
                "spark.expectations.notifications.pagerduty.integration.key": "123",
                "spark.expectations.notifications.pagerduty.webhook.url": "https://test.url/",
            },
            None,
        )
    ],
)
def test_set_notification_param(notification, expected_result):
    # This function helps/implements test cases for while setting notification
    # configurations
    mock_context = Mock(spec=SparkExpectationsContext)
    mock_context.spark = spark

    # Create an instance of the class and set the product_id
    reader_handler = SparkExpectationsReader(mock_context)

    if expected_result is None:
        assert reader_handler.set_notification_param(notification) == expected_result

        if notification.get("spark.expectations.notifications.email.enabled"):
            # assert parameter are context class respective method's are called correctly
            mock_context.set_enable_mail.assert_called_once_with(
                notification.get("spark.expectations.notifications.email.enabled")
            )
            mock_context.set_mail_subject.assert_called_once_with(
                notification.get("spark.expectations.notifications.email.subject")
            )
            mock_context.set_mail_smtp_server.assert_called_once_with(
                notification.get("spark.expectations.notifications.email.smtp.host")
            )
            mock_context.set_mail_smtp_port.assert_called_once_with(
                notification.get("spark.expectations.notifications.email.smtp.port")
            )
            mock_context.set_mail_from.assert_called_once_with(
                notification.get("spark.expectations.notifications.email.from")
            )
            mock_context.set_to_mail.assert_called_once_with(
                notification.get("spark.expectations.notifications.email.to.other.mail.com")
            )
        if notification.get("spark.expectations.notifications.email.custom.body.enable"):
            mock_context.set_enable_mail.assert_called_once_with(
                notification.get("spark.expectations.notifications.email.enabled")
            )
            mock_context.set_mail_subject.assert_called_once_with(
                notification.get("spark.expectations.notifications.email.subject")
            )
            mock_context.set_mail_smtp_server.assert_called_once_with(
                notification.get("spark.expectations.notifications.email.smtp.host")
            )
            mock_context.set_mail_smtp_port.assert_called_once_with(
                notification.get("spark.expectations.notifications.email.smtp.port")
            )
            mock_context.set_mail_from.assert_called_once_with(
                notification.get("spark.expectations.notifications.email.from")
            )
            mock_context.set_to_mail.assert_called_once_with(
                notification.get("spark.expectations.notifications.email.to.other.mail.com")
            )
            mock_context.set_enable_custom_email_body.assert_called_once_with(
                notification.get("spark.expectations.notifications.email.custom.body.enable")
            )
            mock_context.set_email_custom_body.assert_called_once_with(
                notification.get("spark.expectations.notifications.email.custom.body")
            )
        if notification.get("spark.expectations.notifications.email.smtp.server.auth"):
            mock_context.set_enable_smtp_server_auth.assert_called_once_with(
                notification.get("spark.expectations.notifications.email.smtp.server.auth")
            )
            if notification.get("spark.expectations.notifications.email.smtp.password"):
                mock_context.set_mail_smtp_password.assert_called_once_with(
                    notification.get("spark.expectations.notifications.email.smtp.password")
                )
            elif notification.get("spark.expectations.notifications.smtp.creds.dict"):
                mock_context.set_smtp_creds_dict.assert_called_once_with(
                    notification.get("spark.expectations.notifications.smtp.creds.dict")
                )
        if notification.get("spark.expectations.notifications.slack.enabled"):
            mock_context.set_enable_slack.assert_called_once_with(
                notification.get("spark.expectations.notifications.slack.enabled")
            )
            mock_context.set_slack_webhook_url.assert_called_once_with(
                notification.get("spark.expectations.notifications.slack.webhook.url")
            )
        if notification.get("spark.expectations.notifications.teams.enabled"):
            mock_context.set_enable_teams.assert_called_once_with(
                notification.get("spark.expectations.notifications.teams.enabled")
            )
            mock_context.set_teams_webhook_url.assert_called_once_with(
                notification.get("spark.expectations.notifications.teams.webhook.url")
            )
        if notification.get("spark.expectations.notifications.pagerduty.enabled"):
            mock_context.set_enable_pagerduty.assert_called_once_with(
                notification.get("spark.expectations.notifications.pagerduty.enabled")
            )
            mock_context.set_pagerduty_integration_key.assert_called_once_with(
                notification.get("spark.expectations.notifications.pagerduty.integration.key")
            )
            mock_context.set_pagerduty_webhook_url.assert_called_once_with(
                notification.get("spark.expectations.notifications.pagerduty.webhook.url")
            )
    else:
        with pytest.raises(
            expected_result,
            match=r"All params/variables required for [a-z]+ notification "
            "is not configured or supplied|error occurred while reading "
            "notification configurations SMTP password is not set or secret dict for its retrieval is not provided|"
            "error occurred while reading notification configurations SMTP creds dict contains non-string keys or values",
        ):
            reader_handler.set_notification_param(notification)


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
                        "priority": "medium",
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
                        "priority": "medium",
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
                        "priority": "medium",
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
                        "priority": "medium",
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
                        "priority": "medium",
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
                        "priority": "medium",
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
                        "priority": "medium",
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
                        "priority": "medium",
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
                        "priority": "medium",
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


def test_set_notification_param_exception(_fixture_reader):
    with pytest.raises(
        SparkExpectationsMiscException, match=r"error occurred while reading notification configurations .*"
    ):
        _fixture_reader.set_notification_param(["a", "b", "c"])


def test_get_rules_dlt_exception(_fixture_reader):
    with pytest.raises(SparkExpectationsMiscException, match=r"error occurred while retrieving rules list .*"):
        _fixture_reader.get_rules_from_df("product_rules_1", "table1", is_dlt=True, tag=None)


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


def test_get_rules_from_table_exception(_fixture_reader):
    with pytest.raises(SparkExpectationsMiscException, match=r"error occurred while retrieving rules list .*"):
        _fixture_reader.get_rules_from_df(
            "mock_rules_table_1",
            "table1",
        )


def test_set_notification_param_missing_dq_obs_alert_flag():
    """Test set_notification_param when se_dq_obs_alert_flag is False - covers lines 82-83 in reader.py"""
    from spark_expectations.config.user_config import Constants as user_config
    
    mock_context = Mock(spec=SparkExpectationsContext)
    mock_context.spark = spark
    reader = SparkExpectationsReader(mock_context)
    
    # Test notification configuration that triggers the else branch (lines 82-83)
    notification_missing_alert_flag = {
        user_config.se_enable_obs_dq_report_result: True,
        user_config.se_dq_obs_alert_flag: False,  # This will trigger the else branch
    }
    
    # This should raise SparkExpectationsMiscException and hit lines 82-83
    with pytest.raises(SparkExpectationsMiscException, 
                      match="All params/variables required for email notification is not configured or supplied"):
        reader.set_notification_param(notification_missing_alert_flag)
    
    # Verify that set_se_dq_obs_alert_flag(False) was called (line 82)
    mock_context.set_se_dq_obs_alert_flag.assert_called_with(False)


def test_set_notification_param_zoom_missing_webhook():
    """Test set_notification_param when zoom is enabled but webhook URL is missing - covers line 179 in reader.py"""
    from spark_expectations.config.user_config import Constants as user_config
    
    mock_context = Mock(spec=SparkExpectationsContext)
    mock_context.spark = spark
    reader = SparkExpectationsReader(mock_context)
    
    # Test zoom notification with missing webhook URL - need teams enabled first since zoom is nested
    notification_zoom_no_webhook = {
        user_config.se_notifications_enable_teams: True,
        user_config.se_notifications_teams_webhook_url: "https://teams.webhook.url",
        user_config.se_notifications_enable_zoom: True,
        # Missing se_notifications_zoom_webhook_url - this will trigger the else branch (line 179)
    }
    
    # This should raise SparkExpectationsMiscException at line 179
    with pytest.raises(SparkExpectationsMiscException, 
                      match="All params/variables required for zoom notification is not configured or supplied"):
        reader.set_notification_param(notification_zoom_no_webhook)


def test_set_notification_param_smtp_auth_no_password():
    """Test set_notification_param SMTP auth without password - covers lines 140 in reader.py"""
    from spark_expectations.config.user_config import Constants as user_config
    
    mock_context = Mock(spec=SparkExpectationsContext)
    mock_context.spark = spark
    reader = SparkExpectationsReader(mock_context)
    
    # Test SMTP auth without password or creds dict - should hit line 140
    notification_smtp_auth_no_creds = {
        user_config.se_notifications_enable_email: True,
        user_config.se_notifications_email_smtp_host: "smtp.example.com",
        user_config.se_notifications_email_from: "test@example.com", 
        user_config.se_notifications_email_to_other_mail_id: "recipient@example.com",
        user_config.se_notifications_email_subject: "Test",
        user_config.se_notifications_enable_smtp_server_auth: True,
        # Missing both se_notifications_smtp_password and se_notifications_smtp_creds_dict
    }
    
    # This should raise SparkExpectationsMiscException due to KeyError
    with pytest.raises(SparkExpectationsMiscException, 
                      match="error occurred while reading notification configurations"):
        reader.set_notification_param(notification_smtp_auth_no_creds)


def test_process_rules_df_custom_output_enabled():
    """Test _process_rules_df method to cover lines 237, 273 in reader.py"""
    from spark_expectations.config.user_config import Constants as user_config
    
    mock_context = Mock(spec=SparkExpectationsContext)
    mock_context.spark = spark
    reader = SparkExpectationsReader(mock_context)
    
    # Test process_rules_df with custom output enabled
    _dq_queries_dict = {}
    column_map = {
        "product_id": "test_product",
        "table_name": "test_table", 
        "rule": "test_rule",
        "expectation": "SELECT COUNT(*) FROM {table} WHERE {condition}"
    }
    
    # Row with custom delimiter and enable_querydq_custom_output = True
    _row = {
        "query_dq_delimiter": "|",
        "enable_querydq_custom_output": True,
        "expectation": "SELECT COUNT(*)@table:source_table@condition:col1 > 0"
    }
    
    params = {"table": "source_table", "condition": "col1 > 0"}
    
    # This should cover lines related to custom query processing
    result_dict, result_map = reader._process_rules_df(_dq_queries_dict, column_map, _row, params)
    
    # Verify the custom output flag is set
    assert result_map["enable_querydq_custom_output"] is True


def test_set_notification_param_obs_dq_report_positive():
    """Test set_notification_param with complete obs DQ report configuration - covers lines 63-78"""
    from spark_expectations.config.user_config import Constants as user_config
    
    mock_context = Mock(spec=SparkExpectationsContext)
    mock_context.spark = spark
    reader = SparkExpectationsReader(mock_context)
    
    # Complete obs DQ report notification configuration  
    notification_obs_dq_complete = {
        user_config.se_enable_obs_dq_report_result: True,
        user_config.se_dq_obs_alert_flag: True,
        user_config.se_notifications_email_smtp_port: "587",
        user_config.se_notifications_email_subject: "DQ Report",
        user_config.se_notifications_smtp_password: "password123",
        user_config.se_notifications_email_from: "dq@example.com",
        user_config.se_notifications_email_to_other_mail_id: "recipient@example.com",
        user_config.se_notifications_email_smtp_host: "smtp.example.com",
        user_config.se_dq_obs_default_email_template: "default_template"
    }
    
    # This should cover lines 63-78 (complete configuration path)
    reader.set_notification_param(notification_obs_dq_complete)
    
    # Verify the obs DQ report configuration was set
    mock_context.set_enable_obs_dq_report_result.assert_called_with(True)
    mock_context.set_se_dq_obs_alert_flag.assert_called_with(True)
    mock_context.set_mail_smtp_port.assert_called_with(587)


def test_set_notification_param_zoom_complete():
    """Test set_notification_param with complete zoom configuration - covers lines 173-177"""
    from spark_expectations.config.user_config import Constants as user_config
    
    mock_context = Mock(spec=SparkExpectationsContext)
    mock_context.spark = spark
    reader = SparkExpectationsReader(mock_context)
    
    # Complete zoom notification configuration (needs teams first since it's nested)
    notification_zoom_complete = {
        user_config.se_notifications_enable_teams: True,
        user_config.se_notifications_teams_webhook_url: "https://teams.webhook.url",
        user_config.se_notifications_enable_zoom: True,
        user_config.se_notifications_zoom_webhook_url: "https://zoom.webhook.url",
        user_config.se_notifications_zoom_token: "zoom_token_123"
    }
    
    # This should cover lines 173-177 (complete zoom configuration path)
    reader.set_notification_param(notification_zoom_complete)
    
    # Verify zoom configuration was set
    mock_context.set_enable_zoom.assert_called_with(True)
    mock_context.set_zoom_webhook_url.assert_called_with("https://zoom.webhook.url")
    mock_context.set_zoom_token.assert_called_with("zoom_token_123")
