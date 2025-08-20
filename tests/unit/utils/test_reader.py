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
            },
            SparkExpectationsMiscException,
        ),
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
    else:
        with pytest.raises(
            expected_result,
            match=r"All params/variables required for [a-z]+ notification "
            "is not configured or supplied|error occurred while reading "
            "notification configurations SMTP password is not set or secret dict for its retrieval is not provided|"
            "error occurred while reading notification configurations SMTP creds dict contains non-string keys or values",
        ):
            reader_handler.set_notification_param(notification)


def test_set_notification_param_exception(_fixture_reader):
    with pytest.raises(
        SparkExpectationsMiscException, match=r"error occurred while reading notification configurations .*"
    ):
        _fixture_reader.set_notification_param(["a", "b", "c"])


def test_get_rules_dlt_exception(_fixture_reader):
    with pytest.raises(SparkExpectationsMiscException, match=r"error occurred while retrieving rules list .*"):
        _fixture_reader.get_rules_from_df("product_rules_1", "table1", is_dlt=True, tag=None)


def test_get_rules_from_table_exception(_fixture_reader):
    with pytest.raises(SparkExpectationsMiscException, match=r"error occurred while retrieving rules list .*"):
        _fixture_reader.get_rules_from_df(
            "mock_rules_table_1",
            "table1",
        )
