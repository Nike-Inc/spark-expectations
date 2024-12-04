from unittest.mock import patch

import pytest

from spark_expectations.core.exceptions import SparkExpectationsEmailException
from spark_expectations.notifications.plugins.email import (
    SparkExpectationsEmailPluginImpl,
)


@patch(
    "spark_expectations.notifications.plugins.email.SparkExpectationsContext",
    autospec=True,
    spec_set=True,
)
def test_send_notification_success(_mock_context):
    # arrange
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_mail = True
    _mock_context.get_mail_from = "sender@example.com"
    _mock_context.get_to_mail = "receiver1@example.com, receiver2@example.com"
    _mock_context.get_mail_subject = "Test Email"
    _mock_context.get_mail_smtp_server = "mailhost.example.com"
    _mock_context.get_mail_smtp_port = 587

    mock_config_args = {"message": "Test Email Body"}

    with (
        patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp,
        patch("spark_expectations.notifications.plugins.email.MIMEMultipart") as _mock_mltp,
    ):
        # act
        email_handler.send_notification(_context=_mock_context, _config_args=mock_config_args)

        # assert
        mock_smtp.assert_called_with(_mock_context.get_mail_smtp_server, _mock_context.get_mail_smtp_port)
        mock_smtp().starttls.assert_called()
        mock_smtp().sendmail.assert_called_with(
            _mock_context.get_mail_from,
            [email.strip() for email in _mock_context.get_to_mail.split(",")],
            _mock_mltp().as_string(),
        )
        mock_smtp().quit.assert_called()


@patch(
    "spark_expectations.notifications.plugins.email.SparkExpectationsContext",
    autospec=True,
    spec_set=True,
)
def test_send_notification_disable_mail(_mock_context):
    # arrange
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_mail = False

    mock_config_args = {"message": "Test Email Body"}

    with patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp:
        # act
        email_handler.send_notification(_mock_context, mock_config_args)

        # assert
        mock_smtp.assert_not_called()


@patch(
    "spark_expectations.notifications.plugins.email.SparkExpectationsContext",
    autospec=True,
    spec_set=True,
)
def test_send_notification_exception(_mock_context):
    # arrange
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_mail = True
    _mock_context.get_mail_from = "sender@example.com"
    _mock_context.get_to_mail = "receiver@example.com"
    _mock_context.get_mail_subject = "Test Email"
    _mock_context.get_mail_smtp_server = "mailhost.example.com"
    _mock_context.get_mail_smtp_port = 587

    mock_config_args = {"message": "Test Email Body"}

    with (
        patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp,
        pytest.raises(SparkExpectationsEmailException),
    ):
        mock_smtp.side_effect = Exception("Test Exception")
        # act
        email_handler.send_notification(_mock_context, mock_config_args)

        # assert
        mock_smtp.assert_called_with(_mock_context.get_mail_smtp_server, _mock_context.get_mail_smtp_port)
