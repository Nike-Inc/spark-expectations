from unittest.mock import patch
import pytest
from spark_expectations.core.exceptions import SparkExpectationsEmailException
from spark_expectations.notifications.plugins.email import SparkExpectationsEmailPluginImpl


@patch('spark_expectations.notifications.plugins.email.SparkExpectationsContext', autospec=True, spec_set=True)
def test_send_notification_success(_mock_context):
    # arrange
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_mail = True
    _mock_context.get_mail_from = "sender@example.com"
    _mock_context.get_to_mail = "receiver1@example.com, receiver2@example.com"
    _mock_context.get_mail_subject = "Test Email"
    _mock_context.get_mail_smtp_server = "mailhost.example.com"
    _mock_context.get_mail_smtp_port = 587

    mock_config_args = {
        "message": "Test Email Body"
    }

    with patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp, \
            patch("spark_expectations.notifications.plugins.email.MIMEMultipart") as _mock_mltp:
        # act
        email_handler.send_notification(_context=_mock_context, _config_args=mock_config_args)

        # assert
        mock_smtp.assert_called_with(_mock_context.get_mail_smtp_server, _mock_context.get_mail_smtp_port)
        mock_smtp().starttls.assert_called()
        mock_smtp().sendmail.assert_called_with(_mock_context.get_mail_from, [email.strip() for email in _mock_context.get_to_mail.split(",")],
                                                _mock_mltp().as_string())
        mock_smtp().quit.assert_called()


@patch('spark_expectations.notifications.plugins.email.SparkExpectationsContext', autospec=True, spec_set=True)
def test_send_notification_disable_mail(_mock_context):
    # arrange
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_mail = False

    mock_config_args = {
        "message": "Test Email Body"
    }

    with patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp:
        # act
        email_handler.send_notification(_mock_context, mock_config_args)

        # assert
        mock_smtp.assert_not_called()


@patch('spark_expectations.notifications.plugins.email.SparkExpectationsContext', autospec=True, spec_set=True)
def test_send_notification_exception(_mock_context):
    # arrange
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_mail = True
    _mock_context.get_mail_from = "sender@example.com"
    _mock_context.get_to_mail = "receiver@example.com"
    _mock_context.get_mail_subject = "Test Email"
    _mock_context.get_mail_smtp_server = "mailhost.example.com"
    _mock_context.get_mail_smtp_port = 587

    mock_config_args = {
        "message": "Test Email Body"
    }

    with patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp, \
            pytest.raises(SparkExpectationsEmailException):
        mock_smtp.side_effect = Exception("Test Exception")
        # act
        email_handler.send_notification(_mock_context, mock_config_args)

        # assert
        mock_smtp.assert_called_with(_mock_context.get_mail_smtp_server, _mock_context.get_mail_smtp_port)


@patch('spark_expectations.notifications.plugins.email.SparkExpectationsContext', autospec=True, spec_set=True)
def test_send_notification_with_smtp_auth(_mock_context):
    # arrange
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_mail = True
    _mock_context.get_mail_from = "sender@example.com"
    _mock_context.get_to_mail = "receiver1@example.com, receiver2@example.com"
    _mock_context.get_mail_subject = "Test Email"
    _mock_context.get_mail_smtp_server = "mailhost.example.com"
    _mock_context.get_mail_smtp_port = 587
    _mock_context.get_enable_smtp_server_auth = True

    mock_config_args = {
        "message": "Test Email Body"
    }

    with patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp, \
            patch("spark_expectations.notifications.plugins.email.MIMEMultipart") as _mock_mltp, \
            patch("spark_expectations.notifications.plugins.email.SparkExpectationsEmailPluginImpl._get_smtp_password") as mock_get_smtp_password:
        # act
        email_handler.send_notification(_context=_mock_context, _config_args=mock_config_args)

        # assert
        mock_smtp.assert_called_with(_mock_context.get_mail_smtp_server, _mock_context.get_mail_smtp_port)
        mock_smtp().starttls.assert_called()
        mock_get_smtp_password.assert_called_once_with(_mock_context, mock_smtp.return_value)
        mock_smtp().sendmail.assert_called_with(_mock_context.get_mail_from, [email.strip() for email in _mock_context.get_to_mail.split(",")],
                                                _mock_mltp().as_string())
        mock_smtp().quit.assert_called()


@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend", autospec=True, spec_set=True)
@patch('spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret', autospec=True, spec_set=True)
def test_get_cerberus_password(_mock_secret_handler, _mock_get_secret):
    email_handler = SparkExpectationsEmailPluginImpl()
    smtp_creds_dict = {
        "se.streaming.secret.type": "cerberus",
        "se.streaming.cerberus.url": "https://xyz.com",
        "se.streaming.cerberus.sdb.path": "app/project/env",
        "spark.expectations.notifications.cerberus.smtp.password": "password_key",
    }
    _mock_get_secret.return_value = {
        "password_key": "test_password"
    }
    _mock_secret_handler.get_secret = _mock_get_secret

    password = email_handler._get_cerberus_password(_mock_secret_handler, smtp_creds_dict)
    assert password == "test_password"


@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend", autospec=True, spec_set=True)
@patch('spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret', autospec=True, spec_set=True)
def test_get_cerberus_password_none(_mock_secret_handler, _mock_get_secret):
    email_handler = SparkExpectationsEmailPluginImpl()
    smtp_creds_dict = {
        "se.streaming.secret.type": "cerberus",
        "se.streaming.cerberus.url": "https://xyz.com",
        "se.streaming.cerberus.sdb.path": "app/project/env",
        "spark.expectations.notifications.cerberus.smtp.password": None,
    }
    _mock_get_secret.return_value = None
    _mock_secret_handler.get_secret = _mock_get_secret

    password = email_handler._get_cerberus_password(_mock_secret_handler, smtp_creds_dict)
    assert password is None

@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend", autospec=True, spec_set=True)
@patch('spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret', autospec=True, spec_set=True)
def test_get_databricks_password(_mock_secret_handler, _mock_get_secret):
    email_handler = SparkExpectationsEmailPluginImpl()
    smtp_creds_dict = {
        "se.streaming.secret.type": "databricks",
        "se.streaming.dbx.workspace.url": "https://xyz.databricks.com",
        "se.streaming.dbx.secret.scope": "my_secret_scope",
        "spark.expectations.notifications.dbx.smtp.password": "password_key",
    }
    _mock_get_secret.return_value = "test_password"
    _mock_secret_handler.get_secret = _mock_get_secret

    password = email_handler._get_databricks_password(_mock_secret_handler, smtp_creds_dict)
    assert password == "test_password"


@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend", autospec=True, spec_set=True)
@patch('spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret', autospec=True, spec_set=True)
def test_get_databricks_password_none(_mock_secret_handler, _mock_get_secret):
    email_handler = SparkExpectationsEmailPluginImpl()
    smtp_creds_dict = {
        "se.streaming.secret.type": "databricks",
        "se.streaming.dbx.workspace.url": "https://xyz.databricks.com",
        "se.streaming.dbx.secret.scope": "my_secret_scope",
        # Intentionally omitting the key to cover the last line
    }
    _mock_get_secret.return_value = None
    _mock_secret_handler.get_secret = _mock_get_secret

    password = email_handler._get_databricks_password(_mock_secret_handler, smtp_creds_dict)
    assert password is None

@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend", autospec=True, spec_set=True)
def test_retrieve_password_cerberus(_mock_secret_handler):
    email_handler = SparkExpectationsEmailPluginImpl()
    secret_type = "cerberus"
    smtp_secret_dict = {
        "se.streaming.secret.type": "cerberus",
        "se.streaming.cerberus.url": "https://xyz.com",
        "se.streaming.cerberus.sdb.path": "app/project/env",
        "spark.expectations.notifications.cerberus.smtp.password": "password_key",
    }
    with patch("spark_expectations.notifications.plugins.email.SparkExpectationsEmailPluginImpl._get_cerberus_password") as mock_get_cerberus_password:
        email_handler._retrieve_password(_mock_secret_handler, secret_type, smtp_secret_dict)
        mock_get_cerberus_password.assert_called_once_with(_mock_secret_handler, smtp_secret_dict)


@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend", autospec=True, spec_set=True)
def test_retrieve_password_databricks(_mock_secret_handler):
    email_handler = SparkExpectationsEmailPluginImpl()
    secret_type = "databricks"
    smtp_secret_dict = {
        "se.streaming.secret.type": "databricks",
        "se.streaming.dbx.workspace.url": "https://xyz.databricks.com",
        "se.streaming.dbx.secret.scope": "my_secret_scope",
        "spark.expectations.notifications.dbx.smtp.password": "password_key",
    }
    with patch("spark_expectations.notifications.plugins.email.SparkExpectationsEmailPluginImpl._get_databricks_password") as mock_get_databricks_password:
        email_handler._retrieve_password(_mock_secret_handler, secret_type, smtp_secret_dict)
        mock_get_databricks_password.assert_called_once_with(_mock_secret_handler, smtp_secret_dict)


@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend", autospec=True, spec_set=True)
@patch('spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret', autospec=True, spec_set=True)
def test_get_databricks_password(_mock_secret_handler, _mock_get_secret):
    email_handler = SparkExpectationsEmailPluginImpl()
    smtp_creds_dict = {
        "se.streaming.secret.type": "databricks",
        "se.streaming.dbx.workspace.url": "https://xyz.databricks.com",
        "se.streaming.dbx.secret.scope": "my_secret_scope",
        "spark.expectations.notifications.dbx.smtp.password": "password_key",
    }
    _mock_get_secret.return_value = "test_password"
    _mock_secret_handler.get_secret = _mock_get_secret

    password = email_handler._get_databricks_password(_mock_secret_handler, smtp_creds_dict)
    assert password == "test_password"


@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend", autospec=True, spec_set=True)
@patch('spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret', autospec=True, spec_set=True)
def test_get_databricks_password_none(_mock_secret_handler, _mock_get_secret):
    email_handler = SparkExpectationsEmailPluginImpl()
    smtp_creds_dict = {
        "se.streaming.secret.type": "databricks",
        "se.streaming.dbx.workspace.url": "https://xyz.databricks.com",
        "se.streaming.dbx.secret.scope": "my_secret_scope",
        # Intentionally omitting the key to cover the last line
    }
    _mock_get_secret.return_value = None
    _mock_secret_handler.get_secret = _mock_get_secret

    password = email_handler._get_databricks_password(_mock_secret_handler, smtp_creds_dict)
    assert password is None


@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend", autospec=True, spec_set=True)
def test_retrieve_password_none(_mock_secret_handler):
    email_handler = SparkExpectationsEmailPluginImpl()
    secret_type = "not_databricks"
    smtp_secret_dict = {
        "se.streaming.secret.type": "not_databricks",
        "se.streaming.dbx.workspace.url": "https://xyz.databricks.com",
        "se.streaming.dbx.secret.scope": "my_secret_scope",
        "spark.expectations.notifications.dbx.smtp.password": "password_key",
    }

    password = email_handler._retrieve_password(_mock_secret_handler, secret_type, smtp_secret_dict)
    assert password is None



@patch('spark_expectations.notifications.plugins.email.SparkExpectationsContext', autospec=True, spec_set=True)
def test_get_smtp_password(_mock_context):
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_mail_from = "test@example.com"
    _mock_context.get_mail_smtp_password = "test_password"

    with patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp:
        server = mock_smtp.return_value
        email_handler._get_smtp_password(_mock_context, server)
        server.login.assert_called_once_with("test@example.com", "test_password")


@patch('spark_expectations.notifications.plugins.email.SparkExpectationsContext', autospec=True, spec_set=True)
def test_get_smtp_password_with_retrieve_method(_mock_context):
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_mail_from = "test@example.com"
    _mock_context.get_mail_smtp_password = None
    _mock_context.get_smtp_creds_dict = {
        "se.streaming.secret.type": "databricks",
        "se.streaming.dbx.workspace.url": "https://xyz.databricks.com",
        "se.streaming.dbx.secret.scope": "my_secret_scope",
        "spark.expectations.notifications.dbx.smtp.password": "password_key",
    }

    with patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp,\
            patch("spark_expectations.notifications.plugins.email.SparkExpectationsEmailPluginImpl._retrieve_password") as mock_retrieve_password:
        server = mock_smtp.return_value
        mock_retrieve_password.return_value = "test_password"
        email_handler._get_smtp_password(_mock_context, server)
        server.login.assert_called_once_with("test@example.com", "test_password")

@patch('spark_expectations.notifications.plugins.email.SparkExpectationsContext', autospec=True, spec_set=True)
def test_get_smtp_password_missing_key(_mock_context):
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_mail_from = "test@example.com"
    _mock_context.get_mail_smtp_password = None
    _mock_context.get_smtp_creds_dict = {
        "se.streaming.secret.type": "databricks",
        "se.streaming.dbx.workspace.url": "https://xyz.databricks.com",
        "se.streaming.dbx.secret.scope": "my_secret_scope",
        "spark.expectations.notifications.dbx.smtp.password": None,
    }

    with patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp, \
            patch("spark_expectations.notifications.plugins.email.SparkExpectationsEmailPluginImpl._retrieve_password", side_effect=KeyError):
        server = mock_smtp.return_value

        with pytest.raises(SparkExpectationsEmailException, match="SMTP password key is missing in the secret."):
            email_handler._get_smtp_password(_mock_context, server)

@patch('spark_expectations.notifications.plugins.email.SparkExpectationsContext', autospec=True, spec_set=True)
def test_get_smtp_password_failed_retrieve(_mock_context):
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_mail_from = "test@example.com"
    _mock_context.get_mail_smtp_password = None
    _mock_context.get_smtp_creds_dict = {
        "se.streaming.secret.type": "databricks",
        "se.streaming.dbx.workspace.url": "https://xyz.databricks.com",
        "se.streaming.dbx.secret.scope": "my_secret_scope",
        "spark.expectations.notifications.dbx.smtp.password": "password_key",
    }

    with patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp, \
            patch("spark_expectations.notifications.plugins.email.SparkExpectationsEmailPluginImpl._retrieve_password", side_effect=Exception("Generic error")):
        server = mock_smtp.return_value

        with pytest.raises(SparkExpectationsEmailException, match="Failed to retrieve SMTP password."):
            email_handler._get_smtp_password(_mock_context, server)


@patch('spark_expectations.notifications.plugins.email.SparkExpectationsContext', autospec=True, spec_set=True)
def test_get_smtp_password_none_exception(_mock_context):
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_mail_from = "test@example.com"
    _mock_context.get_mail_smtp_password = None
    _mock_context.get_smtp_creds_dict = {
        "se.streaming.secret.type": "databricks",
        "se.streaming.dbx.workspace.url": "https://xyz.databricks.com",
        "se.streaming.dbx.secret.scope": "my_secret_scope",
        "spark.expectations.notifications.dbx.smtp.password": "password_key",
    }

    with patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp, \
            patch("spark_expectations.notifications.plugins.email.SparkExpectationsEmailPluginImpl._retrieve_password") as mock_retrieve_password:
        server = mock_smtp.return_value
        mock_retrieve_password.return_value = None

        with pytest.raises(SparkExpectationsEmailException, match="SMTP password is not set."):
            email_handler._get_smtp_password(_mock_context, server)

