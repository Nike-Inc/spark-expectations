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
def test_get_smtp_password_cerberus(_mock_context):
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_secret_type = "cerberus"
    _mock_context.get_mail_from = "test@example.com"
    _mock_context.get_se_streaming_stats_dict = {
        "secret_type": "cerberus"
    }
    _mock_context.get_cbs_sdb_path = "path/to/secret"
    _mock_context.get_smtp_password_key = "password_key"
    _mock_context.get_cerberus_url = "https://cerberus.example.com"

    with patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp, \
            patch("spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret") as mock_get_secret:
        mock_get_secret.return_value = {
            "password_key": "test_password"
        }
        server = mock_smtp.return_value
        email_handler._get_smtp_password(_mock_context, server)
        server.login.assert_called_once_with("test@example.com", "test_password")


@patch('spark_expectations.notifications.plugins.email.SparkExpectationsContext', autospec=True, spec_set=True)
def test_get_smtp_password_none_exception(_mock_context):
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_secret_type = "cerberus"
    _mock_context.get_mail_from = "test@example.com"
    _mock_context.get_se_streaming_stats_dict = {
        "secret_type": "cerberus"
    }
    _mock_context.get_cbs_sdb_path = "path/to/secret"
    _mock_context.get_smtp_password_key = "password_key"
    _mock_context.get_cerberus_url = "https://cerberus.example.com"

    with patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp, \
            patch("spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret") as mock_get_secret:
        mock_get_secret.return_value = {}  # Mocking the return value as an empty dictionary
        server = mock_smtp.return_value

        with pytest.raises(SparkExpectationsEmailException, match="SMTP password is not set."):
            email_handler._get_smtp_password(_mock_context, server)

@patch('spark_expectations.notifications.plugins.email.SparkExpectationsContext', autospec=True, spec_set=True)
def test_get_smtp_password_databricks(_mock_context):
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_secret_type = "databricks"
    _mock_context.get_mail_from = "test@example.com"
    _mock_context.get_se_streaming_stats_dict = {
        "secret_type": "databricks"
    }
    _mock_context.get_smtp_password_key = "password_key"

    with patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp, \
            patch("spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret") as mock_get_secret:
        mock_get_secret.return_value = "test_password"
        server = mock_smtp.return_value

        email_handler._get_smtp_password(_mock_context, server)
        server.login.assert_called_once_with("test@example.com", "test_password")


@patch('spark_expectations.notifications.plugins.email.SparkExpectationsContext', autospec=True, spec_set=True)
def test_get_smtp_password_databricks_missing_key(_mock_context):
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_secret_type = "databricks"
    _mock_context.get_mail_from = "test@example.com"
    _mock_context.get_se_streaming_stats_dict = {
        "secret_type": "databricks"
    }
    _mock_context.get_smtp_password_key = None

    with patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp:
        server = mock_smtp.return_value

        # Assert that the SparkExpectationsEmailException is raised for missing key
        with pytest.raises(SparkExpectationsEmailException, match="SMTP password is not set."):
            email_handler._get_smtp_password(_mock_context, server)


@patch('spark_expectations.notifications.plugins.email.SparkExpectationsContext', autospec=True, spec_set=True)
def test_get_smtp_password_key_error(_mock_context):
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_secret_type = "cerberus"
    _mock_context.get_mail_from = "test@example.com"
    _mock_context.get_se_streaming_stats_dict = {
        "secret_type": "cerberus"
    }
    _mock_context.get_cbs_sdb_path = "path/to/secret"
    _mock_context.get_smtp_password_key = "password_key"

    with patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp, \
            patch("spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret", side_effect=KeyError):
        server = mock_smtp.return_value

        # Assert that the SparkExpectationsEmailException is raised for KeyError
        with pytest.raises(SparkExpectationsEmailException, match="SMTP password key is missing in the secret."):
            email_handler._get_smtp_password(_mock_context, server)


@patch('spark_expectations.notifications.plugins.email.SparkExpectationsContext', autospec=True, spec_set=True)
def test_get_smtp_password_generic_exception(_mock_context):
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_secret_type = "cerberus"
    _mock_context.get_mail_from = "test@example.com"
    _mock_context.get_se_streaming_stats_dict = {
        "secret_type": "cerberus"
    }
    _mock_context.get_cbs_sdb_path = "path/to/secret"
    _mock_context.get_smtp_password_key = "password_key"

    with patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp, \
            patch("spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret", side_effect=Exception("Test Exception")):
        server = mock_smtp.return_value

        # Assert that the SparkExpectationsEmailException is raised for a generic exception
        with pytest.raises(SparkExpectationsEmailException, match="Failed to retrieve SMTP password."):
            email_handler._get_smtp_password(_mock_context, server)

@patch('spark_expectations.notifications.plugins.email.SparkExpectationsContext', autospec=True, spec_set=True)
def test_get_smtp_password_cerberus_password_none(_mock_context):
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_secret_type = "cerberus"
    _mock_context.get_mail_from = "test@example.com"
    _mock_context.get_se_streaming_stats_dict = {
        "secret_type": "cerberus"
    }
    _mock_context.get_cbs_sdb_path = "path/to/secret"
    _mock_context.get_smtp_password_key = "password_key"

    with patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp, \
            patch("spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret", return_value="not_a_dict"):
        server = mock_smtp.return_value

        # Assert that the SparkExpectationsEmailException is raised for password being None
        with pytest.raises(SparkExpectationsEmailException, match="SMTP password is not set."):
            email_handler._get_smtp_password(_mock_context, server)

@patch('spark_expectations.notifications.plugins.email.SparkExpectationsContext', autospec=True, spec_set=True)
def test_get_smtp_password_databricks_password_none(_mock_context):
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_secret_type = "databricks"
    _mock_context.get_mail_from = "test@example.com"
    _mock_context.get_se_streaming_stats_dict = {
        "secret_type": "databricks"
    }
    _mock_context.get_smtp_password_key = None

    with patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp:
        server = mock_smtp.return_value

        # Assert that the SparkExpectationsEmailException is raised for password being None
        with pytest.raises(SparkExpectationsEmailException, match="SMTP password is not set."):
            email_handler._get_smtp_password(_mock_context, server)

@patch('spark_expectations.notifications.plugins.email.SparkExpectationsContext', autospec=True, spec_set=True)
def test_get_smtp_password_secret_type_else(_mock_context):
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_secret_type = "unknown"
    _mock_context.get_mail_from = "test@example.com"
    _mock_context.get_se_streaming_stats_dict = {
        "secret_type": "unknown"
    }

    with patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp:
        server = mock_smtp.return_value

        # Assert that the SparkExpectationsEmailException is raised for password being None
        with pytest.raises(SparkExpectationsEmailException, match="SMTP password is not set."):
            email_handler._get_smtp_password(_mock_context, server)


@patch('spark_expectations.notifications.plugins.email.SparkExpectationsContext', autospec=True, spec_set=True)
@patch.object(SparkExpectationsEmailPluginImpl, '_get_smtp_password')
def test_send_notification_with_smtp_auth(mock_get_smtp_password, _mock_context):
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
            patch("spark_expectations.notifications.plugins.email.MIMEMultipart") as _mock_mltp:
        # act
        email_handler.send_notification(_context=_mock_context, _config_args=mock_config_args)

        # assert
        mock_smtp.assert_called_with(_mock_context.get_mail_smtp_server, _mock_context.get_mail_smtp_port)
        mock_smtp().starttls.assert_called()
        mock_get_smtp_password.assert_called_once_with(_mock_context, mock_smtp())
        mock_smtp().sendmail.assert_called_with(_mock_context.get_mail_from, [email.strip() for email in _mock_context.get_to_mail.split(",")],
                                                _mock_mltp().as_string())
        mock_smtp().quit.assert_called()