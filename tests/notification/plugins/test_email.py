from unittest.mock import patch, MagicMock
import pytest
from spark_expectations.core.exceptions import SparkExpectationsEmailException
from spark_expectations.notifications.plugins.email import SparkExpectationsEmailPluginImpl


@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
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


@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
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


@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
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


@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
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

    mock_config_args = {"message": "Test Email Body"}

    with (
        patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp,
        patch("spark_expectations.notifications.plugins.email.MIMEMultipart") as _mock_mltp,
        patch(
            "spark_expectations.notifications.plugins.email.SparkExpectationsEmailPluginImpl._get_smtp_password"
        ) as mock_get_smtp_password,
    ):
        # act
        email_handler.send_notification(_context=_mock_context, _config_args=mock_config_args)

        # assert
        mock_smtp.assert_called_with(_mock_context.get_mail_smtp_server, _mock_context.get_mail_smtp_port)
        mock_smtp().starttls.assert_called()
        mock_get_smtp_password.assert_called_once_with(_mock_context, mock_smtp.return_value)
        mock_smtp().sendmail.assert_called_with(
            _mock_context.get_mail_from,
            [email.strip() for email in _mock_context.get_to_mail.split(",")],
            _mock_mltp().as_string(),
        )
        mock_smtp().quit.assert_called()


@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend", autospec=True, spec_set=True)
@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret", autospec=True, spec_set=True)
def test_get_cerberus_password(_mock_secret_handler, _mock_get_secret):
    email_handler = SparkExpectationsEmailPluginImpl()
    smtp_creds_dict = {
        "se.streaming.secret.type": "cerberus",
        "se.streaming.cerberus.url": "https://xyz.com",
        "se.streaming.cerberus.sdb.path": "app/project/env",
        "spark.expectations.notifications.cerberus.smtp.password": "password_key",
    }
    _mock_get_secret.return_value = {"password_key": "test_password"}
    _mock_secret_handler.get_secret = _mock_get_secret

    password = email_handler._get_cerberus_password(_mock_secret_handler, smtp_creds_dict)
    assert password == "test_password"


@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend", autospec=True, spec_set=True)
@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret", autospec=True, spec_set=True)
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
@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret", autospec=True, spec_set=True)
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
@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret", autospec=True, spec_set=True)
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
    with patch(
        "spark_expectations.notifications.plugins.email.SparkExpectationsEmailPluginImpl._get_cerberus_password"
    ) as mock_get_cerberus_password:
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
    with patch(
        "spark_expectations.notifications.plugins.email.SparkExpectationsEmailPluginImpl._get_databricks_password"
    ) as mock_get_databricks_password:
        email_handler._retrieve_password(_mock_secret_handler, secret_type, smtp_secret_dict)
        mock_get_databricks_password.assert_called_once_with(_mock_secret_handler, smtp_secret_dict)


@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend", autospec=True, spec_set=True)
@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret", autospec=True, spec_set=True)
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
@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret", autospec=True, spec_set=True)
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


@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
def test_get_smtp_password(_mock_context):
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_mail_from = "test@example.com"
    _mock_context.get_mail_smtp_password = "test_password"

    with patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp:
        server = mock_smtp.return_value
        email_handler._get_smtp_password(_mock_context, server)
        server.login.assert_called_once_with("test@example.com", "test_password")


@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
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

    with (
        patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp,
        patch(
            "spark_expectations.notifications.plugins.email.SparkExpectationsEmailPluginImpl._retrieve_password"
        ) as mock_retrieve_password,
    ):
        server = mock_smtp.return_value
        mock_retrieve_password.return_value = "test_password"
        email_handler._get_smtp_password(_mock_context, server)
        server.login.assert_called_once_with("test@example.com", "test_password")


@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
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

    with (
        patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp,
        patch(
            "spark_expectations.notifications.plugins.email.SparkExpectationsEmailPluginImpl._retrieve_password",
            side_effect=KeyError,
        ),
    ):
        server = mock_smtp.return_value

        with pytest.raises(SparkExpectationsEmailException, match="SMTP password key is missing in the secret."):
            email_handler._get_smtp_password(_mock_context, server)


@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
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

    with (
        patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp,
        patch(
            "spark_expectations.notifications.plugins.email.SparkExpectationsEmailPluginImpl._retrieve_password",
            side_effect=Exception("Generic error"),
        ),
    ):
        server = mock_smtp.return_value

        with pytest.raises(SparkExpectationsEmailException, match="Failed to retrieve SMTP password."):
            email_handler._get_smtp_password(_mock_context, server)


@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
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

    with (
        patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp,
        patch(
            "spark_expectations.notifications.plugins.email.SparkExpectationsEmailPluginImpl._retrieve_password"
        ) as mock_retrieve_password,
    ):
        server = mock_smtp.return_value
        mock_retrieve_password.return_value = None

        with pytest.raises(SparkExpectationsEmailException, match="SMTP password is not set."):
            email_handler._get_smtp_password(_mock_context, server)


@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
def test_process_message_plain_text(_mock_context):
    """Test that _process_message returns plain text content when content_type is not html"""
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_templated_basic_email_body = False

    config_args = {"message": "Test message content", "content_type": "plain"}

    mail_content, content_type = email_handler._process_message(_mock_context, config_args)

    assert mail_content == "Test message content"
    assert content_type == "plain"


@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
def test_process_message_html_content(_mock_context):
    """Test that _process_message keeps html content_type when specified"""
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_templated_basic_email_body = False

    config_args = {"message": "<p>Test HTML content</p>", "content_type": "html"}

    mail_content, content_type = email_handler._process_message(_mock_context, config_args)

    assert mail_content == "<p>Test HTML content</p>"
    assert content_type == "html"


@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
def test_process_message_detailed_notification(_mock_context):
    """Test that template is not used when email_notification_type is 'detailed'"""
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_templated_basic_email_body = True
    _mock_context.get_basic_default_template = None

    config_args = {
        "message": "Test detailed message",
        "content_type": "html",
        "email_notification_type": "detailed",
    }

    mail_content, content_type = email_handler._process_message(_mock_context, config_args)

    assert mail_content == "Test detailed message"
    assert content_type == "html"


@patch("spark_expectations.notifications.plugins.email.Environment")
@patch("spark_expectations.notifications.plugins.email.PackageLoader")
@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
def test_process_message_with_template_filesystem(_mock_context, mock_fs_loader, mock_env):
    """Test that template is used when email_notification_type is not 'detailed' and template enabled"""
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_templated_basic_email_body = True
    _mock_context.get_basic_default_template = None
    _mock_context.get_mail_subject = "Test Subject"

    # Setup template mocks
    mock_template = MagicMock()
    mock_template.module.render_table = "render_function"
    mock_template.render.return_value = "<table>Rendered HTML</table>"
    mock_env.return_value.get_template.return_value = mock_template

    # Test message with format similar to your expected format
    test_message = "Title Line\ntable_name: test_table\nrun_id: 12345"

    config_args = {
        "message": test_message,
        "content_type": "plain",
    }

    mail_content, content_type = email_handler._process_message(_mock_context, config_args)

    # Check that the template was used
    mock_fs_loader.assert_called_once_with("spark_expectations", "config/templates")
    mock_env.assert_called_once_with(loader=mock_fs_loader.return_value)
    mock_env.return_value.get_template.assert_called_once_with("basic_email_alert_template.jinja")
    mock_template.render.assert_called_once()

    # Check the resulting content
    assert "<h2>Test Subject</h2>" in mail_content
    assert "<table>Rendered HTML</table>" in mail_content
    assert content_type == "html"

    # Verify the message data was parsed correctly
    call_args = mock_template.render.call_args[1]
    assert call_args["title"] == "Title Line"
    assert call_args["rows"] == [["table_name", " test_table"], ["run_id", " 12345"]]
    assert call_args["render_table"] == "render_function"


@patch("spark_expectations.notifications.plugins.email.Environment")
@patch("spark_expectations.notifications.plugins.email.BaseLoader")
@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
def test_process_message_with_template_baseloader(_mock_context, mock_base_loader, mock_env):
    """Test template processing when a custom template is provided"""
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_templated_basic_email_body = True
    _mock_context.get_basic_default_template = "Custom template string"
    _mock_context.get_mail_subject = "Custom Subject"

    # Setup template mocks
    mock_template = MagicMock()
    mock_template.module.render_table = "render_function"
    mock_template.render.return_value = "<div>Custom Template</div>"
    mock_env.return_value.from_string.return_value = mock_template

    test_message = "Alert\nkey1: value1\nkey2: value2"

    config_args = {
        "message": test_message,
        "content_type": "plain",
    }

    mail_content, content_type = email_handler._process_message(_mock_context, config_args)

    # Check the template was used with BaseLoader
    mock_env.return_value.from_string.assert_called_with("Custom template string")
    mock_template.render.assert_called_once()
    assert "<h2>Custom Subject</h2>" in mail_content
    assert "<div>Custom Template</div>" in mail_content
    assert content_type == "html"

    # Verify message parsing
    call_args = mock_template.render.call_args[1]
    assert call_args["title"] == "Alert"
    assert call_args["rows"] == [["key1", " value1"], ["key2", " value2"]]
    assert call_args["render_table"] == "render_function"


@patch("spark_expectations.notifications.plugins.email.Environment")
@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
def test_process_message_with_empty_message(_mock_context, mock_env):
    """Test that _process_message handles empty messages gracefully"""
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_templated_basic_email_body = True
    _mock_context.get_basic_default_template = "Template string"
    _mock_context.get_mail_subject = "Subject"

    # Setup template mock
    mock_template = MagicMock()
    mock_template.module.render_table = "render_function"
    mock_template.render.return_value = "<p>Empty Content</p>"
    mock_env.return_value.from_string.return_value = mock_template

    # Test with empty message
    config_args = {
        "message": "",
        "content_type": "plain",
    }

    mail_content, content_type = email_handler._process_message(_mock_context, config_args)

    # Check that template was rendered with empty title and rows
    call_args = mock_template.render.call_args[1]
    assert call_args["title"] == ""
    assert call_args["rows"] == []
    assert content_type == "html"


@patch("spark_expectations.notifications.plugins.email.Environment")
@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
def test_process_message_with_malformed_data(_mock_context, mock_env):
    """Test that message parsing handles malformed data gracefully"""
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_templated_basic_email_body = True
    _mock_context.get_basic_default_template = "Template string"
    _mock_context.get_mail_subject = "Subject"

    # Setup template mock
    mock_template = MagicMock()
    mock_template.module.render_table = "render_function"
    mock_template.render.return_value = "<p>Rendered Content</p>"
    mock_env.return_value.from_string.return_value = mock_template

    # Test with malformed message (no key-value pairs)
    test_message = "Just a single line with no key-value pairs"

    config_args = {
        "message": test_message,
        "content_type": "plain",
    }

    mail_content, content_type = email_handler._process_message(_mock_context, config_args)

    # Check that title was extracted and empty rows were passed
    call_args = mock_template.render.call_args[1]
    assert call_args["title"] == "Just a single line with no key-value pairs"
    assert call_args["rows"] == []


@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
def test_process_message_no_content_type(_mock_context):
    """Test that _process_message defaults to plain text when no content_type is provided"""
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_templated_basic_email_body = False

    # No content_type provided
    config_args = {"message": "Test message without content type"}

    mail_content, content_type = email_handler._process_message(_mock_context, config_args)

    assert mail_content == "Test message without content type"
    assert content_type == "plain"


# test default template when type is custom
@patch("spark_expectations.notifications.plugins.email.Environment")
@patch("spark_expectations.notifications.plugins.email.PackageLoader")
@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
def test_process_message_with_custom_template(_mock_context, mock_fs_loader, mock_env):
    """Test that default custom template is used"""
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_templated_custom_email = True
    _mock_context.get_custom_default_template = None
    _mock_context.get_mail_subject = "Test Subject"

    # Setup template mocks
    mock_template = MagicMock()
    mock_template.module.render_table = "render_function"
    mock_template.render.return_value = "<table>Rendered HTML</table>"
    mock_env.return_value.get_template.return_value = mock_template

    # Test message with format similar to your expected format
    test_message = 'CUSTOM EMAIL\n{"product_id": "product_id1", "table_name": "test_table"}'

    config_args = {
        "message": test_message,
        "content_type": "plain",
    }

    mail_content, content_type = email_handler._process_message(_mock_context, config_args)

    # Check that the template was used
    mock_fs_loader.assert_called_once_with("spark_expectations","config/templates")
    mock_env.assert_called_once_with(loader=mock_fs_loader.return_value)
    mock_env.return_value.get_template.assert_called_once_with("custom_email_alert_template.jinja")
    mock_template.render.assert_called_once()

    # Check the resulting content
    assert "<table>Rendered HTML</table>" in mail_content
    assert content_type == "html"

    # Verify the message data was parsed correctly
    call_args = mock_template.render.call_args[0]
    assert call_args[0] == {'product_id': 'product_id1', 'table_name': 'test_table'}


# test template from user config when type is custom
@patch("spark_expectations.notifications.plugins.email.Environment")
@patch("spark_expectations.notifications.plugins.email.BaseLoader")
@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
def test_process_message_with_custom_template(_mock_context, mock_base_loader, mock_env):
    """Test custom email template processing when a template is provided in the user config"""
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_templated_custom_email = True
    _mock_context.get_custom_default_template = "Custom template"
    _mock_context.get_mail_subject = "Test Subject"

    # Setup template mocks
    mock_template = MagicMock()
    mock_template.module.render_table = "render_function"
    mock_template.render.return_value = "<div>Custom Template</div>"
    mock_env.return_value.from_string.return_value = mock_template

    # Test message with format similar to your expected format
    test_message = 'CUSTOM EMAIL\n{"product_id": "product_id1", "table_name": "test_table"}'

    config_args = {
        "message": test_message,
        "content_type": "plain",
    }

    mail_content, content_type = email_handler._process_message(_mock_context, config_args)

    # Check the template was used with BaseLoader
    mock_env.return_value.from_string.assert_called_with("Custom template")
    mock_template.render.assert_called_once()

    # Check the resulting content
    assert "<div>Custom Template</div>" in mail_content
    assert content_type == "html"

    # Verify the message data was parsed correctly
    call_args = mock_template.render.call_args[0]
    assert call_args[0] == {'product_id': 'product_id1', 'table_name': 'test_table'}

# test custom email throws json error
@patch("spark_expectations.notifications.plugins.email.Environment")
@patch("spark_expectations.notifications.plugins.email.PackageLoader")
@patch("spark_expectations.notifications.plugins.email._log")
def test_process_message_invalid_json_logs_and_fallback(mock_log, mock_fs_loader, mock_env):
    email_handler = SparkExpectationsEmailPluginImpl()
    context = MagicMock()
    context.get_enable_templated_custom_email = True
    context.get_custom_default_template = None

    # Simulate invalid JSON in mail_content
    config_args = {
        "message": "CUSTOM EMAIL\n{'invalid': unquoted_value}",  # Not valid JSON
        "content_type": "plain"
    }

    mail_content, content_type = email_handler._process_message(context, config_args)

    # Check that the error was logged
    assert mock_log.error.call_count == 1
    assert "JSON decode error" in mock_log.error.call_args[0][0]

    # Check that the returned content is a plain text error message
    assert mail_content.startswith("Error: Invalid JSON format in custom email content.")
    assert "Original content:" in mail_content
    assert content_type == "plain"

# test custom email template rendering throws error
@patch("spark_expectations.notifications.plugins.email.Environment")
@patch("spark_expectations.notifications.plugins.email.PackageLoader")
@patch("spark_expectations.notifications.plugins.email._log")
def test_process_message_template_render_exception(mock_log, mock_fs_loader, mock_env):
    email_handler = SparkExpectationsEmailPluginImpl()
    context = MagicMock()
    context.get_enable_templated_custom_email = True
    context.get_custom_default_template = None

    # Valid JSON, but template.render will raise a general Exception
    config_args = {
        "message": 'CUSTOM EMAIL\n{"product_id": "product_id1", "table_name": "test_table"}',
        "content_type": "plain"
    }

    # Setup template mock to raise Exception on render
    mock_template = MagicMock()
    mock_template.render.side_effect = Exception("Render error")
    mock_env.return_value.get_template.return_value = mock_template

    mail_content, content_type = email_handler._process_message(context, config_args)

    # Check that the error was logged
    assert mock_log.error.call_count >= 1
    assert "Template rendering error" in mock_log.error.call_args[0][0]

    # Check that the returned content is a plain text error message
    assert mail_content.startswith("Error: Failed to render custom email template.")
    assert "Original content:" in mail_content
    assert content_type == "plain"
