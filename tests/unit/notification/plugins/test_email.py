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


# =============================================================================
# Retry Logic Tests
# =============================================================================


@patch("spark_expectations.notifications.plugins.email.time.sleep")
@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
def test_send_notification_retry_success_on_second_attempt(_mock_context, mock_sleep):
    """Test that send_notification succeeds on retry after first failure"""
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_mail = True
    _mock_context.get_mail_from = "sender@example.com"
    _mock_context.get_to_mail = "receiver@example.com"
    _mock_context.get_mail_subject = "Test Email"
    _mock_context.get_mail_smtp_server = "mailhost.example.com"
    _mock_context.get_mail_smtp_port = 587
    _mock_context.get_enable_smtp_server_auth = False

    mock_config_args = {"message": "Test Email Body"}

    with (
        patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp,
        patch("spark_expectations.notifications.plugins.email.MIMEMultipart") as _mock_mltp,
        patch("spark_expectations.notifications.plugins.email._log") as mock_log,
    ):
        # First call fails, second succeeds
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server
        mock_server.sendmail.side_effect = [Exception("Temporary failure"), None]

        # act - disable jitter for predictable testing
        email_handler.send_notification(
            _context=_mock_context,
            _config_args=mock_config_args,
            max_retries=3,
            base_retry_delay=0.1,
            enable_jitter=False,
        )

        # assert - should have called SMTP twice (first failed, second succeeded)
        assert mock_smtp.call_count == 2
        assert mock_server.sendmail.call_count == 2
        mock_sleep.assert_called_once()  # Should have slept once between retries
        mock_log.warning.assert_called()  # Should have logged the retry warning


@patch("spark_expectations.notifications.plugins.email.time.sleep")
@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
def test_send_notification_retry_all_attempts_exhausted(_mock_context, mock_sleep):
    """Test that exception is raised after all retry attempts are exhausted"""
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_mail = True
    _mock_context.get_mail_from = "sender@example.com"
    _mock_context.get_to_mail = "receiver@example.com"
    _mock_context.get_mail_subject = "Test Email"
    _mock_context.get_mail_smtp_server = "mailhost.example.com"
    _mock_context.get_mail_smtp_port = 587
    _mock_context.get_enable_smtp_server_auth = False

    mock_config_args = {"message": "Test Email Body"}

    with (
        patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp,
        patch("spark_expectations.notifications.plugins.email._log") as mock_log,
        pytest.raises(SparkExpectationsEmailException) as exc_info,
    ):
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server
        mock_server.sendmail.side_effect = Exception("Persistent failure")

        # act - disable jitter for predictable testing
        email_handler.send_notification(
            _context=_mock_context,
            _config_args=mock_config_args,
            max_retries=3,
            base_retry_delay=0.1,
            enable_jitter=False,
        )

    # assert
    assert "after 3 attempts" in str(exc_info.value)
    assert "Persistent failure" in str(exc_info.value)
    assert mock_smtp.call_count == 3
    assert mock_sleep.call_count == 2  # Sleep between attempts 1-2 and 2-3
    mock_log.error.assert_called()  # Should have logged the final error


@patch("spark_expectations.notifications.plugins.email.time.sleep")
@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
def test_send_notification_retry_concurrent_connection_error_exponential_backoff(_mock_context, mock_sleep):
    """Test that concurrent connection error (432) uses exponential backoff"""
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_mail = True
    _mock_context.get_mail_from = "sender@example.com"
    _mock_context.get_to_mail = "receiver@example.com"
    _mock_context.get_mail_subject = "Test Email"
    _mock_context.get_mail_smtp_server = "smtp.outlook.com"
    _mock_context.get_mail_smtp_port = 587
    _mock_context.get_enable_smtp_server_auth = False

    mock_config_args = {"message": "Test Email Body"}

    with (
        patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp,
        pytest.raises(SparkExpectationsEmailException),
    ):
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server
        # Simulate Microsoft 432 concurrent connection error
        mock_server.sendmail.side_effect = Exception(
            "(432, b'4.3.2 Concurrent connections limit exceeded')"
        )

        # act - disable jitter for predictable testing
        email_handler.send_notification(
            _context=_mock_context,
            _config_args=mock_config_args,
            max_retries=3,
            base_retry_delay=5.0,
            enable_jitter=False,
        )

    # assert - verify exponential backoff delays for 432 error
    # First retry: 5.0 * (2^0) = 5.0
    # Second retry: 5.0 * (2^1) = 10.0
    sleep_calls = [call[0][0] for call in mock_sleep.call_args_list]
    assert sleep_calls[0] == 5.0   # First retry delay
    assert sleep_calls[1] == 10.0  # Second retry delay (exponential)


@patch("spark_expectations.notifications.plugins.email.time.sleep")
@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
def test_send_notification_retry_server_cleanup_on_failure(_mock_context, mock_sleep):
    """Test that server connection is properly closed on failure before retry"""
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_mail = True
    _mock_context.get_mail_from = "sender@example.com"
    _mock_context.get_to_mail = "receiver@example.com"
    _mock_context.get_mail_subject = "Test Email"
    _mock_context.get_mail_smtp_server = "mailhost.example.com"
    _mock_context.get_mail_smtp_port = 587
    _mock_context.get_enable_smtp_server_auth = False

    mock_config_args = {"message": "Test Email Body"}

    with (
        patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp,
        patch("spark_expectations.notifications.plugins.email.MIMEMultipart"),
    ):
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server
        # Fail on first attempt, succeed on second
        mock_server.sendmail.side_effect = [Exception("Temporary failure"), None]

        # act - disable jitter for predictable testing
        email_handler.send_notification(
            _context=_mock_context,
            _config_args=mock_config_args,
            max_retries=3,
            base_retry_delay=0.1,
            enable_jitter=False,
        )

        # assert - server.quit should be called for cleanup after failure and after success
        # First call fails -> quit called in cleanup
        # Second call succeeds -> quit called normally
        assert mock_server.quit.call_count == 2


@patch("spark_expectations.notifications.plugins.email.time.sleep")
@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
def test_send_notification_retry_with_custom_max_retries(_mock_context, mock_sleep):
    """Test that custom max_retries parameter is respected"""
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_mail = True
    _mock_context.get_mail_from = "sender@example.com"
    _mock_context.get_to_mail = "receiver@example.com"
    _mock_context.get_mail_subject = "Test Email"
    _mock_context.get_mail_smtp_server = "mailhost.example.com"
    _mock_context.get_mail_smtp_port = 587
    _mock_context.get_enable_smtp_server_auth = False

    mock_config_args = {"message": "Test Email Body"}

    with (
        patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp,
        pytest.raises(SparkExpectationsEmailException) as exc_info,
    ):
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server
        mock_server.sendmail.side_effect = Exception("Failure")

        # act - use custom max_retries of 5, disable jitter for predictable testing
        email_handler.send_notification(
            _context=_mock_context,
            _config_args=mock_config_args,
            max_retries=5,
            base_retry_delay=0.1,
            enable_jitter=False,
        )

    # assert
    assert "after 5 attempts" in str(exc_info.value)
    assert mock_smtp.call_count == 5
    assert mock_sleep.call_count == 4  # Sleep between each retry


@patch("spark_expectations.notifications.plugins.email.time.sleep")
@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
def test_send_notification_retry_non_rate_limit_error_linear_backoff(_mock_context, mock_sleep):
    """Test that non-rate-limit errors use linear backoff instead of exponential"""
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_mail = True
    _mock_context.get_mail_from = "sender@example.com"
    _mock_context.get_to_mail = "receiver@example.com"
    _mock_context.get_mail_subject = "Test Email"
    _mock_context.get_mail_smtp_server = "mailhost.example.com"
    _mock_context.get_mail_smtp_port = 587
    _mock_context.get_enable_smtp_server_auth = False

    mock_config_args = {"message": "Test Email Body"}

    with (
        patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp,
        pytest.raises(SparkExpectationsEmailException),
    ):
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server
        # Generic error (not 432)
        mock_server.sendmail.side_effect = Exception("Generic SMTP error")

        # act - disable jitter for predictable testing
        email_handler.send_notification(
            _context=_mock_context,
            _config_args=mock_config_args,
            max_retries=3,
            base_retry_delay=2.0,
            enable_jitter=False,
        )

    # assert - verify linear backoff delays for non-432 errors
    # First retry: 2.0 * 1 = 2.0
    # Second retry: 2.0 * 2 = 4.0
    sleep_calls = [call[0][0] for call in mock_sleep.call_args_list]
    assert sleep_calls[0] == 2.0  # First retry delay
    assert sleep_calls[1] == 4.0  # Second retry delay (linear)


@patch("spark_expectations.notifications.plugins.email.time.sleep")
@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
def test_send_notification_retry_logs_warning_on_each_failure(_mock_context, mock_sleep):
    """Test that a warning is logged for each failed attempt except the last"""
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_mail = True
    _mock_context.get_mail_from = "sender@example.com"
    _mock_context.get_to_mail = "receiver@example.com"
    _mock_context.get_mail_subject = "Test Email"
    _mock_context.get_mail_smtp_server = "mailhost.example.com"
    _mock_context.get_mail_smtp_port = 587
    _mock_context.get_enable_smtp_server_auth = False

    mock_config_args = {"message": "Test Email Body"}

    with (
        patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp,
        patch("spark_expectations.notifications.plugins.email._log") as mock_log,
        pytest.raises(SparkExpectationsEmailException),
    ):
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server
        mock_server.sendmail.side_effect = Exception("Test error")

        # act - disable jitter for predictable testing
        email_handler.send_notification(
            _context=_mock_context,
            _config_args=mock_config_args,
            max_retries=3,
            base_retry_delay=0.1,
            enable_jitter=False,
        )

    # assert - 2 warnings for retries, 1 error for final failure
    assert mock_log.warning.call_count == 2
    assert mock_log.error.call_count == 1

    # Verify warning messages contain attempt numbers
    warning_calls = [str(call) for call in mock_log.warning.call_args_list]
    assert any("1/3" in call for call in warning_calls)
    assert any("2/3" in call for call in warning_calls)


@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
def test_send_notification_no_retry_when_mail_disabled(_mock_context):
    """Test that no retries occur when mail is disabled"""
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_mail = False

    mock_config_args = {"message": "Test Email Body"}

    with patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp:
        # act
        email_handler.send_notification(
            _context=_mock_context,
            _config_args=mock_config_args,
            max_retries=3,
        )

        # assert - SMTP should never be called
        mock_smtp.assert_not_called()


# =============================================================================
# Jitter Tests for Parallel Task Handling
# =============================================================================


@patch("spark_expectations.notifications.plugins.email.random.uniform")
@patch("spark_expectations.notifications.plugins.email.time.sleep")
@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
def test_send_notification_jitter_initial_delay(_mock_context, mock_sleep, mock_random):
    """Test that initial jitter delay is applied when jitter is enabled"""
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_mail = True
    _mock_context.get_mail_from = "sender@example.com"
    _mock_context.get_to_mail = "receiver@example.com"
    _mock_context.get_mail_subject = "Test Email"
    _mock_context.get_mail_smtp_server = "mailhost.example.com"
    _mock_context.get_mail_smtp_port = 587
    _mock_context.get_enable_smtp_server_auth = False

    mock_config_args = {"message": "Test Email Body"}

    # Mock random.uniform to return predictable values
    mock_random.return_value = 1.5  # Initial jitter value

    with (
        patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp,
        patch("spark_expectations.notifications.plugins.email.MIMEMultipart"),
    ):
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server

        # act - with jitter enabled (default)
        email_handler.send_notification(
            _context=_mock_context,
            _config_args=mock_config_args,
            enable_jitter=True,
        )

        # assert - random.uniform should be called for initial jitter
        mock_random.assert_called_with(0, 3.0)
        # First sleep call should be the initial jitter
        assert mock_sleep.call_args_list[0][0][0] == 1.5


@patch("spark_expectations.notifications.plugins.email.random.uniform")
@patch("spark_expectations.notifications.plugins.email.time.sleep")
@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
def test_send_notification_jitter_no_initial_delay_when_disabled(_mock_context, mock_sleep, mock_random):
    """Test that no initial jitter delay when jitter is disabled"""
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_mail = True
    _mock_context.get_mail_from = "sender@example.com"
    _mock_context.get_to_mail = "receiver@example.com"
    _mock_context.get_mail_subject = "Test Email"
    _mock_context.get_mail_smtp_server = "mailhost.example.com"
    _mock_context.get_mail_smtp_port = 587
    _mock_context.get_enable_smtp_server_auth = False

    mock_config_args = {"message": "Test Email Body"}

    with (
        patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp,
        patch("spark_expectations.notifications.plugins.email.MIMEMultipart"),
    ):
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server

        # act - with jitter disabled
        email_handler.send_notification(
            _context=_mock_context,
            _config_args=mock_config_args,
            enable_jitter=False,
        )

        # assert - random.uniform should NOT be called when jitter is disabled
        mock_random.assert_not_called()
        # No sleep should be called on success without retries
        mock_sleep.assert_not_called()


@patch("spark_expectations.notifications.plugins.email.random.uniform")
@patch("spark_expectations.notifications.plugins.email.time.sleep")
@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
def test_send_notification_jitter_added_to_retry_delays(_mock_context, mock_sleep, mock_random):
    """Test that jitter is added to retry delays when enabled"""
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_mail = True
    _mock_context.get_mail_from = "sender@example.com"
    _mock_context.get_to_mail = "receiver@example.com"
    _mock_context.get_mail_subject = "Test Email"
    _mock_context.get_mail_smtp_server = "mailhost.example.com"
    _mock_context.get_mail_smtp_port = 587
    _mock_context.get_enable_smtp_server_auth = False

    mock_config_args = {"message": "Test Email Body"}

    # Mock random.uniform to return predictable values
    # First call: initial jitter (0-3)
    # Second call: retry jitter (0 to wait_time*0.5)
    mock_random.side_effect = [1.0, 0.5]  # Initial jitter=1.0, retry jitter=0.5

    with (
        patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp,
        patch("spark_expectations.notifications.plugins.email.MIMEMultipart"),
    ):
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server
        # Fail first, succeed second
        mock_server.sendmail.side_effect = [Exception("Temporary failure"), None]

        # act - with jitter enabled
        email_handler.send_notification(
            _context=_mock_context,
            _config_args=mock_config_args,
            max_retries=3,
            base_retry_delay=2.0,
            enable_jitter=True,
        )

        # assert - sleep should include jitter
        # First sleep: initial jitter = 1.0
        # Second sleep: base_retry_delay * 1 + jitter = 2.0 + 0.5 = 2.5
        sleep_calls = [call[0][0] for call in mock_sleep.call_args_list]
        assert sleep_calls[0] == 1.0   # Initial jitter
        assert sleep_calls[1] == 2.5   # Retry delay with jitter (2.0 + 0.5)


@patch("spark_expectations.notifications.plugins.email.random.uniform")
@patch("spark_expectations.notifications.plugins.email.time.sleep")
@patch("spark_expectations.notifications.plugins.email.SparkExpectationsContext", autospec=True, spec_set=True)
def test_send_notification_jitter_helps_parallel_tasks(_mock_context, mock_sleep, mock_random):
    """Test that jitter creates different delays for parallel task simulation"""
    email_handler = SparkExpectationsEmailPluginImpl()
    _mock_context.get_enable_mail = True
    _mock_context.get_mail_from = "sender@example.com"
    _mock_context.get_to_mail = "receiver@example.com"
    _mock_context.get_mail_subject = "Test Email"
    _mock_context.get_mail_smtp_server = "mailhost.example.com"
    _mock_context.get_mail_smtp_port = 587
    _mock_context.get_enable_smtp_server_auth = False

    mock_config_args = {"message": "Test Email Body"}

    with (
        patch("spark_expectations.notifications.plugins.email.smtplib.SMTP") as mock_smtp,
        patch("spark_expectations.notifications.plugins.email.MIMEMultipart"),
    ):
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server

        # Simulate first task with jitter=0.5
        mock_random.return_value = 0.5
        email_handler.send_notification(
            _context=_mock_context,
            _config_args=mock_config_args,
            enable_jitter=True,
        )
        first_task_delay = mock_sleep.call_args_list[0][0][0]

        mock_sleep.reset_mock()

        # Simulate second task with jitter=2.5
        mock_random.return_value = 2.5
        email_handler.send_notification(
            _context=_mock_context,
            _config_args=mock_config_args,
            enable_jitter=True,
        )
        second_task_delay = mock_sleep.call_args_list[0][0][0]

        # assert - different tasks get different delays
        assert first_task_delay != second_task_delay
        assert first_task_delay == 0.5
        assert second_task_delay == 2.5
