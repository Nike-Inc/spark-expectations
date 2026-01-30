import json
import time
from typing import Dict, Union, Optional
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from jinja2 import Environment, PackageLoader, BaseLoader
from spark_expectations import _log
from spark_expectations.notifications.plugins.base_notification import (
    SparkExpectationsNotification,
    spark_expectations_notification_impl,
)
from spark_expectations.core.exceptions import SparkExpectationsEmailException
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.secrets import SparkExpectationsSecretsBackend
from spark_expectations.config.user_config import Constants as user_config


# Create the email plugin
class SparkExpectationsEmailPluginImpl(SparkExpectationsNotification):
    """
    This class implements/supports functionality to send email
    """

    def _get_cerberus_password(
        self, secret_handler: SparkExpectationsSecretsBackend, smtp_secret_dict: dict
    ) -> Optional[str]:
        cbs_sdb_path = smtp_secret_dict.get(user_config.cbs_sdb_path)
        smtp_password_key = smtp_secret_dict.get(user_config.cbs_smtp_password)
        if cbs_sdb_path and smtp_password_key:
            secret = secret_handler.get_secret(cbs_sdb_path)
            if isinstance(secret, dict):
                return secret.get(smtp_password_key)
        return None

    def _get_databricks_password(
        self, secret_handler: SparkExpectationsSecretsBackend, smtp_secret_dict: dict
    ) -> Optional[str]:
        smtp_password_key = smtp_secret_dict.get(user_config.dbx_smtp_password)
        if smtp_password_key:
            return secret_handler.get_secret(smtp_password_key)
        return None

    def _retrieve_password(
        self,
        secret_handler: SparkExpectationsSecretsBackend,
        secret_type: str,
        smtp_secret_dict: dict,
    ) -> Optional[str]:
        if secret_type == "cerberus":
            return self._get_cerberus_password(secret_handler, smtp_secret_dict)
        elif secret_type == "databricks":
            return self._get_databricks_password(secret_handler, smtp_secret_dict)
        return None

    def _get_smtp_password(self, _context: SparkExpectationsContext, server: smtplib.SMTP) -> None:
        """
        Retrieves the SMTP password from secret and logs in to the server.
        Args:
            _context: SparkExpectationsContext object
            server: smtplib.SMTP object
        """
        sender = _context.get_mail_from
        password = _context.get_mail_smtp_password

        if not password:
            smtp_secret_dict = _context.get_smtp_creds_dict
            secret_handler = SparkExpectationsSecretsBackend(secret_dict=smtp_secret_dict)
            secret_type = smtp_secret_dict.get(user_config.secret_type)
            if secret_type:
                try:
                    password = self._retrieve_password(secret_handler, secret_type, smtp_secret_dict)
                except KeyError:
                    raise SparkExpectationsEmailException("SMTP password key is missing in the secret.")
                except Exception as e:
                    raise SparkExpectationsEmailException("   Failed to retrieve SMTP password.") from e

        if password is None:
            raise SparkExpectationsEmailException("SMTP password is not set.")
        server.login(sender, password)

    def _process_message(
        self, _context: SparkExpectationsContext, _config_args: Dict[Union[str], Union[str, bool]]
    ) -> tuple[str, str]:
        """
        Takes in the notification message and applies a content type and an html template if that option is set.
        Args:
            _context: SparkExpectationsContext object
            _config_args: Dict[Union[str], Union[str, bool]]
        """
        mail_content = f"""{_config_args.get("message")}"""

        # Check if the content is HTML
        if _config_args.get("content_type") == "html":
            content_type = "html"
        else:
            content_type = "plain"

        if mail_content.startswith("CUSTOM EMAIL\n"):
            mail_content = mail_content[len("CUSTOM EMAIL\n") :]  # remove leading "CUSTOM EMAIL" text

            if _context.get_enable_templated_custom_email is True:
                try:
                    custom_email_data = json.loads(mail_content)
                    if not _context.get_custom_default_template:
                        template_dir = "config/templates"
                        env_loader = Environment(loader=PackageLoader("spark_expectations", template_dir))
                        template = env_loader.get_template("custom_email_alert_template.jinja")
                    else:
                        template_string = _context.get_custom_default_template
                        template = Environment(loader=BaseLoader).from_string(template_string)

                    mail_content = template.render(custom_email_data)
                    content_type = "html"
                except json.JSONDecodeError as e:
                    _log.error(f"JSON decode error in custom email: {e}")
                    mail_content = (
                        f"Error: Invalid JSON format in custom email content.\nOriginal content:\n{mail_content}"
                    )
                    content_type = "plain"
                except Exception as e:
                    _log.error(f"Template rendering error in custom email: {e}")
                    mail_content = f"Error: Failed to render custom email template.\nOriginal content:\n{mail_content}"
                    content_type = "plain"

        elif (_config_args.get("email_notification_type")) != "detailed":
            if _context.get_enable_templated_basic_email_body is True:
                if not _context.get_basic_default_template:
                    template_dir = "config/templates"
                    env_loader = Environment(loader=PackageLoader("spark_expectations", template_dir))
                    template = env_loader.get_template("basic_email_alert_template.jinja")
                else:
                    template_string = _context.get_basic_default_template
                    template = Environment(loader=BaseLoader).from_string(template_string)

                lines = mail_content.strip().split("\n")
                title = lines[0].strip() if lines else ""

                data = []
                for i in range(1, len(lines)):
                    line = lines[i].strip()
                    if line and ":" in line:
                        parts = line.split(":", 1)
                        data.append(parts)

                message_data = {"title": title, "rows": data}
                html_data = template.render(render_table=template.module.render_table, **message_data)
                mail_content = f"<h2>{_context.get_mail_subject}</h2>" + html_data
                content_type = "html"

        return mail_content, content_type

    @spark_expectations_notification_impl
    def send_notification(
        self,
        _context: SparkExpectationsContext,
        _config_args: Dict[Union[str], Union[str, bool]],
        max_retries: int = 4,
        base_retry_delay: float = 5.0,
    ) -> None:
        """
        function to send email notification for requested mail id's with retry logic
        
        Args:
            _context: object of SparkExpectationsContext
            _config_args: dict which consists of: receiver mail(str), subject: subject of
                          the mail(str) and body: body of the mail(str)
            max_retries: maximum number of retry attempts (default: 4)
            base_retry_delay: initial delay between retries in seconds (default: 5.0)
        Returns:

        """
        if _context.get_enable_mail is not True:
            return

        last_exception = None

        for attempt in range(1, max_retries + 1):
            server = None
            try:
                msg = MIMEMultipart()
                msg["From"] = _context.get_mail_from
                msg["To"] = _context.get_to_mail
                msg["Subject"] = _context.get_mail_subject

                mail_content, content_type = self._process_message(_context, _config_args)

                msg.attach(MIMEText(mail_content, content_type))

                # mailhost.com
                server = smtplib.SMTP(_context.get_mail_smtp_server, _context.get_mail_smtp_port)
                server.starttls()
                if _context.get_enable_smtp_server_auth:
                    self._get_smtp_password(_context, server)
                text = msg.as_string()
                server.sendmail(
                    _context.get_mail_from,
                    [email.strip() for email in _context.get_to_mail.split(",")],
                    text,
                )
                server.quit()
                server = None  # Mark as closed

                _log.info("email sent successfully")
                return  # Success, exit the method

            except Exception as e:
                last_exception = e
                # Ensure server connection is closed before retry
                if server is not None:
                    try:
                        server.quit()
                    except Exception:
                        pass  # Ignore cleanup errors

                if attempt < max_retries:
                    # Use longer delays for concurrent connection errors (432)
                    error_str = str(e)
                    if "432" in error_str or "concurrent" in error_str.lower():
                        # Longer delay for rate-limiting errors with exponential backoff
                        wait_time = base_retry_delay * (2 ** (attempt - 1))
                    else:
                        wait_time = base_retry_delay * attempt

                    _log.warning(
                        f"Email send attempt {attempt}/{max_retries} failed: {e}. "
                        f"Retrying in {wait_time} seconds..."
                    )
                    time.sleep(wait_time)
                else:
                    _log.error(
                        f"Email send attempt {attempt}/{max_retries} failed: {e}. "
                        f"No more retries left."
                    )

        # All retries exhausted
        raise SparkExpectationsEmailException(
            f"error occurred while sending email notification from spark expectations project "
            f"after {max_retries} attempts: {last_exception}"
        )
