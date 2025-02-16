from typing import Dict, Union, Optional
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from spark_expectations import _log
from spark_expectations.notifications.plugins.base_notification import (
    SparkExpectationsNotification,
    spark_expectations_notification_impl,
)
from spark_expectations.core.exceptions import SparkExpectationsEmailException
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.secrets import SparkExpectationsSecretsBackend
from spark_expectations.config.user_config import Constants as user_config
import re


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

    def _get_smtp_password(
        self, _context: SparkExpectationsContext, server: smtplib.SMTP
    ) -> None:
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
            secret_handler = SparkExpectationsSecretsBackend(
                secret_dict=smtp_secret_dict
            )
            secret_type = smtp_secret_dict.get(user_config.secret_type)
            if secret_type:
                try:
                    password = self._retrieve_password(
                        secret_handler, secret_type, smtp_secret_dict
                    )
                except KeyError:
                    raise SparkExpectationsEmailException(
                        "SMTP password key is missing in the secret."
                    )
                except Exception as e:
                    raise SparkExpectationsEmailException(
                        "   Failed to retrieve SMTP password."
                    ) from e

        if password is None:
            raise SparkExpectationsEmailException("SMTP password is not set.")
        server.login(sender, password)

    @spark_expectations_notification_impl
    def send_notification(
        self,
        _context: SparkExpectationsContext,
        _config_args: Dict[Union[str], Union[str, bool]],
    ) -> None:
        """
        function to send email notification for requested mail id's
        Args:
            _context: object of SparkExpectationsContext
            _config_args: dict(which consists of: receiver mail(str), subject: subject of
                          the mail(str) and body: body of the mail(str)
        Returns:

        """
        try:
            if _context.get_enable_mail is True:
                msg = MIMEMultipart()
                msg["From"] = _context.get_mail_from
                msg["To"] = _context.get_to_mail
                msg["Subject"] = _context.get_mail_subject

                # body = _config_args.get('mail_body')
                mail_content = f"""{_config_args.get("message")}"""

                # Check if the content is HTML
                if re.search(r"<[a-z][\s\S]*>", mail_content, re.IGNORECASE):
                    content_type = "html"
                else:
                    content_type = "plain"

                msg.attach(MIMEText(mail_content, content_type))

                # mailhost.com
                server = smtplib.SMTP(
                    _context.get_mail_smtp_server, _context.get_mail_smtp_port
                )
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

                _log.info("email send successfully")

        except Exception as e:
            raise SparkExpectationsEmailException(
                f"error occurred while sending email notification from spark expectations project {e}"
            )
