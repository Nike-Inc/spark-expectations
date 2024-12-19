from typing import Dict, Union
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


# Create the email plugin
class SparkExpectationsEmailPluginImpl(SparkExpectationsNotification):
    """
    This class implements/supports functionality to send email
    """

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
        secret_handler = SparkExpectationsSecretsBackend(
            secret_dict=_context.get_se_streaming_stats_dict
        )
        secret_type = _context.get_secret_type
        try:
            if secret_type == "cerberus":
                cbs_sdb_path = _context.get_cbs_sdb_path
                smtp_password_key = _context.get_smtp_password_key
                if cbs_sdb_path and smtp_password_key:
                    secret = secret_handler.get_secret(cbs_sdb_path)
                    if isinstance(secret, dict):
                        password = secret.get(smtp_password_key)
                    else:
                        password = None
            elif secret_type == "databricks":
                smtp_password_key = _context.get_smtp_password_key
                if smtp_password_key:
                    password = secret_handler.get_secret(smtp_password_key)
                else:
                    password = None
            else:
                password = None
        except KeyError:
            raise SparkExpectationsEmailException(
                "SMTP password key is missing in the secret."
            )
        except Exception as e:
            raise SparkExpectationsEmailException(
                "Failed to retrieve SMTP password."
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
            _config_args: dict(which consist of: receiver mail(str), subject: subject of
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
                msg.attach(MIMEText(mail_content, "plain"))

                # mailhost.com
                server = smtplib.SMTP(
                    _context.get_mail_smtp_server, _context.get_mail_smtp_port
                )
                server.starttls()
                if _context.get_enable_smtp_server_auth is True:
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
