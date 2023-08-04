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


# Create the email plugin
class SparkExpectationsEmailPluginImpl(SparkExpectationsNotification):
    """
    This class implements/supports functionality to send email
    """

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
            _config_args: dict(which consist to: receiver mail(str), subject: subject of
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
                text = msg.as_string()
                server.sendmail(_context.get_mail_from, _context.get_to_mail, text)
                server.quit()

                _log.info("email send successfully")

        except Exception as e:
            raise SparkExpectationsEmailException(
                f"error occurred while sending email notification from spark expectations project {e}"
            )
