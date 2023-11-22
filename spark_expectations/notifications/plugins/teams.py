from typing import Dict, Union
import requests
from spark_expectations import _log
from spark_expectations.notifications.plugins.base_notification import (
    SparkExpectationsNotification,
    spark_expectations_notification_impl,
)
from spark_expectations.core.exceptions import (
    SparkExpectationsTeamsNotificationException,
)
from spark_expectations.core.context import SparkExpectationsContext


class SparkExpectationsTeamsPluginImpl(SparkExpectationsNotification):
    """
    This class implements/supports functionality to send teams notification
    """

    @spark_expectations_notification_impl
    def send_notification(
        self,
        _context: SparkExpectationsContext,
        _config_args: Dict[Union[str], Union[str, bool]],
    ) -> None:
        """
        function to send the teams notification for assigned channel
        Args:
            _context: SparkExpectationsContext class object
            _config_args: dict

        Returns: None

        """
        try:
            if _context.get_enable_teams is True:
                # payload = {"token": "{token}", "channel": kwargs['channel'], "text": kwargs['message']}

                message = _config_args.get("message")

                # Format Message for Teams
                if isinstance(message, str):
                    message = message.replace("\n", "\n\n").replace("            ", "")

                payload = {
                    "title": "SE Notification",
                    "themeColor": "008000",
                    "text": message,
                }

                response = requests.post(
                    _context.get_teams_webhook_url, json=payload, timeout=10
                )

                # Check the response for success or failure
                if response.status_code == 200:
                    _log.info("Message posted successfully!")
                else:
                    _log.info("Failed to post message")
                    raise SparkExpectationsTeamsNotificationException(
                        "error occurred while sending teams notification from spark expectations project"
                    )

        except Exception as e:
            raise SparkExpectationsTeamsNotificationException(e)
