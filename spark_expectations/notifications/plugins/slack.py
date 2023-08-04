from typing import Dict, Union
import requests
from spark_expectations import _log
from spark_expectations.notifications.plugins.base_notification import (
    SparkExpectationsNotification,
    spark_expectations_notification_impl,
)
from spark_expectations.core.exceptions import (
    SparkExpectationsSlackNotificationException,
)
from spark_expectations.core.context import SparkExpectationsContext


class SparkExpectationsSlackPluginImpl(SparkExpectationsNotification):
    """
    This class implements/supports functionality to send slack notification
    """

    @spark_expectations_notification_impl
    def send_notification(
        self,
        _context: SparkExpectationsContext,
        _config_args: Dict[Union[str], Union[str, bool]],
    ) -> None:
        """
        function to send the slack notification for assigned channel
        Args:
            _context: SparkExpectationsContext class object
            _config_args: dict

        Returns: None

        """
        try:
            if _context.get_enable_slack is True:
                # payload = {"token": "{token}", "channel": kwargs['channel'], "text": kwargs['message']}
                payload = {"text": _config_args.get("message")}
                response = requests.post(
                    _context.get_slack_webhook_url, json=payload, timeout=10
                )

                # Check the response for success or failure
                if response.status_code == 200:
                    _log.info("Message posted successfully!")
                else:
                    _log.info("Failed to post message")
                    raise SparkExpectationsSlackNotificationException(
                        "error occurred while sending slack notification from spark expectations project"
                    )

        except Exception as e:
            raise SparkExpectationsSlackNotificationException(e)
