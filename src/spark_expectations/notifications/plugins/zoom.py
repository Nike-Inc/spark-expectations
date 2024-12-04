from typing import Dict, Union
import requests
from spark_expectations import _log
from spark_expectations.notifications.plugins.base_notification import (
    SparkExpectationsNotification,
    spark_expectations_notification_impl,
)
from spark_expectations.core.exceptions import (
    SparkExpectationsZoomNotificationException,
)
from spark_expectations.core.context import SparkExpectationsContext


class SparkExpectationsZoomPluginImpl(SparkExpectationsNotification):
    """
    This class implements/supports functionality to send Zoom notification
    """

    @spark_expectations_notification_impl
    def send_notification(
        self,
        _context: SparkExpectationsContext,
        _config_args: Dict[str, Union[str, bool]],
    ) -> None:
        """
        function to send the Zoom notification
        Args:
            _context: SparkExpectationsContext class object
            _config_args: dict

        Returns: None

        """
        try:
            if _context.get_enable_zoom is True:
                message = _config_args.get("message")

                # Format Message for Zoom
                if isinstance(message, str):
                    message = message.replace("\n", "\n\n").replace("            ", "")

                payload = {
                    "title": "SE Notification",
                    "themeColor": "008000",
                    "text": message,
                }
                headers = {
                    "Authorization": f"Bearer {_context.get_zoom_token}",  # Use get_zoom_token to retrieve token.
                    "Content-Type": "application/json",
                }
                response = requests.post(
                    _context.get_zoom_webhook_url,
                    json=payload,
                    headers=headers,
                    timeout=10,
                )

                # Check the response for success or failure
                if response.status_code == 200:
                    _log.info("Message posted successfully!")
                else:
                    _log.info("Failed to post message")
                    raise SparkExpectationsZoomNotificationException(
                        "error occurred while sending Zoom notification from spark expectations project"
                    )

        except Exception as e:
            raise SparkExpectationsZoomNotificationException(e)
