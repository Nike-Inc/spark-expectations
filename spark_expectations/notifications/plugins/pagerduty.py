from typing import Dict, Union
import requests
from spark_expectations import _log
from spark_expectations.notifications.plugins.base_notification import (
    SparkExpectationsNotification,
    spark_expectations_notification_impl,
)
from spark_expectations.core.exceptions import (
    SparkExpectationsPagerDutyNotificationException,
)
from spark_expectations.core.context import SparkExpectationsContext

class SparkExpectationsPagerDutyPluginImpl(SparkExpectationsNotification):
    """
    This class implements/supports functionality to send PagerDuty notifications
    """

    @spark_expectations_notification_impl
    def send_notification(
        self,
        _context: SparkExpectationsContext,
        _config_args: Dict[str, Union[str, bool]],
    ) -> None:
        """
        function to send the PagerDuty notification
        Args:
            _context: SparkExpectationsContext class object
            _config_args: dict

        Returns: None

        """
        try:
            if _context.get_enable_pagerduty is True:
                message = _config_args.get("message")

                # Sending request to PagerDuty Events API v2 > https://developer.pagerduty.com/docs/send-alert-event
                # Severity Levels can be: critical, error, warning, or info
                payload = {
                    "routing_key": _context.get_pagerduty_routing_key,
                    "event_action": "trigger",
                    "payload": {
                        "summary": message,
                        "source": "Spark Expectations",
                        "severity": "error",
                    },
                }
                headers = {
                    "Content-Type": "application/json",
                }
                response = requests.post(
                    _context.get_pagerduty_webhook_url,
                    json=payload,
                    headers=headers,
                    timeout=10,
                )

                # Check the response for success or failure
                if response.status_code == 202:
                    _log.info("PagerDuty notification sent successfully!")
                else:
                    raise SparkExpectationsPagerDutyNotificationException(
                        f"Failed to send PagerDuty notification. Status code: {response.status_code}, Response: {response.text}"
                    )
        except Exception as e:
            raise SparkExpectationsPagerDutyNotificationException(f"Error sending PagerDuty notification: {str(e)}")