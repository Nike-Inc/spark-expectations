from typing import Dict, Union, Optional
import requests
from spark_expectations import _log
from spark_expectations.notifications.plugins.base_notification import (
    SparkExpectationsNotification,
    spark_expectations_notification_impl,
)
from spark_expectations.core.exceptions import (
    SparkExpectationsPagerDutyException,
)
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.secrets import SparkExpectationsSecretsBackend
from spark_expectations.config.user_config import Constants as user_config


class SparkExpectationsPagerDutyPluginImpl(SparkExpectationsNotification):
    """
    This class implements/supports functionality to send PagerDuty notifications
    """

    def _get_cerberus_integration_key(
        self, secret_handler: SparkExpectationsSecretsBackend, pd_secret_dict: dict
    ) -> Optional[str]:
        cbs_sdb_path = pd_secret_dict.get(user_config.cbs_sdb_path)
        pd_integration_key = pd_secret_dict.get(user_config.se_notifications_pagerduty_integration_key)
        if cbs_sdb_path and pd_integration_key:
            secret = secret_handler.get_secret(cbs_sdb_path)
            if isinstance(secret, dict):
                return secret.get(pd_integration_key)
        return None

    def _get_databricks_integration_key(
        self, secret_handler: SparkExpectationsSecretsBackend, pd_secret_dict: dict
    ) -> Optional[str]:
        pd_integration_key = pd_secret_dict.get(user_config.se_notifications_pagerduty_integration_key)
        if pd_integration_key:
            return secret_handler.get_secret(pd_integration_key)
        return None

    def _retrieve_integration_key(
        self,
        secret_handler: SparkExpectationsSecretsBackend,
        secret_type: str,
        pd_secret_dict: dict,
    ) -> Optional[str]:
        if secret_type == "cerberus":
            return self._get_cerberus_integration_key(secret_handler, pd_secret_dict)
        elif secret_type == "databricks":
            return self._get_databricks_integration_key(secret_handler, pd_secret_dict)
        return None

    def _get_pd_integration_key(self, _context: SparkExpectationsContext) -> None:
        """
        Retrieves the PagerDuty integration key from secret and sets it in the context.
        Args:
            _context: SparkExpectationsContext object
        """
        integration_key = _context.get_pagerduty_integration_key

        if not integration_key:
            pagerduty_secret_dict = _context.get_pagerduty_creds_dict
            secret_handler = SparkExpectationsSecretsBackend(secret_dict=pagerduty_secret_dict)
            secret_type = pagerduty_secret_dict.get(user_config.secret_type)
            if secret_type:
                try:
                    integration_key = self._retrieve_integration_key(secret_handler, secret_type, pagerduty_secret_dict)
                except KeyError:
                    _log.error("KeyError: PagerDuty integration key is missing.")
                    raise SparkExpectationsPagerDutyException("KeyError: PagerDuty integration key is missing.")
                except Exception as e:
                    _log.error(f"Error retrieving PagerDuty integration key: {e}")
                    raise SparkExpectationsPagerDutyException("Failed to retrieve PagerDuty integration key.") from e

        if integration_key is None:
            raise SparkExpectationsPagerDutyException("PagerDuty integration key is not set.")

    def _is_failure_notification(self, message: str) -> bool:
        """
        Check if the notification message indicates a failure or error condition.
        PagerDuty incidents should only be created for failure scenarios.
        
        Args:
            message: The notification message content
            
        Returns:
            bool: True if this is a failure-related notification, False otherwise
        """
        if not message:
            return False
            
        # Convert to lowercase for case-insensitive matching
        message_lower = message.lower()
        
        # Exclude informational notifications about ignored rules - these should NOT create PagerDuty incidents
        if "action_if_failed are set to ignore" in message_lower:
            return False
        
        # Define failure indicators - only create incidents for actual failures
        failure_patterns = [
            "has been failed",  # Job failure
            "failed",
            "error percentage has been exceeded above the threshold",  # Error threshold breach
            "dropped error percentage has been exceeded above the threshold",  # Error drop threshold breach
        ]
        
        # Check if any failure pattern is present in the message
        return any(pattern in message_lower for pattern in failure_patterns)

    @spark_expectations_notification_impl
    def send_notification(
        self,
        _context: SparkExpectationsContext,
        _config_args: Dict[str, Union[str, bool]],
    ) -> None:
        """
        function to create a PagerDuty incident
        Args:
            _context: SparkExpectationsContext class object
            _config_args: dict which contains message(str) and other related configurations for PD
        Returns: None
        """
        try:
            if _context.get_enable_pagerduty is True:
                message = _config_args.get("message")
                
                # Only send PagerDuty notifications for failure scenarios
                if not self._is_failure_notification(str(message)):
                    _log.info("PagerDuty notification skipped - not a failure scenario")
                    return

                if not _context.get_pagerduty_integration_key:
                    self._get_pd_integration_key(_context)

                # Generate a deduplication key to consolidate multiple incidents into one
                # Using table name and product ID to ensure same failure creates only one incident
                table_name = _context.get_table_name or "unknown_table"
                product_id = _context.product_id or "unknown_product"
                dedup_key = f"spark_expectations_{product_id}_{table_name}_failure"

                # Sending request to PagerDuty Events API v2 > https://developer.pagerduty.com/docs/send-alert-event
                # Severity Levels can be: critical, error, warning, or info
                payload = {
                    "routing_key": _context.get_pagerduty_integration_key,
                    "dedup_key": dedup_key,
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
                    raise SparkExpectationsPagerDutyException(
                        f"Failed to send PagerDuty notification. Status code: {response.status_code}, Response: {response.text}"
                    )
        except Exception as e:
            raise SparkExpectationsPagerDutyException(f"Error sending PagerDuty notification: {str(e)}")
