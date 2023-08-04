from typing import Dict, Union
import pluggy
from spark_expectations.core.context import SparkExpectationsContext

SPARK_EXPECTATIONS_NOTIFICATION_PLUGIN = "spark_expectations_notification_plugins"

notification_plugin_spec = pluggy.HookspecMarker(SPARK_EXPECTATIONS_NOTIFICATION_PLUGIN)
spark_expectations_notification_impl = pluggy.HookimplMarker(
    SPARK_EXPECTATIONS_NOTIFICATION_PLUGIN
)


class SparkExpectationsNotification:
    """
    This is base class for notifications plugin
    """

    @notification_plugin_spec
    def send_notification(
        self,
        _context: SparkExpectationsContext,
        _config_args: Dict[Union[str], Union[str, bool]],
    ) -> None:
        """
        function consist signature to notification, which will be implemented in the child class
        Args:
            _context:object of SparkExpectationsContext
            _config_args: dict which contains required parameter to send notification

        Returns: None

        """
        pass
