import functools
import pluggy
from spark_expectations import _log
from spark_expectations.notifications.plugins.base_notification import (
    SparkExpectationsNotification,
    SPARK_EXPECTATIONS_NOTIFICATION_PLUGIN,
)

from spark_expectations.notifications.plugins.email import (
    SparkExpectationsEmailPluginImpl,
)
from spark_expectations.notifications.plugins.slack import (
    SparkExpectationsSlackPluginImpl,
)
from spark_expectations.notifications.plugins.teams import (
    SparkExpectationsTeamsPluginImpl,
)
from spark_expectations.notifications.plugins.zoom import (
    SparkExpectationsZoomPluginImpl,  # Import Zoom plugin
)


@functools.lru_cache
def get_notifications_hook() -> pluggy.PluginManager:
    """
    function provides pluggy hook manger to send email, slack and zoom notification
    Returns:
        PluginManager: pluggy Manager object

    """
    pm = pluggy.PluginManager(SPARK_EXPECTATIONS_NOTIFICATION_PLUGIN)
    pm.add_hookspecs(SparkExpectationsNotification)
    pm.register(
        SparkExpectationsEmailPluginImpl(), "spark_expectations_email_notification"
    )
    pm.register(
        SparkExpectationsSlackPluginImpl(), "spark_expectations_slack_notification"
    )
    pm.register(
        SparkExpectationsTeamsPluginImpl(), "spark_expectations_teams_notification"
    )
    pm.register(
        SparkExpectationsZoomPluginImpl(),
        "spark_expectations_zoom_notification",  # Register Zoom plugin
    )
    for name, plugin_instance in pm.list_name_plugin():
        _log.info(
            "Loaded plugin with name: %s and class: %s",
            name,
            plugin_instance.__class__.__name__,
        )
    return pm


_notification_hook = get_notifications_hook().hook
