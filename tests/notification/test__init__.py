import pluggy
from spark_expectations.notifications import get_notifications_hook
from spark_expectations.notifications.plugins.email import (
    SparkExpectationsEmailPluginImpl,
)
from spark_expectations.notifications.plugins.slack import (
    SparkExpectationsSlackPluginImpl,
)


def test_notifications_hook():
    pm = get_notifications_hook()
    pm.get_plugins()

    # act
    email_plugin = pm.get_plugin("spark_expectations_email_notification")
    slack_plugin = pm.get_plugin("spark_expectations_slack_notification")
    # Check that the correct number of plugins have been registered
    assert len(pm.list_name_plugin()) == 2
    # assert
    assert isinstance(pm, pluggy.PluginManager)
    assert email_plugin is not None
    assert slack_plugin is not None
    assert isinstance(email_plugin, SparkExpectationsEmailPluginImpl)
    assert isinstance(slack_plugin, SparkExpectationsSlackPluginImpl)
