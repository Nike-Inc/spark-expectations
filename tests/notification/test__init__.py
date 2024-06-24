import pluggy
from spark_expectations.notifications import get_notifications_hook
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
    SparkExpectationsZoomPluginImpl,
)


def test_notifications_hook():
    pm = get_notifications_hook()
    pm.get_plugins()

    # act
    email_plugin = pm.get_plugin("spark_expectations_email_notification")
    slack_plugin = pm.get_plugin("spark_expectations_slack_notification")
    teams_plugin = pm.get_plugin("spark_expectations_teams_notification")
    zoom_plugin = pm.get_plugin("spark_expectations_zoom_notification")
    # Check that the correct number of plugins have been registered
    assert len(pm.list_name_plugin()) == 4
    # assert
    assert isinstance(pm, pluggy.PluginManager)
    assert email_plugin is not None
    assert slack_plugin is not None
    assert teams_plugin is not None
    assert zoom_plugin is not None
    assert isinstance(email_plugin, SparkExpectationsEmailPluginImpl)
    assert isinstance(slack_plugin, SparkExpectationsSlackPluginImpl)
    assert isinstance(teams_plugin, SparkExpectationsTeamsPluginImpl)
    assert isinstance(zoom_plugin, SparkExpectationsZoomPluginImpl)
