# Microsoft Teams Notifications

Spark Expectations can send data quality notifications to Microsoft Teams channels via incoming webhooks.

## Setup

### 1. Create an Incoming Webhook in Teams

1. Open the Teams channel where you want to receive notifications.
2. Click the channel name, then **Manage channel** > **Connectors** (or **Workflows**).
3. Add an **Incoming Webhook** connector.
4. Give it a name (e.g., "Spark Expectations DQ") and copy the generated webhook URL.

### 2. Configure Spark Expectations

```python
from spark_expectations.config.user_config import Constants as user_config

user_conf = {
    user_config.se_notifications_enable_teams: True,
    user_config.se_notifications_teams_webhook_url: "https://outlook.office.com/webhook/...",
    user_config.se_notifications_on_start: True,
    user_config.se_notifications_on_completion: True,
    user_config.se_notifications_on_fail: True,
}
```

| Constant | Description |
|---|---|
| `se_notifications_enable_teams` | Set to `True` to enable Teams notifications |
| `se_notifications_teams_webhook_url` | The incoming webhook URL from your Teams channel |

## Notification Content

Teams notifications include the same information as other channels:

- **Product ID** and **table name**
- **Run ID** and **run timestamp**
- **Input, output, and error counts** with percentages
- **DQ status** (pass/fail for each phase)
- **Kafka write status** (if streaming is enabled)

## Trigger Configuration

Control when Teams notifications are sent using the standard notification trigger flags:

| Trigger | Constant | Default |
|---|---|---|
| Job start | `se_notifications_on_start` | `False` |
| Job completion | `se_notifications_on_completion` | `True` |
| Job failure | `se_notifications_on_fail` | `True` |
| Error drop threshold breach | `se_notifications_on_error_drop_exceeds_threshold_breach` | `False` |
