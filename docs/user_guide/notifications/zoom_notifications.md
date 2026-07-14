# Zoom Notifications

Spark Expectations can send data quality notifications to Zoom channels via incoming webhooks.

## Setup

### 1. Create a Zoom Incoming Webhook

1. Go to the [Zoom App Marketplace](https://marketplace.zoom.us/) and create or configure an Incoming Webhook app.
2. Configure the webhook to post to your desired Zoom channel.
3. Copy the **webhook URL** and **verification token**.

### 2. Configure Spark Expectations

```python
from spark_expectations.config.user_config import Constants as user_config

user_conf = {
    user_config.se_notifications_enable_zoom: True,
    user_config.se_notifications_zoom_webhook_url: "https://inbots.zoom.us/incoming/hook/...",
    user_config.se_notifications_zoom_token: "your-verification-token",
    user_config.se_notifications_on_start: True,
    user_config.se_notifications_on_completion: True,
    user_config.se_notifications_on_fail: True,
}
```

| Constant | Description |
|---|---|
| `se_notifications_enable_zoom` | Set to `True` to enable Zoom notifications |
| `se_notifications_zoom_webhook_url` | The incoming webhook URL for your Zoom channel |
| `se_notifications_zoom_token` | Verification token for authenticating the webhook |

## Notification Content

Zoom notifications include the same information as other channels:

- **Product ID** and **table name**
- **Run ID** and **run timestamp**
- **Input, output, and error counts** with percentages
- **DQ status** (pass/fail for each phase)
- **Kafka write status** (if streaming is enabled)

## Trigger Configuration

Control when Zoom notifications are sent using the standard notification trigger flags:

| Trigger | Constant | Default |
|---|---|---|
| Job start | `se_notifications_on_start` | `False` |
| Job completion | `se_notifications_on_completion` | `True` |
| Job failure | `se_notifications_on_fail` | `True` |
| Error drop threshold breach | `se_notifications_on_error_drop_exceeds_threshold_breach` | `False` |
