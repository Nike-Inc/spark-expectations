# Slack Notifications

Spark Expectations supports sending notifications to Slack channels via webhooks when data quality checks are performed. This enables teams to stay informed about data quality issues in real-time.

By default, Slack notifications are disabled. To enable them, you need to configure the required parameters and set up a Slack webhook URL.

## Prerequisites

Before configuring Slack notifications, you need:

###  Slack Webhook URL

1. Go to [https://api.slack.com/apps](https://api.slack.com/apps) and create a new app
2. Enable **"Incoming Webhooks"** and add one to your workspace
3. Select your target channel (public, private, or direct message)
4. Copy the generated webhook URL:
   ```
   https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX
   ```

!!! important "Security"
    Store webhook URLs securely using environment variables - never commit them to code.

## Notification Config Parameters

### Required Parameters

!!! info "user_config.se_notifications_enable_slack"
    Master toggle to enable Slack notifications. Set to `True` to activate Slack notifications.

!!! info "user_config.se_notifications_slack_webhook_url" 
    The Slack webhook URL obtained from your Slack app configuration. This is where notifications will be sent.

### Notification Triggers

These parameters control **when** Slack notifications are sent during Spark-Expectations runs:

- <abbr title="Enable notifications when job starts">user_config.se_notifications_on_start</abbr>
- <abbr title="Enable notifications when job ends">user_config.se_notifications_on_completion</abbr> 
- <abbr title="Enable notifications on failure">user_config.se_notifications_on_fail</abbr>
- <abbr title="Notify if error drop threshold is breached">user_config.se_notifications_on_error_drop_exceeds_threshold_breach</abbr>
- <abbr title="Notify if rules with action 'ignore' fail">user_config.se_notifications_on_rules_action_if_failed_set_ignore</abbr>
- <abbr title="Threshold value for error drop notifications">user_config.se_notifications_on_error_drop_threshold</abbr>

## Configuration Example

Here's how to configure Slack notifications in your Spark Expectations setup:

```python
from spark_expectations.config.user_config import *

# Basic Slack configuration
user_configs = {
    # Enable Slack notifications
    se_notifications_enable_slack: True,
    
    # Slack webhook URL (replace with your actual webhook URL)
    se_notifications_slack_webhook_url: "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX",
    
    # Configure when to send notifications
    se_notifications_on_start: True,
    se_notifications_on_completion: True, 
    se_notifications_on_fail: True,
    se_notifications_on_error_drop_exceeds_threshold_breach: True,
    se_notifications_on_error_drop_threshold: 15,
    
    # Other required configurations
    se_enable_error_table: True,
    se_dq_rules_params: {
        "env": "dev",
        "table": "your_table_name",
        "data_object_name": "your_data_object",
        "data_source": "your_data_source",
    }
}
```

## Example Notebook

For a complete working example of Slack notifications implementation, see our example notebook:

- [**Slack Notification Example**](../../../notebooks/spark_expectations_basic_slack_notification.ipynb) - Demonstrates how to set up and use Slack notifications with Spark Expectations

This notebook includes:

- Step-by-step configuration setup
- Complete code examples with real Slack webhook integration
- Sample data quality rules and notification triggers
- Best practices for Slack notification implementation

## Message Format

Slack notifications sent by Spark Expectations include:

- **Job Status**: Whether the data quality check started, completed, or failed
- **Data Quality Results**: Summary of passed/failed expectations  
- **Error Details**: Information about specific data quality issues
- **Metadata**: Table name, environment, timestamp, and other contextual information

### Testing Slack Integration

You can test your Slack webhook configuration using curl:

```bash
curl -X POST -H 'Content-type: application/json' \
--data '{"text":"Test message from Spark Expectations"}' \
YOUR_WEBHOOK_URL
```
