# Configuration Reference

This page documents all configuration constants available in Spark-Expectations. Use these settings to control notifications, streaming, secrets, and data quality processing behavior.

## Configuration Resolution Order

Spark-Expectations resolves configuration values in the following order (later sources override earlier ones):

1. **Default YAML** — `spark-expectations-default-config.yaml` provides baseline values
2. **Spark session config** — Values set via `spark.conf.set("key", "value")`
3. **User config dict** — The `user_conf` parameter passed to `@se.with_expectations()` or constructor options

!!! tip "Override precedence"
    Any value you pass in `user_conf` overrides both the default YAML and Spark session configuration for that key.

## How to Use

Import the `Constants` class and use the constant names as keys in your configuration dictionaries:

```python
from spark_expectations.config.user_config import Constants as user_config

user_conf = {
    user_config.se_notifications_enable_email: True,
    user_config.se_notifications_email_smtp_host: "smtp.example.com",
    user_config.se_streaming_stats_topic_name: "my-dq-stats-topic",
}
```

You can also pass these keys to `stats_streaming_options`, `notification_config`, or other config parameters when constructing `SparkExpectations`.

---

## Notification Channels

!!! info "Channel-specific settings"
    Each notification channel (Email, Slack, Teams, Zoom, PagerDuty) has its own enable flag and connection settings. Configure only the channels you use.

=== "Email"

    | Constant | Spark Config Key | Type | Default | Description |
    |----------|------------------|------|---------|-------------|
    | `user_config.se_notifications_enable_email` | `spark.expectations.notifications.email.enabled` | bool | `false` | Enable email notifications |
    | `user_config.se_notifications_enable_smtp_server_auth` | `spark.expectations.notifications.email.smtp.server.auth` | bool | `false` | Enable SMTP server authentication |
    | `user_config.se_notifications_smtp_password` | `spark.expectations.notifications.email.smtp.password` | str | `""` | SMTP password (direct) |
    | `user_config.se_notifications_smtp_creds_dict` | `spark.expectations.notifications.smtp.creds.dict` | dict | — | Dict for secret-backed SMTP password |
    | `user_config.se_notifications_smtp_user_name` | `spark.expectations.notifications.smtp.user.name` | str | `""` | SMTP username |
    | `user_config.se_notifications_email_smtp_host` | `spark.expectations.notifications.email.smtp.host` | str | `""` | SMTP host |
    | `user_config.se_notifications_email_smtp_port` | `spark.expectations.notifications.email.smtp.port` | int | `25` | SMTP port |
    | `user_config.se_notifications_email_from` | `spark.expectations.notifications.email.from` | str | `""` | Sender email |
    | `user_config.se_notifications_email_to_other_mail_id` | `spark.expectations.notifications.email.to.other.mail.com` | str | `""` | Recipient email(s) |
    | `user_config.se_notifications_email_subject` | `spark.expectations.notifications.email.subject` | str | `"spark-expectations-testing"` | Email subject line |
    | `user_config.se_notifications_email_custom_body` | `spark.expectations.notifications.email.custom.body` | str | `""` | Custom email body content |
    | `user_config.se_notifications_enable_custom_email_body` | `spark.expectations.notifications.email.custom.body.enable` | bool | `false` | Enable custom email body |
    | `user_config.se_notifications_enable_templated_basic_email_body` | `spark.expectations.notifications.email.templated.basic.body.enable` | bool | `false` | Enable Jinja template for basic emails |
    | `user_config.se_notifications_default_basic_email_template` | `spark.expectations.notifications.email.default.basic.template` | str | `""` | Custom Jinja template path for basic emails |
    | `user_config.se_notifications_enable_templated_custom_email` | `spark.expectations.notifications.email.templated.custom.email.enable` | bool | `false` | Enable custom Jinja template emails |
    | `user_config.se_notifications_email_custom_template` | `spark.expectations.notifications.email.custom.template` | str | `""` | Custom Jinja template path |
    | `user_config.se_notifications_service_account_email` | `spark.expectations.notifications.service.account.email` | str | — | Service account email |
    | `user_config.se_notifications_service_account_password` | `spark.expectations.notifications.service.account.password` | str | — | Service account password |
    | `user_config.cbs_smtp_password` | `spark.expectations.notifications.cerberus.smtp.password` | str | — | Cerberus key for SMTP password |
    | `user_config.dbx_smtp_password` | `spark.expectations.notifications.dbx.smtp.password` | str | — | Databricks key for SMTP password |

    !!! note "Secret-backed SMTP password"
        Use `se_notifications_smtp_creds_dict` with Cerberus or Databricks keys (`cbs_smtp_password` or `dbx_smtp_password`) when you store SMTP credentials in a secret store instead of passing them directly.

=== "Slack"

    | Constant | Spark Config Key | Type | Default | Description |
    |----------|------------------|------|---------|-------------|
    | `user_config.se_notifications_enable_slack` | `spark.expectations.notifications.slack.enabled` | bool | `false` | Enable Slack notifications |
    | `user_config.se_notifications_slack_webhook_url` | `spark.expectations.notifications.slack.webhook.url` | str | `""` | Slack webhook URL |
    | `user_config.se_notifications_min_priority_slack` | `spark.expectations.notifications.slack.min.priority` | str | `"low"` | Minimum rule priority to send Slack alerts |

=== "Teams"

    | Constant | Spark Config Key | Type | Default | Description |
    |----------|------------------|------|---------|-------------|
    | `user_config.se_notifications_enable_teams` | `spark.expectations.notifications.teams.enabled` | bool | `false` | Enable Teams notifications |
    | `user_config.se_notifications_teams_webhook_url` | `spark.expectations.notifications.teams.webhook.url` | str | `""` | Teams webhook URL |

=== "Zoom"

    | Constant | Spark Config Key | Type | Default | Description |
    |----------|------------------|------|---------|-------------|
    | `user_config.se_notifications_enable_zoom` | `spark.expectations.notifications.zoom.enabled` | bool | `false` | Enable Zoom notifications |
    | `user_config.se_notifications_zoom_webhook_url` | `spark.expectations.notifications.zoom.webhook.url` | str | `""` | Zoom webhook URL |
    | `user_config.se_notifications_zoom_token` | `spark.expectations.notifications.zoom.token` | str | `""` | Zoom auth token |

=== "PagerDuty"

    | Constant | Spark Config Key | Type | Default | Description |
    |----------|------------------|------|---------|-------------|
    | `user_config.se_notifications_enable_pagerduty` | `spark.expectations.notifications.pagerduty.enabled` | bool | — | Enable PagerDuty notifications |
    | `user_config.se_notifications_pagerduty_integration_key` | `spark.expectations.notifications.pagerduty.integration.key` | str | — | PagerDuty integration key (routing key for Events API v2) |
    | `user_config.se_notifications_pagerduty_webhook_url` | `spark.expectations.notifications.pagerduty.webhook.url` | str | — | PagerDuty webhook URL |

---

## Notification Triggers

Control when notifications are sent.

| Constant | Spark Config Key | Type | Default | Description |
|----------|------------------|------|---------|-------------|
| `user_config.se_notifications_on_start` | `spark.expectations.notifications.on.start` | bool | `false` | Notify on job start |
| `user_config.se_notifications_on_completion` | `spark.expectations.notifications.on.completion` | bool | `true` | Notify on job completion |
| `user_config.se_notifications_on_fail` | `spark.expectations.notifications.on.fail` | bool | `true` | Notify on job failure |
| `user_config.se_notifications_on_error_drop_exceeds_threshold_breach` | `spark.expectations.notifications.on.error.drop.exceeds.threshold.breach` | bool | `false` | Notify when error drop exceeds threshold |
| `user_config.se_notifications_on_rules_action_if_failed_set_ignore` | `spark.expectations.notifications.on.rules.action.if.failed.set.ignore` | bool | `false` | Notify when ignore-action rules fail |
| `user_config.se_notifications_on_error_drop_threshold` | `spark.expectations.notifications.error.drop.threshold` | int | `100` | Error drop threshold percentage |

---

## Observability

| Constant | Spark Config Key | Type | Default | Description |
|----------|------------------|------|---------|-------------|
| `user_config.se_enable_obs_dq_report_result` | `spark.expectations.notifications.observability.enabled` | bool | — | Enable DQ observability report generation |
| `user_config.se_dq_obs_alert_flag` | `spark.expectations.notifications.alert.flag.disable` | bool | — | Enable alert email with observability report |
| `user_config.se_dq_obs_default_email_template` | `spark.expectations.dq.obs.default.detailed.email.template` | str | — | Custom Jinja template for observability email |
| `user_config.se_dq_obs_mode_of_communication` | `spark.expectations.dq.obs.mode.of.communication` | str | — | Communication mode for observability alerts |
| `user_config.se_user_defined_custom_dataframe` | `spark.expectations.user.defined.custom.dataframe` | DataFrame | — | User-defined custom DataFrame for reporting |
| `user_config.se_notifications_enable_custom_dataframe` | `spark.expectations.notifications.enable.custom.dataframe` | bool | — | Enable custom DataFrame in notifications |

---

## Streaming / Kafka

| Constant | Spark Config Key | Type | Default | Description |
|----------|------------------|------|---------|-------------|
| `user_config.se_enable_streaming` | `se.streaming.enable` | bool | `true` | Enable stats streaming to Kafka |
| `user_config.se_streaming_stats_kafka_custom_config_enable` | `se.streaming.stats.kafka.custom.config.enable` | bool | `false` | Enable custom Kafka config |
| `user_config.se_streaming_stats_topic_name` | `se.streaming.stats.topic.name` | str | `"dq-sparkexpectations-stats"` | Kafka topic name |
| `user_config.se_streaming_stats_kafka_bootstrap_server` | `se.streaming.stats.kafka.bootstrap.server` | str | `"localhost:9092"` | Kafka bootstrap server |

---

## Secrets

Set `secret_type` to choose between Databricks and Cerberus for secret storage. Configure only the store you use.

| Constant | Spark Config Key | Type | Default | Description |
|----------|------------------|------|---------|-------------|
| `user_config.secret_type` | `se.streaming.secret.type` | str | `"databricks"` | Secret store type: `"databricks"` or `"cerberus"` |

=== "Cerberus"

    Used when `secret_type` is `"cerberus"`. Keys reference Cerberus SDB paths.

    | Constant | Spark Config Key | Type | Default | Description |
    |----------|------------------|------|---------|-------------|
    | `user_config.cbs_url` | `se.streaming.cerberus.url` | str | — | Cerberus URL |
    | `user_config.cbs_sdb_path` | `se.streaming.cerberus.sdb.path` | str | — | Cerberus SDB path |
    | `user_config.cbs_kafka_server_url` | `se.streaming.cerberus.kafka.server.url` | str | — | Cerberus key for Kafka server URL |
    | `user_config.cbs_secret_token_url` | `se.streaming.cbs.secret.token.url` | str | — | Cerberus key for auth token URL |
    | `user_config.cbs_secret_app_name` | `se.streaming.cbs.secret.app.name` | str | — | Cerberus key for auth app name |
    | `user_config.cbs_secret_token` | `se.streaming.cerberus.secret.token` | str | — | Cerberus key for auth token |
    | `user_config.cbs_topic_name` | `se.streaming.cerberus.token.name` | str | — | Cerberus key for topic name |

=== "Databricks"

    Used when `secret_type` is `"databricks"`. Keys reference Databricks secret scope keys.

    | Constant | Spark Config Key | Type | Default | Description |
    |----------|------------------|------|---------|-------------|
    | `user_config.dbx_workspace_url` | `se.streaming.dbx.workspace.url` | str | `"https://workspace.cloud.databricks.com"` | Databricks workspace URL |
    | `user_config.dbx_secret_scope` | `se.streaming.dbx.secret.scope` | str | `"secret_scope"` | Databricks secret scope name |
    | `user_config.dbx_kafka_server_url` | `se.streaming.dbx.kafka.server.url` | str | `"se_streaming_server_url_secret_key"` | Databricks key for Kafka server URL |
    | `user_config.dbx_secret_token_url` | `se.streaming.dbx.secret.token.url` | str | `"se_streaming_auth_secret_token_url_key"` | Databricks key for auth token URL |
    | `user_config.dbx_secret_app_name` | `se.streaming.dbx.secret.app.name` | str | `"se_streaming_auth_secret_appid_key"` | Databricks key for auth app name |
    | `user_config.dbx_secret_token` | `se.streaming.dbx.secret.token` | str | `"se_streaming_auth_secret_token_key"` | Databricks key for auth token |
    | `user_config.dbx_topic_name` | `se.streaming.dbx.topic.name` | str | `"se_streaming_topic_name"` | Databricks key for topic name |

---

## DQ Processing Options

| Constant | Spark Config Key | Type | Default | Description |
|----------|------------------|------|---------|-------------|
| `user_config.se_enable_error_table` | `se.enable.error.table` | bool | `true` | Enable writing failed rows to error table |
| `user_config.se_dq_rules_params` | `se.dq.rules.params` | dict | — | Dict of dynamic parameters for rule expressions (e.g. `{env}`, `{table}`) |
| `user_config.se_enable_agg_dq_detailed_result` | `spark.expectations.agg.dq.detailed.stats` | bool | `false` | Enable detailed per-rule stats for agg_dq |
| `user_config.se_enable_query_dq_detailed_result` | `spark.expectations.query.dq.detailed.stats` | bool | `false` | Enable detailed per-rule stats for query_dq |
| `user_config.querydq_output_custom_table_name` | `spark.expectations.query.dq.custom.table_name` | str | — | Custom table name for query DQ output |
| `user_config.se_job_metadata` | `spark.expectations.job.metadata` | str/dict | `None` | Custom job metadata (serialized to JSON in stats) |
| `user_config.is_serverless` | `spark.expectations.is.serverless` | bool | `False` | Enable Databricks Serverless mode |

!!! info "Rule parameter substitution"
    Use `se_dq_rules_params` to pass dynamic values into rule expressions. For example, `{"env": "prod", "table": "orders"}` allows rules to reference `{env}` and `{table}` in their expressions.

---

## Usage Example

The following example shows how to combine multiple configuration groups:

```python
from spark_expectations.config.user_config import Constants as user_config
from spark_expectations import SparkExpectations

# Disable streaming, enable email, and set DQ options
user_conf = {
    user_config.se_enable_streaming: False,
    user_config.se_notifications_enable_email: True,
    user_config.se_notifications_email_smtp_host: "smtp.office365.com",
    user_config.se_notifications_email_smtp_port: 587,
    user_config.se_notifications_email_from: "dq@example.com",
    user_config.se_notifications_email_to_other_mail_id: "team@example.com",
    user_config.se_notifications_on_completion: True,
    user_config.se_notifications_on_fail: True,
    user_config.se_enable_agg_dq_detailed_result: True,
    user_config.se_enable_query_dq_detailed_result: True,
    user_config.se_dq_rules_params: {
        "env": "prod",
        "table": "customer_order",
    },
}

se = SparkExpectations(
    product_id="my_product",
    rules_df=spark.table("dq_rules"),
    stats_table="dq_stats",
)

@se.with_expectations(
    target_table="my_schema.my_table",
    write_to_table=True,
    user_conf=user_conf,
)
def build_pipeline():
    return spark.table("source_table")
```

!!! tip "Streaming options"
    For Kafka streaming, pass `stats_streaming_options` to the `SparkExpectations` constructor with keys like `user_config.se_enable_streaming` and `user_config.se_streaming_stats_topic_name`.
