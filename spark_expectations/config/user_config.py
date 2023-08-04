from dataclasses import dataclass


@dataclass
class Constants:
    # declare const user config variables for email notification
    se_notifications_enable_email = "spark.expectations.notifications.email.enabled"
    se_notifications_email_smtp_host = (
        "spark.expectations.notifications.email.smtp_host"
    )
    se_notifications_email_smtp_port = (
        "spark.expectations.notifications.email.smtp_port"
    )
    se_notifications_email_from = "spark.expectations.notifications.email.from"
    se_notifications_email_to_other_mail_id = (
        "spark.expectations.notifications.email.to.other.mail.com"
    )
    se_notifications_email_subject = "spark.expectations.notifications.email.subject"

    # declare const user config variables for slack notification
    se_notifications_enable_slack = "spark.expectations.notifications.slack.enabled"
    se_notifications_slack_webhook_url = (
        "spark.expectations.notifications.slack.webhook_url"
    )

    se_agg_dq = "agg_dq"
    se_source_agg_dq = "source_agg_dq"
    se_final_agg_dq = "final_agg_dq"

    se_query_dq = "query_dq"
    se_source_query_dq = "source_query_dq"
    se_final_query_dq = "final_query_dq"
    se_target_table_view = "target_table_view"

    se_notifications_on_start = "spark.expectations.notifications.on_start"
    se_notifications_on_completion = "spark.expectations.notifications.on.completion"
    se_notifications_on_fail = "spark.expectations.notifications.on.fail"
    se_notifications_on_error_drop_exceeds_threshold_breach = (
        "spark.expectations.notifications.on.error.drop.exceeds.threshold.breach"
    )
    se_notifications_on_error_drop_threshold = (
        "spark.expectations.notifications.error.drop.threshold"
    )

    se_enable_streaming = "se.enable.streaming"

    secret_type = "se.streaming.secret.type"

    cbs_url = "se.streaming.cerberus.url"
    cbs_sdb_path = "se.streaming.cerberus.sdb.path"
    cbs_kafka_server_url = "se.streaming.cerberus.kafka.server.url"
    cbs_secret_token_url = "se.streaming.cbs.secret.token.url"
    cbs_secret_app_name = "se.streaming.cbs.secret.app.name"
    cbs_secret_token = "se.streaming.cerberus.secret.token"
    cbs_topic_name = "se.streaming.cerberus.token.name"

    dbx_workspace_url = "se.streaming.dbx.workspace.url"
    dbx_secret_scope = "se.streaming.dbx.secret.scope"
    dbx_kafka_server_url = "se.streaming.dbx.kafka.server.url"
    dbx_secret_token_url = "se.streaming.dbx.secret.token.url"
    dbx_secret_app_name = "se.streaming.dbx.secret.app.name"
    dbx_secret_token = "se.streaming.dbx.secret.token"
    dbx_topic_name = "se.streaming.dbx.topic.name"
