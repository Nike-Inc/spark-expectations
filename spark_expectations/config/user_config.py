from dataclasses import dataclass


@dataclass
class Constants:
    # declare const user config variables for email notification
    is_serverless = "spark.expectations.is.serverless"
    se_notifications_enable_smtp_server_auth = "spark.expectations.notifications.email.smtp.server.auth"
    se_notifications_smtp_password = "spark.expectations.notifications.email.smtp.password"
    se_notifications_smtp_creds_dict = "spark.expectations.notifications.smtp.creds.dict"
    cbs_smtp_password = "spark.expectations.notifications.cerberus.smtp.password"
    dbx_smtp_password = "spark.expectations.notifications.dbx.smtp.password"

    se_user_defined_custom_dataframe = "spark.expectations.user.defined.custom.dataframe"
    se_notifications_enable_custom_dataframe = "spark.expectations.notifications.enable.custom.dataframe"
    se_dq_obs_default_email_template = "spark.expectations.dq.obs.default.detailed.email.template"
    se_notifications_default_basic_email_template = "spark.expectations.notifications.email.default.basic.template"
    se_dq_obs_mode_of_communication = "spark.expectations.dq.obs.mode.of.communication"
    se_notifications_service_account_email = "spark.expectations.notifications.service.account.email"
    se_dq_obs_alert_flag = "spark.expectations.notifications.alert.flag.disable"
    se_notifications_service_account_password = "spark.expectations.notifications.service.account.password"
    se_notifications_smtp_user_name = "spark.expectations.notifications.smtp.user.name"
    se_notifications_enable_email = "spark.expectations.notifications.email.enabled"
    se_enable_obs_dq_report_result = "spark.expectations.notifications.observability.enabled"
    se_notifications_enable_custom_email_body = "spark.expectations.notifications.email.custom.body.enable"
    se_notifications_email_smtp_host = "spark.expectations.notifications.email.smtp.host"
    se_notifications_email_smtp_port = "spark.expectations.notifications.email.smtp.port"
    se_notifications_email_from = "spark.expectations.notifications.email.from"
    se_notifications_email_to_other_mail_id = "spark.expectations.notifications.email.to.other.mail.com"
    se_notifications_email_subject = "spark.expectations.notifications.email.subject"
    se_notifications_email_custom_body = "spark.expectations.notifications.email.custom.body"
    se_notifications_enable_templated_basic_email_body = (
        "spark.expectations.notifications.email.templated.basic.body.enable"
    )
    se_notifications_enable_templated_custom_email = (
        "spark.expectations.notifications.email.templated.custom.email.enable"
    )
    se_notifications_email_custom_template = "spark.expectations.notifications.email.custom.template"

    # declare const user config variables for slack notification
    se_notifications_enable_slack = "spark.expectations.notifications.slack.enabled"
    se_notifications_slack_webhook_url = "spark.expectations.notifications.slack.webhook.url"

    # declare const user config variables for teams notification
    se_notifications_enable_teams = "spark.expectations.notifications.teams.enabled"
    se_notifications_teams_webhook_url = "spark.expectations.notifications.teams.webhook.url"

    # declare const user config variables for zoom notification
    se_notifications_enable_zoom = "spark.expectations.notifications.zoom.enabled"
    se_notifications_zoom_webhook_url = "spark.expectations.notifications.zoom.webhook.url"
    se_notifications_zoom_token = "spark.expectations.notifications.zoom.token"

    # declare const user configs for pagerduty incidents
    se_notifications_enable_pagerduty = "spark.expectations.notifications.pagerduty.enabled"
    # For Pagerduty Services, your integration key is the routing key when making calls to the Events API v2.
    se_notifications_pagerduty_integration_key = "spark.expectations.notifications.pagerduty.integration.key"
    se_notifications_pagerduty_webhook_url = "spark.expectations.notifications.pagerduty.webhook.url"

    se_notifications_on_start = "spark.expectations.notifications.on.start"
    se_notifications_on_completion = "spark.expectations.notifications.on.completion"
    se_notifications_on_fail = "spark.expectations.notifications.on.fail"
    se_notifications_on_error_drop_exceeds_threshold_breach = (
        "spark.expectations.notifications.on.error.drop.exceeds.threshold.breach"
    )
    se_notifications_on_rules_action_if_failed_set_ignore = (
        "spark.expectations.notifications.on.rules.action.if.failed.set.ignore"
    )
    se_notifications_on_error_drop_threshold = "spark.expectations.notifications.error.drop.threshold"
    se_notifications_min_priority_slack = "spark.expectations.notifications.slack.min.priority"

    se_enable_streaming = "se.streaming.enable"
    se_enable_error_table = "se.enable.error.table"
    se_dq_rules_params = "se.dq.rules.params"
    se_streaming_stats_kafka_custom_config_enable = "se.streaming.stats.kafka.custom.config.enable"
    se_streaming_stats_topic_name = "se.streaming.stats.topic.name"
    se_streaming_stats_kafka_bootstrap_server = "se.streaming.stats.kafka.bootstrap.server"

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

    # declare const user config variables for agg query dq detailed stats
    se_enable_agg_dq_detailed_result = "spark.expectations.agg.dq.detailed.stats"
    se_enable_query_dq_detailed_result = "spark.expectations.query.dq.detailed.stats"
    se_job_metadata = "spark.expectations.job.metadata"

    querydq_output_custom_table_name = "spark.expectations.query.dq.custom.table_name"

    # declare const variable for agg query dq detailed stats

    se_agg_dq_expectation_regex_pattern = r"(\(.+?\)|\w+\(.+?\))(\s*[<>!=]+\s*.+|\s*between\s*.+)$"
    # declare const variable for range in agg query dq detailed stats
    # ex). count(*), count(), count(col1), sum(col1), col1
    allowed_functions = r"(\w+\(\*\)|\w+\(\w+\)|\w+)"
    and_clause = r"(\s+and\s+)"
    # ex). col1 > 1, col 1 < 10
    operator_with_value = r"(\s*[><]\s*\d+)"

    se_agg_dq_expectation_range_regex_pattern = (
        allowed_functions + operator_with_value + and_clause + allowed_functions + operator_with_value
    )