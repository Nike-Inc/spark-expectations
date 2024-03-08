from spark_expectations.config.user_config import Constants as user_config


def test_constants():
    assert user_config.se_notifications_email_smtp_port == "spark.expectations.notifications.email.smtp_port"

    assert user_config.se_notifications_email_from == "spark.expectations.notifications.email.from"

    assert user_config.se_notifications_email_to_other_mail_id == "spark.expectations.notifications.email." \
                                                                  "to.other.mail.com"

    assert user_config.se_notifications_email_subject == "spark.expectations.notifications.email.subject"

    assert user_config.se_notifications_enable_slack == "spark.expectations.notifications.slack.enabled"

    assert user_config.se_notifications_slack_webhook_url == "spark.expectations.notifications.slack.webhook_url"

    assert user_config.se_notifications_on_start == "spark.expectations.notifications.on_start"

    assert user_config.se_notifications_on_completion == "spark.expectations.notifications.on.completion"

    assert user_config.se_notifications_on_fail == "spark.expectations.notifications.on.fail"

    assert user_config.se_notifications_on_error_drop_exceeds_threshold_breach == "spark.expectations." \
                                                                                  "notifications.on.error.drop." \
                                                                                  "exceeds.threshold.breach"

    assert user_config.se_notifications_on_error_drop_threshold == "spark.expectations.notifications." \
                                                                   "error.drop.threshold"

    assert user_config.se_enable_streaming == "se.enable.streaming"

    assert user_config.se_enable_error_table == "se.enable.error.table"

    assert user_config.se_dq_rules_params == "se.dq.rules.params"

    assert user_config.secret_type == "se.streaming.secret.type"

    assert user_config.cbs_url == "se.streaming.cerberus.url"
    assert user_config.cbs_sdb_path == "se.streaming.cerberus.sdb.path"
    assert user_config.cbs_kafka_server_url == "se.streaming.cerberus.kafka.server.url"
    assert user_config.cbs_secret_token_url == "se.streaming.cbs.secret.token.url"
    assert user_config.cbs_secret_app_name == "se.streaming.cbs.secret.app.name"
    assert user_config.cbs_secret_token == "se.streaming.cerberus.secret.token"
    assert user_config.cbs_topic_name == "se.streaming.cerberus.token.name"

    assert user_config.dbx_workspace_url == "se.streaming.dbx.workspace.url"
    assert user_config.dbx_secret_scope == "se.streaming.dbx.secret.scope"
    assert user_config.dbx_kafka_server_url == "se.streaming.dbx.kafka.server.url"
    assert user_config.dbx_secret_token_url == "se.streaming.dbx.secret.token.url"
    assert user_config.dbx_secret_app_name == "se.streaming.dbx.secret.app.name"
    assert user_config.dbx_secret_token == "se.streaming.dbx.secret.token"
    assert user_config.dbx_topic_name == "se.streaming.dbx.topic.name"
