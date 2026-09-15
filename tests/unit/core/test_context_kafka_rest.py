from spark_expectations.config.user_config import Constants as user_config
from spark_expectations.core.context import SparkExpectationsContext


def _ctx(spark, stats_dict):
    ctx = SparkExpectationsContext(product_id="p1", spark=spark)
    ctx.set_se_streaming_stats_dict(stats_dict)
    return ctx


def test_transport_default_when_unset(spark):
    ctx = _ctx(spark, {})
    assert ctx.get_streaming_transport == "kafka_native"


def test_transport_explicit_kafka_rest(spark):
    ctx = _ctx(spark, {user_config.se_streaming_transport: "kafka_rest"})
    assert ctx.get_streaming_transport == "kafka_rest"


def test_rest_base_url_direct_only(spark):
    ctx = _ctx(
        spark,
        {user_config.se_streaming_rest_base_url: "https://kafka-rest.example.com"},
    )
    assert ctx.get_rest_base_url_key is None
    assert ctx.get_rest_base_url_direct == "https://kafka-rest.example.com"


def test_rest_base_url_dbx_secret_key_wins_over_direct(spark):
    ctx = _ctx(
        spark,
        {
            user_config.secret_type: "databricks",
            user_config.dbx_rest_base_url: "dbx_rest_base_url_secret_key",
            user_config.se_streaming_rest_base_url: "https://kafka-rest.example.com",
        },
    )
    assert ctx.get_rest_base_url_key == "dbx_rest_base_url_secret_key"
    assert ctx.get_rest_base_url_direct == "https://kafka-rest.example.com"


def test_rest_base_url_cerberus_secret_key(spark):
    ctx = _ctx(
        spark,
        {
            user_config.secret_type: "cerberus",
            user_config.cbs_rest_base_url: "cbs_rest_base_url_secret_key",
        },
    )
    assert ctx.get_rest_base_url_key == "cbs_rest_base_url_secret_key"


def test_rest_base_url_wrong_secret_type_falls_back_to_direct(spark):
    ctx = _ctx(
        spark,
        {
            user_config.secret_type: "databricks",
            user_config.cbs_rest_base_url: "ignored_cerberus_key",
            user_config.se_streaming_rest_base_url: "https://kafka-rest.example.com",
        },
    )
    assert ctx.get_rest_base_url_key is None
    assert ctx.get_rest_base_url_direct == "https://kafka-rest.example.com"


def test_rest_topic_direct_and_secret_variants(spark):
    ctx = _ctx(
        spark,
        {user_config.se_streaming_rest_topic_name: "dq-topic"},
    )
    assert ctx.get_rest_topic_key is None
    assert ctx.get_rest_topic_direct == "dq-topic"

    ctx = _ctx(
        spark,
        {
            user_config.secret_type: "databricks",
            user_config.dbx_rest_topic_name: "dbx_topic_secret_key",
        },
    )
    assert ctx.get_rest_topic_key == "dbx_topic_secret_key"

    ctx = _ctx(
        spark,
        {
            user_config.secret_type: "cerberus",
            user_config.cbs_rest_topic_name: "cbs_topic_secret_key",
        },
    )
    assert ctx.get_rest_topic_key == "cbs_topic_secret_key"


def test_rest_embedded_format_defaults_to_json(spark):
    ctx = _ctx(spark, {})
    assert ctx.get_rest_embedded_format == "json"

    ctx = _ctx(spark, {user_config.se_streaming_rest_embedded_format: "binary"})
    assert ctx.get_rest_embedded_format == "binary"


def test_rest_api_version_defaults_to_v2(spark):
    ctx = _ctx(spark, {})
    assert ctx.get_rest_api_version == "v2"

    ctx = _ctx(spark, {user_config.se_streaming_rest_api_version: "v3"})
    assert ctx.get_rest_api_version == "v3"


def test_rest_timeout_sec_defaults_and_parses(spark):
    ctx = _ctx(spark, {})
    assert ctx.get_rest_timeout_sec == 30

    ctx = _ctx(spark, {user_config.se_streaming_rest_timeout_sec: 15})
    assert ctx.get_rest_timeout_sec == 15

    ctx = _ctx(spark, {user_config.se_streaming_rest_timeout_sec: "45"})
    assert ctx.get_rest_timeout_sec == 45

    ctx = _ctx(spark, {user_config.se_streaming_rest_timeout_sec: "not-a-number"})
    assert ctx.get_rest_timeout_sec == 30


def test_rest_verify_ssl_defaults_true_and_parses_false(spark):
    ctx = _ctx(spark, {})
    assert ctx.get_rest_verify_ssl is True

    ctx = _ctx(spark, {user_config.se_streaming_rest_verify_ssl: False})
    assert ctx.get_rest_verify_ssl is False

    ctx = _ctx(spark, {user_config.se_streaming_rest_verify_ssl: "false"})
    assert ctx.get_rest_verify_ssl is False

    ctx = _ctx(spark, {user_config.se_streaming_rest_verify_ssl: "TRUE"})
    assert ctx.get_rest_verify_ssl is True


def test_rest_max_retries_defaults_to_three(spark):
    ctx = _ctx(spark, {})
    assert ctx.get_rest_max_retries == 3

    ctx = _ctx(spark, {user_config.se_streaming_rest_max_retries: "not-a-number"})
    assert ctx.get_rest_max_retries == 3
