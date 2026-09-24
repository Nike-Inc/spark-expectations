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


def test_rest_max_retries_parses_int_and_string(spark):
    ctx = _ctx(spark, {user_config.se_streaming_rest_max_retries: 5})
    assert ctx.get_rest_max_retries == 5

    ctx = _ctx(spark, {user_config.se_streaming_rest_max_retries: "7"})
    assert ctx.get_rest_max_retries == 7


def test_rest_backoff_factor_defaults_and_parses(spark):
    ctx = _ctx(spark, {})
    assert ctx.get_rest_backoff_factor == 0.5

    ctx = _ctx(spark, {user_config.se_streaming_rest_backoff_factor: 1.25})
    assert ctx.get_rest_backoff_factor == 1.25

    ctx = _ctx(spark, {user_config.se_streaming_rest_backoff_factor: 2})
    assert ctx.get_rest_backoff_factor == 2.0

    ctx = _ctx(spark, {user_config.se_streaming_rest_backoff_factor: "0.75"})
    assert ctx.get_rest_backoff_factor == 0.75

    ctx = _ctx(spark, {user_config.se_streaming_rest_backoff_factor: "not-a-number"})
    assert ctx.get_rest_backoff_factor == 0.5


def test_rest_pool_connections_defaults_and_parses(spark):
    ctx = _ctx(spark, {})
    assert ctx.get_rest_pool_connections == 4

    ctx = _ctx(spark, {user_config.se_streaming_rest_pool_connections: 8})
    assert ctx.get_rest_pool_connections == 8

    ctx = _ctx(spark, {user_config.se_streaming_rest_pool_connections: "16"})
    assert ctx.get_rest_pool_connections == 16

    ctx = _ctx(spark, {user_config.se_streaming_rest_pool_connections: "not-a-number"})
    assert ctx.get_rest_pool_connections == 4


def test_rest_pool_maxsize_defaults_and_parses(spark):
    ctx = _ctx(spark, {})
    assert ctx.get_rest_pool_maxsize == 10

    ctx = _ctx(spark, {user_config.se_streaming_rest_pool_maxsize: 25})
    assert ctx.get_rest_pool_maxsize == 25

    ctx = _ctx(spark, {user_config.se_streaming_rest_pool_maxsize: "50"})
    assert ctx.get_rest_pool_maxsize == 50

    ctx = _ctx(spark, {user_config.se_streaming_rest_pool_maxsize: "not-a-number"})
    assert ctx.get_rest_pool_maxsize == 10


def test_rest_connect_timeout_sec_defaults_to_scalar_timeout(spark):
    # Falls back to get_rest_timeout_sec when connect_timeout_sec is absent.
    ctx = _ctx(spark, {user_config.se_streaming_rest_timeout_sec: 45})
    assert ctx.get_rest_connect_timeout_sec == 45

    ctx = _ctx(
        spark,
        {
            user_config.se_streaming_rest_timeout_sec: 45,
            user_config.se_streaming_rest_connect_timeout_sec: 10,
        },
    )
    assert ctx.get_rest_connect_timeout_sec == 10

    ctx = _ctx(spark, {user_config.se_streaming_rest_connect_timeout_sec: "12"})
    assert ctx.get_rest_connect_timeout_sec == 12

    # Invalid string falls back to scalar timeout, which itself defaults to 30.
    ctx = _ctx(spark, {user_config.se_streaming_rest_connect_timeout_sec: "not-a-number"})
    assert ctx.get_rest_connect_timeout_sec == 30


def test_rest_read_timeout_sec_defaults_to_scalar_timeout(spark):
    ctx = _ctx(spark, {user_config.se_streaming_rest_timeout_sec: 60})
    assert ctx.get_rest_read_timeout_sec == 60

    ctx = _ctx(
        spark,
        {
            user_config.se_streaming_rest_timeout_sec: 60,
            user_config.se_streaming_rest_read_timeout_sec: 20,
        },
    )
    assert ctx.get_rest_read_timeout_sec == 20

    ctx = _ctx(spark, {user_config.se_streaming_rest_read_timeout_sec: "22"})
    assert ctx.get_rest_read_timeout_sec == 22

    ctx = _ctx(spark, {user_config.se_streaming_rest_read_timeout_sec: "not-a-number"})
    assert ctx.get_rest_read_timeout_sec == 30


def test_transport_falls_back_to_default_for_non_string(spark):
    # Non-string values (e.g., accidentally set to a bool or int) should be ignored.
    ctx = _ctx(spark, {user_config.se_streaming_transport: 42})
    assert ctx.get_streaming_transport == "kafka_native"

    ctx = _ctx(spark, {user_config.se_streaming_transport: ""})
    assert ctx.get_streaming_transport == "kafka_native"


def test_rest_base_url_and_topic_return_none_when_absent(spark):
    ctx = _ctx(spark, {})
    assert ctx.get_rest_base_url_key is None
    assert ctx.get_rest_base_url_direct is None
    assert ctx.get_rest_topic_key is None
    assert ctx.get_rest_topic_direct is None


# ---------------------------------------------------------------------------
# full_url resolver — parallel to base_url. Enables HTTP-ingress endpoints
# where the stream URL is the produce endpoint and no
# ``/topics/{topic}`` segment is appended.
# ---------------------------------------------------------------------------


def test_rest_full_url_direct_only(spark):
    ctx = _ctx(
        spark,
        {
            user_config.se_streaming_rest_full_url: (
                "https://http-ingress.example.com/rest"
            ),
        },
    )
    assert ctx.get_rest_full_url_key is None
    assert ctx.get_rest_full_url_direct == (
        "https://http-ingress.example.com/rest"
    )


def test_rest_full_url_dbx_secret_key(spark):
    ctx = _ctx(
        spark,
        {
            user_config.secret_type: "databricks",
            user_config.dbx_rest_full_url: "dbx_rest_full_url_secret_key",
        },
    )
    assert ctx.get_rest_full_url_key == "dbx_rest_full_url_secret_key"


def test_rest_full_url_cerberus_secret_key(spark):
    ctx = _ctx(
        spark,
        {
            user_config.secret_type: "cerberus",
            user_config.cbs_rest_full_url: "cbs_rest_full_url_secret_key",
        },
    )
    assert ctx.get_rest_full_url_key == "cbs_rest_full_url_secret_key"


def test_rest_full_url_wrong_secret_type_returns_none(spark):
    # secret_type=databricks but only cerberus key is populated → no key
    # (mirrors the URL/topic behaviour).
    ctx = _ctx(
        spark,
        {
            user_config.secret_type: "databricks",
            user_config.cbs_rest_full_url: "cbs_rest_full_url_secret_key",
            user_config.se_streaming_rest_full_url: (
                "https://direct.example.com/rest"
            ),
        },
    )
    assert ctx.get_rest_full_url_key is None
    assert ctx.get_rest_full_url_direct == "https://direct.example.com/rest"


def test_rest_full_url_absent(spark):
    ctx = _ctx(spark, {})
    assert ctx.get_rest_full_url_key is None
    assert ctx.get_rest_full_url_direct is None


def test_rest_full_url_ignores_non_string_values(spark):
    ctx = _ctx(spark, {user_config.se_streaming_rest_full_url: 123})
    assert ctx.get_rest_full_url_direct is None


def test_rest_base_url_and_topic_ignore_non_string_values(spark):
    # Non-string values should not be returned as if they were configured.
    ctx = _ctx(
        spark,
        {
            user_config.se_streaming_rest_base_url: 123,
            user_config.se_streaming_rest_topic_name: False,
        },
    )
    assert ctx.get_rest_base_url_direct is None
    assert ctx.get_rest_topic_direct is None


# ---------------------------------------------------------------------------
# Auth getters (Phase 2 / PR-4-auth). These MUST default to a shape that
# lets get_kafka_rest_write_options short-circuit without touching any
# credential-related config or the secrets backend.
# ---------------------------------------------------------------------------


def test_rest_auth_type_defaults_to_none_when_unset(spark):
    ctx = _ctx(spark, {})
    assert ctx.get_rest_auth_type == "none"


def test_rest_auth_type_defaults_to_none_when_empty(spark):
    ctx = _ctx(spark, {user_config.se_streaming_rest_auth_type: "   "})
    assert ctx.get_rest_auth_type == "none"


def test_rest_auth_type_lowercased_and_stripped(spark):
    ctx = _ctx(spark, {user_config.se_streaming_rest_auth_type: "  BASIC  "})
    assert ctx.get_rest_auth_type == "basic"


def test_rest_auth_type_unknown_value_defaults_to_none(spark, caplog):
    ctx = _ctx(spark, {user_config.se_streaming_rest_auth_type: "mTLS"})
    with caplog.at_level("WARNING"):
        assert ctx.get_rest_auth_type == "none"
    assert any("mtls" in rec.message.lower() for rec in caplog.records if rec.levelname == "WARNING")


def test_rest_username_absent(spark):
    ctx = _ctx(spark, {})
    assert ctx.get_rest_username is None


def test_rest_username_empty_string_treated_as_absent(spark):
    ctx = _ctx(spark, {user_config.se_streaming_rest_username: ""})
    assert ctx.get_rest_username is None


def test_rest_username_present(spark):
    ctx = _ctx(spark, {user_config.se_streaming_rest_username: "svc_dq"})
    assert ctx.get_rest_username == "svc_dq"


def test_rest_auth_secret_key_absent(spark):
    ctx = _ctx(spark, {})
    assert ctx.get_rest_auth_secret_key is None


def test_rest_auth_secret_key_dbx(spark):
    ctx = _ctx(
        spark,
        {
            user_config.secret_type: "databricks",
            user_config.dbx_rest_auth_secret: "dbx_rest_auth_secret_key",
        },
    )
    assert ctx.get_rest_auth_secret_key == "dbx_rest_auth_secret_key"


def test_rest_auth_secret_key_cerberus(spark):
    ctx = _ctx(
        spark,
        {
            user_config.secret_type: "cerberus",
            user_config.cbs_rest_auth_secret: "cbs_rest_auth_secret_key",
        },
    )
    assert ctx.get_rest_auth_secret_key == "cbs_rest_auth_secret_key"


def test_rest_auth_secret_key_wrong_secret_type_returns_none(spark):
    # secret_type=databricks but only cerberus key is populated → no key
    # (mirrors the URL/topic behaviour so the writer utility's lazy-read
    # invariant is deterministic).
    ctx = _ctx(
        spark,
        {
            user_config.secret_type: "databricks",
            user_config.cbs_rest_auth_secret: "cbs_rest_auth_secret_key",
        },
    )
    assert ctx.get_rest_auth_secret_key is None
