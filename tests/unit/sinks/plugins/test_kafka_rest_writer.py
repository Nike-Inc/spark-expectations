import json
from unittest.mock import MagicMock, patch

import pytest
import requests
from urllib3.util.retry import Retry

from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.sinks.plugins.kafka_rest_writer import (
    SparkExpectationsKafkaRestWritePluginImpl,
    _build_rest_headers,
    _build_session,
    _normalize_api_version,
    _normalize_embedded_format,
    _resolve_timeout,
    _retry_count_from_response,
)


def _fake_stats_df(rows):
    stats_df = MagicMock()
    projected = MagicMock()
    stats_df.selectExpr.return_value = projected
    projected.collect.return_value = [{"value": r} for r in rows]
    return stats_df


def _response(status_code=200, json_body=None, text=""):
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    resp.content = json.dumps(json_body).encode("utf-8") if json_body is not None else b""
    resp.json.return_value = json_body if json_body is not None else {}
    return resp


def _mock_session(post_return=None, post_side_effect=None):
    session = MagicMock()
    if post_side_effect is not None:
        session.post.side_effect = post_side_effect
    else:
        session.post.return_value = post_return
    return session


def _write_args(**overrides):
    payload = {"product_id": "p1", "count": 1}
    args = {
        "product_id": "p1",
        "enable_se_streaming": True,
        "transport": "kafka_rest",
        "stats_df": _fake_stats_df([json.dumps(payload)]),
        "rest_write_options": {
            "base_url": "https://kafka-rest.example.com",
            "topic": "dq-stats",
            "embedded_format": "json",
            "api_version": "v2",
            "timeout_sec": 30,
            "verify_ssl": True,
        },
    }
    args.update(overrides)
    return args


def _decode_post_body(data):
    if isinstance(data, bytes):
        return json.loads(data.decode("utf-8"))
    return json.loads(data)


def _assert_record(body, expected_value, key_prefix="p1:", key_suffix=":0"):
    """Assert the Confluent v2 payload envelope for a single record.

    The message key contains a run-timestamp generated at write time, so tests
    verify its shape (``{product_id}:{timestamp}:{row_index}``) rather than
    an exact string. ``key_prefix`` pins the product_id + separator, and
    ``key_suffix`` pins the row index at the end of the key.
    """
    assert isinstance(body, dict) and "records" in body
    assert isinstance(body["records"], list) and len(body["records"]) == 1
    record = body["records"][0]
    assert record["value"] == expected_value
    assert "key" in record, "REST proxy record must always include a message key"
    assert isinstance(record["key"], str)
    assert record["key"].startswith(key_prefix), (
        f"key {record['key']!r} does not start with {key_prefix!r}"
    )
    assert record["key"].endswith(key_suffix), (
        f"key {record['key']!r} does not end with {key_suffix!r}"
    )


def test_build_rest_headers_json_v2():
    headers = _build_rest_headers("v2", "json")
    assert headers == {
        "Content-Type": "application/vnd.kafka.json.v2+json",
        "Accept": "application/vnd.kafka.v2+json",
    }


def test_normalize_api_version_rejects_v3():
    with pytest.raises(SparkExpectationsMiscException, match="only 'v2' is supported"):
        _normalize_api_version("v3")


def test_normalize_api_version_rejects_invalid():
    with pytest.raises(SparkExpectationsMiscException, match="only 'v2' is supported"):
        _normalize_api_version("v1")


def test_normalize_embedded_format_rejects_binary():
    with pytest.raises(SparkExpectationsMiscException, match="only 'json' is supported"):
        _normalize_embedded_format("binary")


def test_normalize_embedded_format_rejects_avro():
    with pytest.raises(SparkExpectationsMiscException, match="only 'json' is supported"):
        _normalize_embedded_format("avro")


def test_normalize_api_version_accepts_v2_variants():
    # Case + optional leading "v" should all normalize to "v2".
    assert _normalize_api_version("v2") == "v2"
    assert _normalize_api_version("V2") == "v2"
    assert _normalize_api_version("2") == "v2"
    assert _normalize_api_version("  v2  ") == "v2"


def test_normalize_embedded_format_accepts_json_variants():
    assert _normalize_embedded_format("json") == "json"
    assert _normalize_embedded_format("JSON") == "json"
    assert _normalize_embedded_format("  json  ") == "json"


def test_writer_posts_v2_json_body_and_headers():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    mock_session = _mock_session(
        post_return=_response(200, {"offsets": [{"partition": 0, "offset": 1, "error_code": None}]}),
    )
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        plugin.writer(_write_args=_write_args())

    assert mock_session.post.call_count == 1
    mock_session.close.assert_called_once()
    call = mock_session.post.call_args
    assert call.args[0] == "https://kafka-rest.example.com/topics/dq-stats"
    assert call.kwargs["headers"]["Content-Type"] == "application/vnd.kafka.json.v2+json"
    assert call.kwargs["headers"]["Accept"] == "application/vnd.kafka.v2+json"
    assert call.kwargs["timeout"] == (30.0, 30.0)
    assert call.kwargs["verify"] is True
    body = _decode_post_body(call.kwargs["data"])
    _assert_record(body, expected_value={"product_id": "p1", "count": 1})


def test_writer_rejects_binary_embedded_format():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _write_args()
    args["rest_write_options"]["embedded_format"] = "binary"
    with patch("spark_expectations.sinks.plugins.kafka_rest_writer._build_session") as mock_build_session:
        with pytest.raises(SparkExpectationsMiscException, match="only 'json' is supported"):
            plugin.writer(_write_args=args)
    mock_build_session.assert_not_called()


def test_writer_rejects_v3_api_version():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _write_args()
    args["rest_write_options"]["api_version"] = "v3"
    with patch("spark_expectations.sinks.plugins.kafka_rest_writer._build_session") as mock_build_session:
        with pytest.raises(SparkExpectationsMiscException, match="only 'v2' is supported"):
            plugin.writer(_write_args=args)
    mock_build_session.assert_not_called()


def test_writer_noops_when_transport_is_native():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session"
    ) as mock_build_session:
        plugin.writer(_write_args=_write_args(transport="kafka_native"))
    mock_build_session.assert_not_called()


def test_writer_noops_when_streaming_disabled():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session"
    ) as mock_build_session:
        plugin.writer(_write_args=_write_args(enable_se_streaming=False))
    mock_build_session.assert_not_called()


def test_writer_raises_on_missing_base_url_or_topic():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _write_args()
    args["rest_write_options"] = {"base_url": "", "topic": "dq-stats"}
    with pytest.raises(SparkExpectationsMiscException, match="'base_url' and 'topic' are required"):
        plugin.writer(_write_args=args)


def test_writer_raises_on_http_error_status():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    mock_session = _mock_session(post_return=_response(500, text="boom"))
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        with pytest.raises(SparkExpectationsMiscException, match="REST proxy HTTP 500 for topic 'dq-stats'"):
            plugin.writer(_write_args=_write_args())


def test_writer_raises_on_per_record_error_code_with_http_200():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    body = {"offsets": [{"partition": 0, "offset": None, "error_code": 50003, "error": "retriable"}]}
    mock_session = _mock_session(post_return=_response(200, body))
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        with pytest.raises(SparkExpectationsMiscException, match="REST proxy record error for topic 'dq-stats'"):
            plugin.writer(_write_args=_write_args())


def test_writer_raises_on_request_exception():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    mock_session = _mock_session(post_side_effect=RuntimeError("network down"))
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        with pytest.raises(SparkExpectationsMiscException, match="network down"):
            plugin.writer(_write_args=_write_args())


def test_writer_iterates_all_rows():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _write_args()
    args["stats_df"] = _fake_stats_df([json.dumps({"i": i}) for i in range(3)])
    mock_session = _mock_session(
        post_return=_response(200, {"offsets": [{"partition": 0, "offset": 1, "error_code": None}]}),
    )
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        plugin.writer(_write_args=args)
    assert mock_session.post.call_count == 3
    for idx, call in enumerate(mock_session.post.call_args_list):
        _assert_record(
            _decode_post_body(call.kwargs["data"]),
            expected_value={"i": idx},
            key_suffix=f":{idx}",
        )


# ---------------------------------------------------------------------------
# _resolve_timeout
# ---------------------------------------------------------------------------


def test_resolve_timeout_defaults_to_scalar_timeout_sec():
    # Legacy scalar `timeout_sec` is applied to both connect and read.
    assert _resolve_timeout({"timeout_sec": 45}) == (45.0, 45.0)


def test_resolve_timeout_uses_split_connect_and_read_overrides():
    resolved = _resolve_timeout(
        {
            "timeout_sec": 30,
            "connect_timeout_sec": 5,
            "read_timeout_sec": 60,
        }
    )
    assert resolved == (5.0, 60.0)


def test_resolve_timeout_falls_back_to_default_when_absent():
    from spark_expectations.config.rest_streaming_defaults import DEFAULT_REST_TIMEOUT_SEC

    assert _resolve_timeout({}) == (float(DEFAULT_REST_TIMEOUT_SEC), float(DEFAULT_REST_TIMEOUT_SEC))


def test_writer_uses_split_connect_and_read_timeouts():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _write_args()
    args["rest_write_options"]["connect_timeout_sec"] = 3
    args["rest_write_options"]["read_timeout_sec"] = 90
    mock_session = _mock_session(
        post_return=_response(200, {"offsets": [{"partition": 0, "offset": 1, "error_code": None}]}),
    )
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        plugin.writer(_write_args=args)
    assert mock_session.post.call_args.kwargs["timeout"] == (3.0, 90.0)


# ---------------------------------------------------------------------------
# _build_session — retry + connection pool wiring
# ---------------------------------------------------------------------------


def test_build_session_configures_retry_and_pool():
    session = _build_session(
        {
            "max_retries": 5,
            "backoff_factor": 1.25,
            "pool_connections": 7,
            "pool_maxsize": 21,
        }
    )
    try:
        assert isinstance(session, requests.Session)
        adapter_https = session.get_adapter("https://example.com")
        adapter_http = session.get_adapter("http://example.com")
        # Same HTTPAdapter class mounted on both schemes.
        assert type(adapter_https).__name__ == "HTTPAdapter"
        assert type(adapter_http).__name__ == "HTTPAdapter"

        # Pool sizes are stored on the adapter.
        assert adapter_https._pool_connections == 7
        assert adapter_https._pool_maxsize == 21

        # Retry policy is attached and covers all four channels.
        retry = adapter_https.max_retries
        assert isinstance(retry, Retry)
        assert retry.total == 5
        assert retry.connect == 5
        assert retry.read == 5
        assert retry.status == 5
        assert retry.backoff_factor == 1.25
        # POST must be retried explicitly (urllib3 excludes it by default).
        assert "POST" in {m.upper() for m in retry.allowed_methods}
        # Retryable HTTP statuses are respected.
        for code in (408, 429, 500, 502, 503, 504):
            assert code in retry.status_forcelist
        assert retry.raise_on_status is False
    finally:
        session.close()


def test_build_session_uses_defaults_when_options_missing():
    from spark_expectations.config.rest_streaming_defaults import (
        DEFAULT_REST_BACKOFF_FACTOR,
        DEFAULT_REST_MAX_RETRIES,
        DEFAULT_REST_POOL_CONNECTIONS,
        DEFAULT_REST_POOL_MAXSIZE,
    )

    session = _build_session({})
    try:
        adapter = session.get_adapter("https://example.com")
        retry = adapter.max_retries
        assert retry.total == DEFAULT_REST_MAX_RETRIES
        assert retry.backoff_factor == DEFAULT_REST_BACKOFF_FACTOR
        assert adapter._pool_connections == DEFAULT_REST_POOL_CONNECTIONS
        assert adapter._pool_maxsize == DEFAULT_REST_POOL_MAXSIZE
    finally:
        session.close()


# ---------------------------------------------------------------------------
# _retry_count_from_response
# ---------------------------------------------------------------------------


def test_retry_count_from_response_returns_zero_when_no_raw():
    response = MagicMock(spec=requests.Response)
    response.raw = None
    assert _retry_count_from_response(response) == 0


def test_retry_count_from_response_returns_zero_when_no_history():
    response = MagicMock(spec=requests.Response)
    response.raw = MagicMock()
    response.raw.retries = MagicMock()
    response.raw.retries.history = ()
    assert _retry_count_from_response(response) == 0


def test_retry_count_from_response_returns_history_length():
    response = MagicMock(spec=requests.Response)
    response.raw = MagicMock()
    response.raw.retries = MagicMock()
    # Two retry attempts recorded before final response.
    response.raw.retries.history = (MagicMock(), MagicMock())
    assert _retry_count_from_response(response) == 2


def test_retry_count_from_response_handles_missing_retries_attr():
    response = MagicMock(spec=requests.Response)
    response.raw = MagicMock(spec=[])  # no `retries` attribute
    assert _retry_count_from_response(response) == 0


def test_writer_logs_warning_on_retried_response():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    response = _response(200, {"offsets": [{"partition": 0, "offset": 1, "error_code": None}]})
    # Simulate urllib3 retry history so _retry_count_from_response returns 2.
    response.raw = MagicMock()
    response.raw.retries = MagicMock()
    response.raw.retries.history = (MagicMock(), MagicMock())
    mock_session = _mock_session(post_return=response)

    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ), patch("spark_expectations.sinks.plugins.kafka_rest_writer._log") as mock_log:
        plugin.writer(_write_args=_write_args())

    # Warning line should mention "retried 2 time(s)".
    warning_calls = [c.args[0] for c in mock_log.warning.call_args_list]
    assert any("retried 2 time(s)" in msg for msg in warning_calls)
    # Final info log includes total_retries=2.
    info_calls = [c.args[0] for c in mock_log.info.call_args_list]
    assert any("total_retries=2" in msg for msg in info_calls)


# ---------------------------------------------------------------------------
# _raise_for_record_errors — additional branches
# ---------------------------------------------------------------------------


def test_writer_raises_on_top_level_error_code_with_http_200():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    # No offsets array, but a top-level error_code should still raise.
    mock_session = _mock_session(
        post_return=_response(200, {"error_code": 40403, "message": "topic not found"}),
    )
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        with pytest.raises(SparkExpectationsMiscException, match="REST proxy record error for topic 'dq-stats'"):
            plugin.writer(_write_args=_write_args())


def test_writer_accepts_empty_response_body():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    # HTTP 200 with an empty body should not raise (no offsets, no error_code).
    resp = MagicMock()
    resp.status_code = 200
    resp.text = ""
    resp.content = b""
    resp.json.return_value = {}
    mock_session = _mock_session(post_return=resp)
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        plugin.writer(_write_args=_write_args())
    assert mock_session.post.call_count == 1


def test_writer_tolerates_non_json_response_body():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    # HTTP 200 but body isn't valid JSON — code path should swallow ValueError
    # and treat payload as empty.
    resp = MagicMock()
    resp.status_code = 200
    resp.text = "<html>oops</html>"
    resp.content = b"<html>oops</html>"
    resp.json.side_effect = ValueError("not json")
    mock_session = _mock_session(post_return=resp)
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        plugin.writer(_write_args=_write_args())
    assert mock_session.post.call_count == 1


# ---------------------------------------------------------------------------
# Additional guard-clause coverage
# ---------------------------------------------------------------------------


def test_writer_noops_when_transport_is_missing():
    # No `transport` key at all (equivalent to None) must be treated as non-REST.
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _write_args()
    args.pop("transport")
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session"
    ) as mock_build_session:
        plugin.writer(_write_args=args)
    mock_build_session.assert_not_called()


def test_writer_raises_when_rest_options_missing_entirely():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _write_args()
    args["rest_write_options"] = None
    with pytest.raises(SparkExpectationsMiscException, match="'base_url' and 'topic' are required"):
        plugin.writer(_write_args=args)


def test_writer_raises_when_only_topic_missing():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _write_args()
    args["rest_write_options"] = {"base_url": "https://kafka-rest.example.com"}
    with pytest.raises(SparkExpectationsMiscException, match="'base_url' and 'topic' are required"):
        plugin.writer(_write_args=args)


def test_writer_defaults_api_version_and_embedded_format():
    # When rest_write_options omits api_version / embedded_format, the writer
    # should fall back to the module-level defaults (v2 / json) and still succeed.
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _write_args()
    args["rest_write_options"] = {
        "base_url": "https://kafka-rest.example.com",
        "topic": "dq-stats",
        "verify_ssl": False,
    }
    mock_session = _mock_session(
        post_return=_response(200, {"offsets": [{"partition": 0, "offset": 1, "error_code": None}]}),
    )
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        plugin.writer(_write_args=args)
    call = mock_session.post.call_args
    assert call.kwargs["headers"]["Content-Type"] == "application/vnd.kafka.json.v2+json"
    assert call.kwargs["verify"] is False


def test_writer_strips_trailing_slash_from_base_url():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _write_args()
    args["rest_write_options"]["base_url"] = "https://kafka-rest.example.com/"
    mock_session = _mock_session(
        post_return=_response(200, {"offsets": [{"partition": 0, "offset": 1, "error_code": None}]}),
    )
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        plugin.writer(_write_args=args)
    # No double slash in the resolved URL.
    assert mock_session.post.call_args.args[0] == "https://kafka-rest.example.com/topics/dq-stats"

def test_writer_default_no_auth_regression_matches_pr324():
    """When rest_write_options has neither ``auth`` nor ``auth_headers``,
    the emitted POST MUST:
    ``auth=None`` and only Content-Type / Accept in headers.
    """
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    mock_session = _mock_session(
        post_return=_response(200, {"offsets": [{"partition": 0, "offset": 1, "error_code": None}]}),
    )
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        plugin.writer(_write_args=_write_args())

    call = mock_session.post.call_args
    # auth kwarg is present but None (byte-compat with pre-auth path).
    assert call.kwargs.get("auth") is None
    headers = call.kwargs["headers"]
    assert "Authorization" not in headers
    # Content-Type / Accept preserved.
    assert headers["Content-Type"] == "application/vnd.kafka.json.v2+json"
    assert headers["Accept"] == "application/vnd.kafka.v2+json"


def test_writer_passes_basic_auth_tuple_to_requests_post():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _write_args()
    args["rest_write_options"]["auth"] = ("svc_dq", "s3cret")
    mock_session = _mock_session(
        post_return=_response(200, {"offsets": [{"partition": 0, "offset": 1, "error_code": None}]}),
    )
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        plugin.writer(_write_args=args)

    call = mock_session.post.call_args
    assert call.kwargs["auth"] == ("svc_dq", "s3cret")
    # basic auth does NOT add an Authorization header itself; requests handles it.
    assert "Authorization" not in call.kwargs["headers"]


def test_writer_merges_bearer_authorization_header_without_overwriting_content_type():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _write_args()
    args["rest_write_options"]["auth_headers"] = {"Authorization": "Bearer tok-abc"}
    mock_session = _mock_session(
        post_return=_response(200, {"offsets": [{"partition": 0, "offset": 1, "error_code": None}]}),
    )
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        plugin.writer(_write_args=args)

    call = mock_session.post.call_args
    headers = call.kwargs["headers"]
    assert headers["Authorization"] == "Bearer tok-abc"
    # Auth headers must NOT overwrite the REST-protocol headers.
    assert headers["Content-Type"] == "application/vnd.kafka.json.v2+json"
    assert headers["Accept"] == "application/vnd.kafka.v2+json"
    # bearer path does NOT populate the requests-level auth kwarg.
    assert call.kwargs.get("auth") is None


def test_writer_auth_headers_cannot_override_protocol_headers():
    """Even if a caller tries to inject a rogue Content-Type via auth_headers,
    the protocol headers from _build_rest_headers win."""
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _write_args()
    args["rest_write_options"]["auth_headers"] = {
        "Authorization": "Bearer tok",
        "Content-Type": "text/plain",  # attempted override
    }
    mock_session = _mock_session(
        post_return=_response(200, {"offsets": [{"partition": 0, "offset": 1, "error_code": None}]}),
    )
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        plugin.writer(_write_args=args)

    headers = mock_session.post.call_args.kwargs["headers"]
    assert headers["Content-Type"] == "application/vnd.kafka.json.v2+json"
    assert headers["Authorization"] == "Bearer tok"

def test_writer_applies_se_job_metadata_struct_helper_before_serialisation():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _write_args()
    original_stats_df = args["stats_df"]

    mock_session = _mock_session(
        post_return=_response(200, {"offsets": [{"partition": 0, "offset": 1, "error_code": None}]}),
    )
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer.apply_se_job_metadata_struct",
        return_value=original_stats_df,
    ) as mock_apply, patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        plugin.writer(_write_args=args)

    # Helper is invoked exactly once, with the stats DataFrame the writer
    # received — this pins the parity contract with the native transport.
    mock_apply.assert_called_once_with(original_stats_df)
    # And serialisation still runs against the (possibly rewritten) DataFrame.
    original_stats_df.selectExpr.assert_called_once_with("to_json(struct(*)) AS value")


# ---------------------------------------------------------------------------
# full_url mode — HTTP-ingress endpoints (e.g. Nike NSP3) that treat the
# stream URL as the produce endpoint and do NOT expect /topics/{topic}
# to be appended.
# ---------------------------------------------------------------------------


def _full_url_args(**overrides):
    """Build ``_write_args`` for ``full_url`` mode (no base_url + topic)."""
    payload = {"product_id": "p1", "count": 1}
    args = {
        "product_id": "p1",
        "enable_se_streaming": True,
        "transport": "kafka_rest",
        "stats_df": _fake_stats_df([json.dumps(payload)]),
        "rest_write_options": {
            "full_url": "https://http-ingress.example.com/rest",
            "embedded_format": "json",
            "api_version": "v2",
            "timeout_sec": 30,
            "verify_ssl": True,
        },
    }
    args.update(overrides)
    return args


def test_writer_uses_full_url_verbatim_without_appending_topics_segment():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    mock_session = _mock_session(
        post_return=_response(200, {"offsets": [{"partition": 0, "offset": 1, "error_code": None}]}),
    )
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        plugin.writer(_write_args=_full_url_args())

    call = mock_session.post.call_args
    # URL is the resolved full_url — no /topics/{topic} suffix.
    assert call.args[0] == "https://http-ingress.example.com/rest"
    # Body envelope stays the Confluent v2 shape so on-topic bytes match native.
    body = _decode_post_body(call.kwargs["data"])
    _assert_record(body, expected_value={"product_id": "p1", "count": 1})
    # Headers stay v2 JSON.
    assert call.kwargs["headers"]["Content-Type"] == "application/vnd.kafka.json.v2+json"
    assert call.kwargs["headers"]["Accept"] == "application/vnd.kafka.v2+json"


def test_writer_full_url_strips_trailing_slash():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _full_url_args()
    args["rest_write_options"]["full_url"] = (
        "https://http-ingress.example.com/rest/"
    )
    mock_session = _mock_session(
        post_return=_response(200, {"offsets": [{"partition": 0, "offset": 1, "error_code": None}]}),
    )
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        plugin.writer(_write_args=args)
    assert mock_session.post.call_args.args[0] == (
        "https://http-ingress.example.com/rest"
    )


def test_writer_full_url_wins_over_base_url_and_topic_when_both_set():
    """When both shapes are present, ``full_url`` takes precedence — we do NOT
    silently compose ``{base_url}/topics/{topic}`` alongside a full URL.
    """
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _full_url_args()
    args["rest_write_options"]["base_url"] = "https://kafka-rest.example.com"
    args["rest_write_options"]["topic"] = "dq-stats"
    mock_session = _mock_session(
        post_return=_response(200, {"offsets": [{"partition": 0, "offset": 1, "error_code": None}]}),
    )
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        plugin.writer(_write_args=args)
    # No /topics/ segment — full_url won.
    assert mock_session.post.call_args.args[0] == (
        "https://http-ingress.example.com/rest"
    )


def test_writer_full_url_treats_topic_as_optional_logical_label_in_logs():
    """In ``full_url`` mode ``topic`` is optional. When provided, it is used
    ONLY for log messages — never for URL composition."""
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _full_url_args()
    args["rest_write_options"]["topic"] = "dq-sparkexpectations-stats"
    mock_session = _mock_session(
        post_return=_response(200, {"offsets": [{"partition": 0, "offset": 1, "error_code": None}]}),
    )
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ), patch("spark_expectations.sinks.plugins.kafka_rest_writer._log") as mock_log:
        plugin.writer(_write_args=args)

    # URL is unchanged (no /topics/ suffix).
    assert mock_session.post.call_args.args[0] == (
        "https://http-ingress.example.com/rest"
    )
    # Log lines mention the logical topic AND url_mode=full_url so operators
    # can distinguish the two produce shapes at a glance.
    info_calls = [c.args[0] for c in mock_log.info.call_args_list]
    assert any(
        "topic: dq-sparkexpectations-stats" in msg and "url_mode=full_url" in msg
        for msg in info_calls
    )


def test_writer_full_url_error_message_includes_url_not_missing_topic():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _full_url_args()
    mock_session = _mock_session(post_return=_response(404, text="not found"))
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        with pytest.raises(
            SparkExpectationsMiscException,
            match=r"REST proxy HTTP 404 for topic '.*' at 'https://http-ingress\.example\.com/rest'",
        ):
            plugin.writer(_write_args=args)


def test_writer_raises_when_neither_full_url_nor_base_url_topic_set():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _write_args()
    args["rest_write_options"] = {"embedded_format": "json", "api_version": "v2"}
    with pytest.raises(
        SparkExpectationsMiscException,
        match=r"either 'full_url' OR both 'base_url' and 'topic' are required",
    ):
        plugin.writer(_write_args=args)


def test_writer_full_url_empty_string_falls_back_to_base_url_topic():
    """An explicitly empty ``full_url`` must not short-circuit; SE should fall
    back to the Confluent-style ``base_url`` + ``topic`` composition."""
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _write_args()
    args["rest_write_options"]["full_url"] = ""  # explicitly empty
    mock_session = _mock_session(
        post_return=_response(200, {"offsets": [{"partition": 0, "offset": 1, "error_code": None}]}),
    )
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        plugin.writer(_write_args=args)
    # Composed URL — same as legacy behavior.
    assert mock_session.post.call_args.args[0] == "https://kafka-rest.example.com/topics/dq-stats"


# ---------------------------------------------------------------------------
# Message key — always populated, includes product_id + run timestamp + row
# index. Compacted topics (``cleanup.policy=compact``) reject value-only
# records; SE must always emit a keyed record regardless of transport shape.
# ---------------------------------------------------------------------------


def test_writer_always_emits_message_key_with_product_id_prefix():
    """Every record in the produce envelope must carry a ``key`` field, and
    the key MUST start with ``{product_id}:``."""
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _write_args(product_id="orders_gold")
    mock_session = _mock_session(
        post_return=_response(200, {"offsets": [{"partition": 0, "offset": 1, "error_code": None}]}),
    )
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        plugin.writer(_write_args=args)

    body = _decode_post_body(mock_session.post.call_args.kwargs["data"])
    record = body["records"][0]
    assert "key" in record
    assert record["key"].startswith("orders_gold:"), record["key"]
    # Key ends with row index 0 for a single-row batch.
    assert record["key"].endswith(":0"), record["key"]


def test_writer_key_falls_back_to_unknown_when_product_id_absent():
    """``product_id`` is populated by SE core, but if it is ever missing the
    plugin must still emit a valid non-empty key rather than crash."""
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _write_args()
    args["product_id"] = None
    mock_session = _mock_session(
        post_return=_response(200, {"offsets": [{"partition": 0, "offset": 1, "error_code": None}]}),
    )
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        plugin.writer(_write_args=args)

    body = _decode_post_body(mock_session.post.call_args.kwargs["data"])
    assert body["records"][0]["key"].startswith("unknown:")


def test_writer_key_run_timestamp_shared_across_rows_in_single_publish():
    """One writer invocation captures ``run_timestamp`` once; every record in
    the batch must share it. Only the trailing row-index differs."""
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _write_args()
    args["stats_df"] = _fake_stats_df([json.dumps({"i": i}) for i in range(3)])
    mock_session = _mock_session(
        post_return=_response(200, {"offsets": [{"partition": 0, "offset": 1, "error_code": None}]}),
    )
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        plugin.writer(_write_args=args)

    assert mock_session.post.call_count == 3
    keys = [
        _decode_post_body(c.kwargs["data"])["records"][0]["key"]
        for c in mock_session.post.call_args_list
    ]
    # Row indices are dense 0..N-1 and unique.
    assert [k.rsplit(":", 1)[1] for k in keys] == ["0", "1", "2"]
    # The product_id + run_timestamp prefix (everything before the trailing
    # ":{index}") is identical across all three records.
    prefixes = {k.rsplit(":", 1)[0] for k in keys}
    assert len(prefixes) == 1, f"run timestamp must be shared per publish; got prefixes={prefixes}"
    # Prefix carries product_id.
    assert next(iter(prefixes)).startswith("p1:")


def test_writer_key_present_in_full_url_mode_as_well():
    """``full_url`` transport (Nike NSP3 HTTP-ingress) uses the same envelope
    shape, so the key MUST also be present when full_url is in play."""
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    mock_session = _mock_session(
        post_return=_response(200, {"offsets": [{"partition": 0, "offset": 1, "error_code": None}]}),
    )
    with patch(
        "spark_expectations.sinks.plugins.kafka_rest_writer._build_session",
        return_value=mock_session,
    ):
        plugin.writer(_write_args=_full_url_args())

    body = _decode_post_body(mock_session.post.call_args.kwargs["data"])
    _assert_record(body, expected_value={"product_id": "p1", "count": 1})
