import json
from unittest.mock import MagicMock, patch

import pytest

from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.sinks.plugins.kafka_rest_writer import (
    SparkExpectationsKafkaRestWritePluginImpl,
    _build_rest_headers,
    _normalize_api_version,
    _normalize_embedded_format,
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
    with pytest.raises(SparkExpectationsMiscException, match="expected 'v2'"):
        _normalize_api_version("v1")


def test_normalize_embedded_format_rejects_binary():
    with pytest.raises(SparkExpectationsMiscException, match="expected 'json'"):
        _normalize_embedded_format("binary")


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
    assert body == {"records": [{"value": {"product_id": "p1", "count": 1}}]}


def test_writer_rejects_binary_embedded_format():
    plugin = SparkExpectationsKafkaRestWritePluginImpl()
    args = _write_args()
    args["rest_write_options"]["embedded_format"] = "binary"
    with patch("spark_expectations.sinks.plugins.kafka_rest_writer._build_session") as mock_build_session:
        with pytest.raises(SparkExpectationsMiscException, match="expected 'json'"):
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
        assert _decode_post_body(call.kwargs["data"]) == {"records": [{"value": {"i": idx}}]}
