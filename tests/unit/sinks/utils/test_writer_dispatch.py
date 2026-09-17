"""Unit tests for Phase 2 dispatch + option-building in ``writer.py``.

Covers three closely-related behaviours:

1. ``SparkExpectationsWriter.get_kafka_rest_write_options`` — verifies both
   the URL / topic secret-indirection and the opt-in auth matrix, with
   explicit read-avoidance assertions against a spy secrets backend so we
   guarantee the lazy-read invariant in the rollout plan is honoured
   (no Cerberus / Databricks / auth secret lookups when auth_type is ``none``).
2. ``SparkExpectationsWriter.write_error_stats`` streaming dispatch —
   branches on ``get_streaming_transport`` and passes the correct
   ``transport`` + ``kafka_write_options`` / ``rest_write_options`` shape
   to ``_sink_hook.writer``.
3. ``se.streaming.enable=False`` — no dispatch call, ``Disabled`` status.

The dispatch tests intentionally construct a real ``SparkExpectationsContext``
+ real writer (mirroring the pattern used by the existing integration
tests) because ``write_error_stats`` reads a large number of context
properties. ``save_df_as_table`` / ``write_detailed_stats`` are patched
so the tests do not require a live Delta target.
"""

from __future__ import annotations

from typing import Dict, List, Optional
from unittest.mock import Mock, patch

import pytest

from spark_expectations.config.user_config import Constants as user_config
from spark_expectations.core import get_spark_session
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.core.expectations import WrappedDataFrameWriter
from spark_expectations.sinks.utils.writer import SparkExpectationsWriter


spark = get_spark_session()


# ---------------------------------------------------------------------------
# Spy secrets backend — records every ``get_secret(key)`` call so tests can
# assert precisely which keys were probed (or that NONE were probed).
# ---------------------------------------------------------------------------


class _SecretSpy:
    """Stand-in for :class:`SparkExpectationsSecretsBackend`.

    Instances are constructed by ``get_kafka_rest_write_options`` via the
    patched class name; the constructor records the dict it was handed and
    every subsequent ``get_secret`` call is appended to a shared log so the
    tests can inspect exact call ordering.
    """

    calls: List[Optional[str]] = []
    resolved: Dict[str, str] = {}

    def __init__(self, secret_dict: Dict[str, str]) -> None:  # noqa: D401 - keep signature parity
        self.secret_dict = secret_dict

    def get_secret(self, secret_key: Optional[str]) -> Optional[str]:
        _SecretSpy.calls.append(secret_key)
        if secret_key is None:
            return None
        return _SecretSpy.resolved.get(secret_key, f"resolved({secret_key})")


@pytest.fixture(autouse=True)
def _reset_secret_spy() -> None:
    _SecretSpy.calls = []
    _SecretSpy.resolved = {}


def _writer_with_stats(stats_dict: Dict[str, object]) -> SparkExpectationsWriter:
    ctx = SparkExpectationsContext(product_id="p1", spark=spark)
    ctx.set_se_streaming_stats_dict(stats_dict)
    return SparkExpectationsWriter(ctx)


# ---------------------------------------------------------------------------
# get_kafka_rest_write_options — URL / topic lazy resolution
# ---------------------------------------------------------------------------


def test_options_direct_base_url_and_topic_never_probe_secrets():
    stats = {
        user_config.se_streaming_transport: "kafka_rest",
        user_config.se_streaming_rest_base_url: "https://kafka-rest.example.com",
        user_config.se_streaming_rest_topic_name: "dq-stats",
    }
    writer = _writer_with_stats(stats)

    with patch(
        "spark_expectations.sinks.utils.writer.SparkExpectationsSecretsBackend",
        _SecretSpy,
    ):
        options = writer.get_kafka_rest_write_options(stats)

    assert options["base_url"] == "https://kafka-rest.example.com"
    assert options["topic"] == "dq-stats"
    # No secret_key was configured for URL/topic → backend must never be
    # called for URL/topic, and NEVER for any auth key (auth defaults off).
    assert _SecretSpy.calls == []
    # Auth fields absent → plugin will publish unauthenticated.
    assert "auth" not in options
    assert "auth_headers" not in options


def test_options_dbx_secret_indirection_resolves_url_and_topic():
    stats = {
        user_config.se_streaming_transport: "kafka_rest",
        user_config.secret_type: "databricks",
        user_config.dbx_rest_base_url: "dbx_url_key",
        user_config.dbx_rest_topic_name: "dbx_topic_key",
    }
    _SecretSpy.resolved = {
        "dbx_url_key": "https://from-dbx.example.com",
        "dbx_topic_key": "dq-from-dbx",
    }
    writer = _writer_with_stats(stats)

    with patch(
        "spark_expectations.sinks.utils.writer.SparkExpectationsSecretsBackend",
        _SecretSpy,
    ):
        options = writer.get_kafka_rest_write_options(stats)

    assert options["base_url"] == "https://from-dbx.example.com"
    assert options["topic"] == "dq-from-dbx"
    # Exactly the two URL/topic reads, in that order. NO auth key reads.
    assert _SecretSpy.calls == ["dbx_url_key", "dbx_topic_key"]


# ---------------------------------------------------------------------------
# get_kafka_rest_write_options — auth matrix (opt-in) + read avoidance
# ---------------------------------------------------------------------------


def test_options_auth_type_unset_resolves_to_default_none():
    """Unset auth_type resolves to DEFAULT_REST_AUTH_TYPE ('none') — same
    behaviour as an explicit auth_type='none'. Credential fields must not be read.
    """
    stats = {
        user_config.se_streaming_transport: "kafka_rest",
        user_config.se_streaming_rest_base_url: "https://kafka-rest.example.com",
        user_config.se_streaming_rest_topic_name: "dq-stats",
        # These MUST be ignored — auth_type is unset.
        user_config.se_streaming_rest_username: "should-not-be-read",
        user_config.secret_type: "databricks",
        user_config.dbx_rest_auth_secret: "dbx_auth_secret_key",
    }
    _SecretSpy.resolved = {"dbx_auth_secret_key": "MUST-NOT-APPEAR"}
    writer = _writer_with_stats(stats)

    with patch(
        "spark_expectations.sinks.utils.writer.SparkExpectationsSecretsBackend",
        _SecretSpy,
    ):
        options = writer.get_kafka_rest_write_options(stats)

    # URL / topic used direct values → zero secret backend calls in total.
    assert _SecretSpy.calls == []
    assert "auth" not in options
    assert "auth_headers" not in options


def test_options_auth_type_none_short_circuits_before_touching_credentials():
    stats = {
        user_config.se_streaming_transport: "kafka_rest",
        user_config.se_streaming_rest_base_url: "https://kafka-rest.example.com",
        user_config.se_streaming_rest_topic_name: "dq-stats",
        user_config.se_streaming_rest_auth_type: "none",
        # These MUST be ignored.
        user_config.se_streaming_rest_username: "should-not-be-read",
        user_config.secret_type: "databricks",
        user_config.dbx_rest_auth_secret: "dbx_auth_secret_key",
    }
    writer = _writer_with_stats(stats)

    with patch(
        "spark_expectations.sinks.utils.writer.SparkExpectationsSecretsBackend",
        _SecretSpy,
    ):
        options = writer.get_kafka_rest_write_options(stats)

    assert _SecretSpy.calls == []
    assert "auth" not in options
    assert "auth_headers" not in options


def test_options_unknown_auth_type_warns_and_resolves_to_none(caplog):
    stats = {
        user_config.se_streaming_transport: "kafka_rest",
        user_config.se_streaming_rest_base_url: "https://kafka-rest.example.com",
        user_config.se_streaming_rest_topic_name: "dq-stats",
        user_config.se_streaming_rest_auth_type: "mtls",  # not in REST_AUTH_TYPES
        user_config.se_streaming_rest_username: "should-not-be-read",
        user_config.secret_type: "databricks",
        user_config.dbx_rest_auth_secret: "dbx_auth_secret_key",
    }
    writer = _writer_with_stats(stats)

    with patch(
        "spark_expectations.sinks.utils.writer.SparkExpectationsSecretsBackend",
        _SecretSpy,
    ), caplog.at_level("WARNING"):
        options = writer.get_kafka_rest_write_options(stats)

    assert any("mtls" in rec.message for rec in caplog.records if rec.levelname == "WARNING")
    assert _SecretSpy.calls == []
    assert "auth" not in options
    assert "auth_headers" not in options


def test_options_basic_auth_populates_tuple_and_reads_only_the_auth_secret():
    stats = {
        user_config.se_streaming_transport: "kafka_rest",
        user_config.se_streaming_rest_base_url: "https://kafka-rest.example.com",
        user_config.se_streaming_rest_topic_name: "dq-stats",
        user_config.se_streaming_rest_auth_type: "basic",
        user_config.se_streaming_rest_username: "svc_dq",
        user_config.secret_type: "databricks",
        user_config.dbx_rest_auth_secret: "dbx_auth_secret_key",
    }
    _SecretSpy.resolved = {"dbx_auth_secret_key": "s3cret"}
    writer = _writer_with_stats(stats)

    with patch(
        "spark_expectations.sinks.utils.writer.SparkExpectationsSecretsBackend",
        _SecretSpy,
    ):
        options = writer.get_kafka_rest_write_options(stats)

    assert options["auth"] == ("svc_dq", "s3cret")
    assert "auth_headers" not in options
    # Only the auth-secret key was read (URL/topic were direct values).
    assert _SecretSpy.calls == ["dbx_auth_secret_key"]


def test_options_basic_auth_missing_username_raises_before_calling_post():
    stats = {
        user_config.se_streaming_transport: "kafka_rest",
        user_config.se_streaming_rest_base_url: "https://kafka-rest.example.com",
        user_config.se_streaming_rest_topic_name: "dq-stats",
        user_config.se_streaming_rest_auth_type: "basic",
        # username missing
        user_config.secret_type: "databricks",
        user_config.dbx_rest_auth_secret: "dbx_auth_secret_key",
    }
    _SecretSpy.resolved = {"dbx_auth_secret_key": "s3cret"}
    writer = _writer_with_stats(stats)

    with patch(
        "spark_expectations.sinks.utils.writer.SparkExpectationsSecretsBackend",
        _SecretSpy,
    ), pytest.raises(SparkExpectationsMiscException, match="auth_type=basic"):
        writer.get_kafka_rest_write_options(stats)


def test_options_basic_auth_missing_secret_key_raises():
    stats = {
        user_config.se_streaming_transport: "kafka_rest",
        user_config.se_streaming_rest_base_url: "https://kafka-rest.example.com",
        user_config.se_streaming_rest_topic_name: "dq-stats",
        user_config.se_streaming_rest_auth_type: "basic",
        user_config.se_streaming_rest_username: "svc_dq",
        # No secret_type / no auth secret key → get_rest_auth_secret_key → None.
    }
    writer = _writer_with_stats(stats)

    with patch(
        "spark_expectations.sinks.utils.writer.SparkExpectationsSecretsBackend",
        _SecretSpy,
    ), pytest.raises(SparkExpectationsMiscException, match="auth_type=basic"):
        writer.get_kafka_rest_write_options(stats)
    # secret_key was None → backend must NOT be called.
    assert _SecretSpy.calls == []


def test_options_bearer_auth_populates_authorization_header():
    stats = {
        user_config.se_streaming_transport: "kafka_rest",
        user_config.se_streaming_rest_base_url: "https://kafka-rest.example.com",
        user_config.se_streaming_rest_topic_name: "dq-stats",
        user_config.se_streaming_rest_auth_type: "bearer",
        user_config.secret_type: "cerberus",
        user_config.cbs_rest_auth_secret: "cbs_auth_secret_key",
    }
    _SecretSpy.resolved = {"cbs_auth_secret_key": "tok-abc"}
    writer = _writer_with_stats(stats)

    with patch(
        "spark_expectations.sinks.utils.writer.SparkExpectationsSecretsBackend",
        _SecretSpy,
    ):
        options = writer.get_kafka_rest_write_options(stats)

    assert options["auth_headers"] == {"Authorization": "Bearer tok-abc"}
    assert "auth" not in options
    assert _SecretSpy.calls == ["cbs_auth_secret_key"]


def test_options_bearer_missing_secret_key_raises_and_avoids_backend():
    stats = {
        user_config.se_streaming_transport: "kafka_rest",
        user_config.se_streaming_rest_base_url: "https://kafka-rest.example.com",
        user_config.se_streaming_rest_topic_name: "dq-stats",
        user_config.se_streaming_rest_auth_type: "bearer",
        # No secret_type → get_rest_auth_secret_key returns None.
    }
    writer = _writer_with_stats(stats)

    with patch(
        "spark_expectations.sinks.utils.writer.SparkExpectationsSecretsBackend",
        _SecretSpy,
    ), pytest.raises(SparkExpectationsMiscException, match="auth_type=bearer"):
        writer.get_kafka_rest_write_options(stats)
    assert _SecretSpy.calls == []


# ---------------------------------------------------------------------------
# write_error_stats — streaming dispatch. These tests wire up the minimum
# real context state required to reach the dispatch section, mock the
# heavy Delta write path, and assert what _sink_hook.writer receives.
# ---------------------------------------------------------------------------


def _seed_context_for_write_error_stats(stats_dict: Dict[str, object]) -> SparkExpectationsContext:
    """Populate the mandatory context state ``write_error_stats`` reads.

    Mirrors the pattern used by the integration tests in
    ``tests/integration/sinks/utils/test_writer.py::test_write_error_stats_*``.
    Only the fields whose absence raises are set — everything else is left
    at its default so the test surface stays tight.
    """
    ctx = SparkExpectationsContext(product_id="test_product", spark=spark)
    ctx.set_se_streaming_stats_dict(stats_dict)
    ctx.set_table_name("test_table")
    ctx.set_input_count(100)
    ctx.set_error_count(5)
    ctx.set_output_count(95)
    ctx.set_dq_stats_table_name("test_dq_stats_table")
    ctx._stats_table_writer_config = (
        WrappedDataFrameWriter().mode("overwrite").format("delta").build()
    )
    ctx.set_dq_rules_params({"env": "test"})
    ctx._run_id = "test_run_id"
    ctx._run_date = "2023-01-01 10:00:00"
    ctx._source_agg_dq_result = []
    ctx._final_agg_dq_result = []
    ctx._source_query_dq_result = []
    ctx._final_query_dq_result = []
    ctx._summarized_row_dq_res = []
    ctx._rules_exceeds_threshold = []
    ctx._dq_run_status = "Passed"
    ctx._source_agg_dq_status = "Passed"
    ctx._source_query_dq_status = "Passed"
    ctx._row_dq_status = "Passed"
    ctx._final_agg_dq_status = "Passed"
    ctx._final_query_dq_status = "Passed"
    ctx._dq_run_time = 10.0
    ctx._source_agg_dq_run_time = 2.0
    ctx._source_query_dq_run_time = 1.0
    ctx._row_dq_run_time = 3.0
    ctx._final_agg_dq_run_time = 2.0
    ctx._final_query_dq_run_time = 2.0
    ctx._num_row_dq_rules = 5
    ctx._num_dq_rules = 10
    ctx._num_agg_dq_rules = {
        "num_source_agg_dq_rules": 2,
        "num_agg_dq_rules": 3,
        "num_final_agg_dq_rules": 1,
    }
    ctx._num_query_dq_rules = {
        "num_source_query_dq_rules": 1,
        "num_query_dq_rules": 2,
        "num_final_query_dq_rules": 1,
    }
    return ctx


def test_dispatch_native_transport_passes_kafka_write_options():
    ctx = _seed_context_for_write_error_stats(
        {user_config.se_enable_streaming: True}
        # No se.streaming.transport → defaults to kafka_native.
    )
    writer = SparkExpectationsWriter(ctx)
    writer.save_df_as_table = Mock()  # type: ignore[assignment]
    writer.get_kafka_write_options = Mock(  # type: ignore[assignment]
        return_value={"kafka.bootstrap.servers": "localhost:9092", "topic": "t"}
    )
    writer.get_kafka_rest_write_options = Mock()  # type: ignore[assignment]

    with patch("spark_expectations.sinks._sink_hook.writer") as mock_hook:
        writer.write_error_stats()

    writer.get_kafka_rest_write_options.assert_not_called()
    writer.get_kafka_write_options.assert_called_once()
    mock_hook.assert_called_once()
    args = mock_hook.call_args.kwargs["_write_args"]
    assert args["transport"] == "kafka_native"
    assert args["kafka_write_options"] == {"kafka.bootstrap.servers": "localhost:9092", "topic": "t"}
    assert "rest_write_options" not in args
    assert ctx.get_kafka_write_status == "Success"


def test_dispatch_rest_transport_passes_rest_write_options():
    ctx = _seed_context_for_write_error_stats(
        {
            user_config.se_enable_streaming: True,
            user_config.se_streaming_transport: "kafka_rest",
            user_config.se_streaming_rest_base_url: "https://kafka-rest.example.com",
            user_config.se_streaming_rest_topic_name: "dq-stats",
        }
    )
    writer = SparkExpectationsWriter(ctx)
    writer.save_df_as_table = Mock()  # type: ignore[assignment]
    writer.get_kafka_write_options = Mock()  # type: ignore[assignment]
    fake_rest_opts = {
        "base_url": "https://kafka-rest.example.com",
        "topic": "dq-stats",
        "api_version": "v2",
        "embedded_format": "json",
    }
    writer.get_kafka_rest_write_options = Mock(return_value=fake_rest_opts)  # type: ignore[assignment]

    with patch("spark_expectations.sinks._sink_hook.writer") as mock_hook:
        writer.write_error_stats()

    writer.get_kafka_write_options.assert_not_called()
    writer.get_kafka_rest_write_options.assert_called_once()
    mock_hook.assert_called_once()
    args = mock_hook.call_args.kwargs["_write_args"]
    assert args["transport"] == "kafka_rest"
    assert args["rest_write_options"] is fake_rest_opts
    assert "kafka_write_options" not in args
    assert ctx.get_kafka_write_status == "Success"


def test_dispatch_streaming_disabled_never_calls_hook_and_marks_disabled():
    ctx = _seed_context_for_write_error_stats(
        {user_config.se_enable_streaming: False}
    )
    writer = SparkExpectationsWriter(ctx)
    writer.save_df_as_table = Mock()  # type: ignore[assignment]
    writer.get_kafka_write_options = Mock()  # type: ignore[assignment]
    writer.get_kafka_rest_write_options = Mock()  # type: ignore[assignment]

    with patch("spark_expectations.sinks._sink_hook.writer") as mock_hook:
        writer.write_error_stats()

    mock_hook.assert_not_called()
    writer.get_kafka_write_options.assert_not_called()
    writer.get_kafka_rest_write_options.assert_not_called()
    assert ctx.get_kafka_write_status == "Disabled"
