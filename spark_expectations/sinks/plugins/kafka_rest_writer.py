import json
from typing import Any, Dict, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from spark_expectations import _log
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.sinks.plugins.base_writer import (
    SparkExpectationsSinkWriter,
    spark_expectations_writer_impl,
)


_ACCEPT_HEADER = "application/vnd.kafka.v2+json"
_CONTENT_TYPE_JSON_V2 = "application/vnd.kafka.json.v2+json"

_RETRYABLE_STATUS = (408, 429, 500, 502, 503, 504)


def _build_session(rest_options: Dict[str, Any]) -> requests.Session:
    """Build a ``requests.Session`` with retry + connection pooling for the Kafka REST proxy.

    Retries cover network errors AND retryable HTTP statuses (408/429/5xx). ``POST`` is included
    in ``allowed_methods`` explicitly because urllib3 does not retry non-idempotent verbs by default.
    """
    retry = Retry(
        total=int(rest_options.get("max_retries", 3)),
        connect=int(rest_options.get("max_retries", 3)),
        read=int(rest_options.get("max_retries", 3)),
        status=int(rest_options.get("max_retries", 3)),
        backoff_factor=float(rest_options.get("backoff_factor", 0.5)),
        status_forcelist=_RETRYABLE_STATUS,
        allowed_methods=frozenset(["POST"]),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    # Use the default values for pool_connections and pool_maxsize if not provided
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=int(rest_options.get("pool_connections", 4)),
        pool_maxsize=int(rest_options.get("pool_maxsize", 10)),
    )
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _resolve_timeout(rest_options: Dict[str, Any]) -> Tuple[float, float]:
    """Resolve request timeout as a ``(connect, read)`` tuple.

    Accepts either the legacy scalar ``timeout_sec`` (applied to both) or explicit
    ``connect_timeout_sec`` / ``read_timeout_sec`` overrides.
    """
    scalar = float(rest_options.get("timeout_sec", 30))
    connect_timeout = float(rest_options.get("connect_timeout_sec", scalar))
    read_timeout = float(rest_options.get("read_timeout_sec", scalar))
    return (connect_timeout, read_timeout)


def _retry_count_from_response(response: requests.Response) -> int:
    """Best-effort extraction of the number of retries urllib3 performed for this request."""
    raw = getattr(response, "raw", None)
    retries = getattr(raw, "retries", None)
    if retries is None:
        return 0
    history = getattr(retries, "history", ()) or ()
    return len(history)


class SparkExpectationsKafkaRestWritePluginImpl(SparkExpectationsSinkWriter):
    @spark_expectations_writer_impl
    def writer(self, _write_args: Dict[str, Any]) -> None:
        transport = _write_args.get("transport")
        if transport not in (None, "kafka_rest"):
            return

        if not _write_args.get("enable_se_streaming"):
            return

        rest_options: Dict[str, Any] = _write_args.get("rest_write_options") or {}
        base_url = rest_options.get("base_url")
        topic = rest_options.get("topic")
        if not base_url or not topic:
            raise SparkExpectationsMiscException(
                "error occurred while saving data into kafka (REST): "
                "'base_url' and 'topic' are required in rest_write_options"
            )

        timeout = _resolve_timeout(rest_options)
        verify = bool(rest_options.get("verify_ssl", True))
        headers = {"Content-Type": _CONTENT_TYPE_JSON_V2, "Accept": _ACCEPT_HEADER}
        url = f"{str(base_url).rstrip('/')}/topics/{topic}"

        stats_df = _write_args.get("stats_df")
        rows = stats_df.selectExpr("to_json(struct(*)) AS value").collect()

        session = _build_session(rest_options)

        total_rows = len(rows)
        total_bytes_sent = 0
        total_retries = 0
        _log.info(
            f"started write stats data into kafka REST proxy topic: {topic} "
            f"(rows={total_rows}, connect_timeout={timeout[0]}s, read_timeout={timeout[1]}s)"
        )
        try:
            for row in rows:
                raw_json = row["value"]
                body = {"records": [{"value": json.loads(raw_json)}]}
                payload_bytes = json.dumps(body).encode("utf-8")
                try:
                    response = session.post(
                        url,
                        data=payload_bytes,
                        headers=headers,
                        timeout=timeout,
                        verify=verify,
                    )
                except Exception as exc:  # pylint: disable=broad-except
                    raise SparkExpectationsMiscException(
                        f"error occurred while saving data into kafka (REST) topic "
                        f"'{topic}' at '{base_url}': {exc}"
                    ) from exc

                row_retries = _retry_count_from_response(response)
                total_retries += row_retries
                total_bytes_sent += len(payload_bytes)
                if row_retries:
                    _log.warning(
                        f"kafka REST proxy request to topic '{topic}' retried {row_retries} "
                        f"time(s) before status {response.status_code}"
                    )

                self._raise_for_record_errors(response, topic, base_url)
        finally:
            session.close()

        _log.info(
            f"ended writing stats data into kafka REST proxy topic: {topic} "
            f"(rows={total_rows}, bytes_sent={total_bytes_sent}, total_retries={total_retries})"
        )

    @staticmethod
    def _raise_for_record_errors(response: requests.Response, topic: str, base_url: str) -> None:
        if response.status_code >= 400:
            raise SparkExpectationsMiscException(
                f"REST proxy HTTP {response.status_code} for topic '{topic}' at "
                f"'{base_url}': {response.text}"
            )
        payload: Dict[str, Any] = {}
        try:
            if response.content:
                payload = response.json()
        except ValueError:
            payload = {}

        for offset in payload.get("offsets", []) or []:
            if offset.get("error_code") is not None:
                raise SparkExpectationsMiscException(
                    f"REST proxy record error for topic '{topic}' at '{base_url}': {offset}"
                )
        if payload.get("error_code") is not None:
            raise SparkExpectationsMiscException(
                f"REST proxy record error for topic '{topic}' at '{base_url}': {payload}"
            )
