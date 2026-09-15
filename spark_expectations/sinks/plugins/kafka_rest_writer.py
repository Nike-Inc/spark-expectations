import json
from typing import Any, Callable, Dict, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from spark_expectations import _log
from spark_expectations.config.rest_streaming_defaults import (
    DEFAULT_REST_API_VERSION,
    DEFAULT_REST_BACKOFF_FACTOR,
    DEFAULT_REST_EMBEDDED_FORMAT,
    DEFAULT_REST_MAX_RETRIES,
    DEFAULT_REST_POOL_CONNECTIONS,
    DEFAULT_REST_POOL_MAXSIZE,
    DEFAULT_REST_TIMEOUT_SEC,
)
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.sinks.plugins.base_writer import (
    SparkExpectationsSinkWriter,
    spark_expectations_writer_impl,
)

_RETRYABLE_STATUS = (408, 429, 500, 502, 503, 504)

# Supported values today; extend these sets when enabling additional REST flows.
_SUPPORTED_API_VERSIONS = frozenset({"2"})
_SUPPORTED_EMBEDDED_FORMATS = frozenset({"json"})


def _normalize_api_version(api_version: Any) -> str:
    version = str(api_version).strip().lower()
    if version.startswith("v"):
        version = version[1:]
    if version not in _SUPPORTED_API_VERSIONS:
        if version == "3":
            raise SparkExpectationsMiscException(
                f"unsupported kafka REST api_version '{api_version}'; only 'v2' is supported"
            )
        raise SparkExpectationsMiscException(
            f"unsupported kafka REST api_version '{api_version}'; expected 'v2'"
        )
    return f"v{version}"


def _normalize_embedded_format(embedded_format: Any) -> str:
    fmt = str(embedded_format).strip().lower()
    if fmt not in _SUPPORTED_EMBEDDED_FORMATS:
        raise SparkExpectationsMiscException(
            f"unsupported kafka REST embedded_format '{embedded_format}'; expected 'json'"
        )
    return fmt


def _build_rest_headers(api_version: str, embedded_format: str) -> Dict[str, str]:
    """Build Confluent Kafka REST Accept / Content-Type headers."""
    version = _normalize_api_version(api_version)
    fmt = _normalize_embedded_format(embedded_format)

    if version == "v3":
        return {"Content-Type": "application/json", "Accept": "application/json"}

    accept = f"application/vnd.kafka.{version}+json"
    if fmt == "json":
        content_type = f"application/vnd.kafka.json.{version}+json"
    else:
        raise SparkExpectationsMiscException(
            f"unsupported kafka REST embedded_format '{embedded_format}'; expected 'json'"
        )
    return {"Content-Type": content_type, "Accept": accept}


def _build_topic_url(base_url: str, topic: str, api_version: str, cluster_id: Optional[str]) -> str:
    version = _normalize_api_version(api_version)
    base = str(base_url).rstrip("/")
    if version == "v3":
        if not cluster_id:
            raise SparkExpectationsMiscException(
                "cluster_id is required in rest_write_options when api_version is v3"
            )
        return f"{base}/kafka/v3/clusters/{cluster_id}/topics/{topic}/records"
    return f"{base}/topics/{topic}"


def _build_record_payload(raw_json: str, api_version: str, embedded_format: str) -> bytes:
    version = _normalize_api_version(api_version)
    if version == "v3":
        v3_body: Dict[str, Any] = {"value": {"type": "JSON", "data": json.loads(raw_json)}}
        return json.dumps(v3_body).encode("utf-8")

    fmt = _normalize_embedded_format(embedded_format)
    if fmt == "json":
        body: Dict[str, Any] = {"records": [{"value": json.loads(raw_json)}]}
    else:
        raise SparkExpectationsMiscException(
            f"unsupported kafka REST embedded_format '{embedded_format}'; expected 'json'"
        )
    return json.dumps(body).encode("utf-8")


def _resolve_publish_context(rest_options: Dict[str, Any]) -> Tuple[str, Dict[str, str], Callable[[str], bytes]]:
    api_version = rest_options.get("api_version", DEFAULT_REST_API_VERSION)
    embedded_format = rest_options.get("embedded_format", DEFAULT_REST_EMBEDDED_FORMAT)
    base_url = rest_options["base_url"]
    topic = rest_options["topic"]
    cluster_id = rest_options.get("cluster_id")

    url = _build_topic_url(base_url, topic, api_version, cluster_id)
    if _normalize_api_version(api_version) == "v3":
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
    else:
        headers = _build_rest_headers(api_version, embedded_format)

    def build_payload(raw_json: str) -> bytes:
        return _build_record_payload(raw_json, api_version, embedded_format)

    return url, headers, build_payload


def _build_session(rest_options: Dict[str, Any]) -> requests.Session:
    """Build a ``requests.Session`` with retry + connection pooling for the Kafka REST proxy.

    Retries cover network errors AND retryable HTTP statuses (408/429/5xx). ``POST`` is included
    in ``allowed_methods`` explicitly because urllib3 does not retry non-idempotent verbs by default.
    """
    retry = Retry(
        total=int(rest_options.get("max_retries", DEFAULT_REST_MAX_RETRIES)),
        connect=int(rest_options.get("max_retries", DEFAULT_REST_MAX_RETRIES)),
        read=int(rest_options.get("max_retries", DEFAULT_REST_MAX_RETRIES)),
        status=int(rest_options.get("max_retries", DEFAULT_REST_MAX_RETRIES)),
        backoff_factor=float(rest_options.get("backoff_factor", DEFAULT_REST_BACKOFF_FACTOR)),
        status_forcelist=_RETRYABLE_STATUS,
        allowed_methods=frozenset(["POST"]),
        respect_retry_after_header=True,
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=int(rest_options.get("pool_connections", DEFAULT_REST_POOL_CONNECTIONS)),
        pool_maxsize=int(rest_options.get("pool_maxsize", DEFAULT_REST_POOL_MAXSIZE)),
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
    scalar = float(rest_options.get("timeout_sec", DEFAULT_REST_TIMEOUT_SEC))
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
        if transport != "kafka_rest":
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
        url, headers, build_payload = _resolve_publish_context(rest_options)

        stats_df = _write_args.get("stats_df")
        rows = stats_df.selectExpr("to_json(struct(*)) AS value").collect()
        total_rows = len(rows)
        _log.info(f"collected {total_rows} stats row(s) for kafka REST proxy publish to topic: {topic}")

        session = _build_session(rest_options)
        total_bytes_sent = 0
        total_retries = 0
        api_version = rest_options.get("api_version", DEFAULT_REST_API_VERSION)
        embedded_format = rest_options.get("embedded_format", DEFAULT_REST_EMBEDDED_FORMAT)
        _log.info(
            f"started write stats data into kafka REST proxy topic: {topic} "
            f"(rows={total_rows}, api_version={api_version}, embedded_format={embedded_format}, "
            f"connect_timeout={timeout[0]}s, read_timeout={timeout[1]}s)"
        )
        try:
            for row in rows:
                raw_json = row["value"]
                payload_bytes = build_payload(raw_json)
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
