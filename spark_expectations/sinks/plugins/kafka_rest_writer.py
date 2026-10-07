import json
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Tuple

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
from spark_expectations.sinks.utils.stats_metadata import apply_se_job_metadata_struct

_RETRYABLE_STATUS = (408, 429, 500, 502, 503, 504)

# Only v2 + json are supported today. When v3 (or another format) support is
# introduced, extend these sets AND add the corresponding branch in the helpers
# below along with the required configuration (e.g. cluster_id for v3).
_SUPPORTED_API_VERSIONS = frozenset({"2"})
_SUPPORTED_EMBEDDED_FORMATS = frozenset({"json"})


def _normalize_api_version(api_version: Any) -> str:
    """Validate + normalize the REST proxy API version.

    Only ``v2`` is accepted. Any other value (including ``v3``) raises
    :class:`SparkExpectationsMiscException`.
    """
    version = str(api_version).strip().lower()
    if version.startswith("v"):
        version = version[1:]
    if version not in _SUPPORTED_API_VERSIONS:
        raise SparkExpectationsMiscException(
            f"unsupported kafka REST api_version '{api_version}'; only 'v2' is supported"
        )
    return f"v{version}"


def _normalize_embedded_format(embedded_format: Any) -> str:
    """Validate + normalize the REST proxy embedded format.

    Only ``json`` is accepted today. Any other value raises
    :class:`SparkExpectationsMiscException`.
    """
    fmt = str(embedded_format).strip().lower()
    if fmt not in _SUPPORTED_EMBEDDED_FORMATS:
        raise SparkExpectationsMiscException(
            f"unsupported kafka REST embedded_format '{embedded_format}'; only 'json' is supported"
        )
    return fmt


def _build_rest_headers(api_version: str, embedded_format: str) -> Dict[str, str]:
    """Build Confluent Kafka REST v2 Accept / Content-Type headers for JSON payloads."""
    version = _normalize_api_version(api_version)
    _normalize_embedded_format(embedded_format)
    return {
        "Content-Type": f"application/vnd.kafka.json.{version}+json",
        "Accept": f"application/vnd.kafka.{version}+json",
    }


def _build_topic_url(base_url: str, topic: str, api_version: str) -> str:
    """Build the v2 topic-produce URL: ``{base}/topics/{topic}``."""
    _normalize_api_version(api_version)
    base = str(base_url).rstrip("/")
    return f"{base}/topics/{topic}"


def _resolve_produce_url(rest_options: Dict[str, Any]) -> Tuple[str, str]:
    """Resolve the produce URL and the URL-composition mode.

    Two modes are supported:

    * ``full_url`` — when ``rest_options["full_url"]`` is non-empty, that value
      is used verbatim (trailing slash trimmed) and topic is not appended.
      Enables HTTP-ingress endpoints where the stream URL is
      the produce endpoint.
    * ``base_url+topic`` — legacy Confluent REST Proxy v2 shape:
      ``{base_url}/topics/{topic}``.

    Returns a ``(url, mode)`` tuple where ``mode`` is one of
    ``"full_url"`` / ``"base_url+topic"``.

    Raises :class:`SparkExpectationsMiscException` when neither mode has the
    inputs it needs.
    """
    full_url = rest_options.get("full_url")
    if isinstance(full_url, str) and full_url.strip():
        return str(full_url).rstrip("/"), "full_url"

    base_url = rest_options.get("base_url")
    topic = rest_options.get("topic")
    if not base_url or not topic:
        raise SparkExpectationsMiscException(
            "error occurred while saving data into kafka (REST): "
            "either 'full_url' OR both 'base_url' and 'topic' are required "
            "in rest_write_options"
        )
    api_version = rest_options.get("api_version", DEFAULT_REST_API_VERSION)
    return _build_topic_url(base_url, topic, api_version), "base_url+topic"


def _build_record_payload(
    raw_json: str, key: str, api_version: str, embedded_format: str
) -> bytes:
    """Wrap a single JSON row as a Kafka REST v2 records payload."""
    _normalize_api_version(api_version)
    _normalize_embedded_format(embedded_format)
    body: Dict[str, Any] = {"records": [{"key": key, "value": json.loads(raw_json)}]}
    return json.dumps(body).encode("utf-8")


def _build_record_key(product_id: str, run_timestamp: str, row_index: int) -> str:
    """Build the Kafka message key for one stats record.
        Format: ``{product_id}:{run_timestamp}:{row_index}``.
    """
    pid = str(product_id) if product_id else "unknown"
    return f"{pid}:{run_timestamp}:{row_index}"


def _resolve_publish_context(
    rest_options: Dict[str, Any],
) -> Tuple[str, str, Dict[str, str], Callable[[str, str], bytes]]:
    """Resolve the produce URL, URL mode, headers, and payload builder.

    Returns ``(url, url_mode, headers, build_payload)`` where ``url_mode`` is
    one of ``"full_url"`` / ``"base_url+topic"`` (see :func:`_resolve_produce_url`).
    ``build_payload`` is a ``(raw_json, key) -> bytes`` callable that wraps the
    row value and message key into the Confluent v2 ``{"records": [...]}``
    envelope.
    """
    api_version = rest_options.get("api_version", DEFAULT_REST_API_VERSION)
    embedded_format = rest_options.get("embedded_format", DEFAULT_REST_EMBEDDED_FORMAT)

    url, url_mode = _resolve_produce_url(rest_options)
    headers = _build_rest_headers(api_version, embedded_format)

    def build_payload(raw_json: str, key: str) -> bytes:
        return _build_record_payload(raw_json, key, api_version, embedded_format)

    return url, url_mode, headers, build_payload


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
        timeout = _resolve_timeout(rest_options)
        verify = bool(rest_options.get("verify_ssl", True))
        url, url_mode, headers, build_payload = _resolve_publish_context(rest_options)
        topic_label = rest_options.get("topic") or url

        auth = rest_options.get("auth")  # tuple(user, secret) for basic; None otherwise
        auth_headers = rest_options.get("auth_headers") or {}
        if auth_headers:
            # Merge without letting auth headers overwrite Content-Type / Accept.
            merged = dict(auth_headers)
            merged.update(headers)
            headers = merged

        stats_df = _write_args.get("stats_df")
        # Convert se_job_metadata from JSON string to a proper struct so it appears as a nested object 
        stats_df = apply_se_job_metadata_struct(stats_df)
        rows = stats_df.selectExpr("to_json(struct(*)) AS value").collect()
        total_rows = len(rows)
        _log.info(f"collected {total_rows} stats row(s) for kafka REST proxy publish to topic: {topic_label}")

        # Kafka message key — always populated. Compacted topics
        # (``cleanup.policy=compact``) reject value-only records
        product_id = _write_args.get("product_id") or "unknown"
        run_timestamp = datetime.now(timezone.utc).isoformat()

        session = _build_session(rest_options)
        total_bytes_sent = 0
        total_retries = 0
        api_version = rest_options.get("api_version", DEFAULT_REST_API_VERSION)
        embedded_format = rest_options.get("embedded_format", DEFAULT_REST_EMBEDDED_FORMAT)
        auth_mode = "basic" if auth else ("bearer" if "Authorization" in headers else "none")
        _log.info(
            f"started write stats data into kafka REST proxy topic: {topic_label} "
            f"(url_mode={url_mode}, url={url}, rows={total_rows}, "
            f"api_version={api_version}, embedded_format={embedded_format}, "
            f"connect_timeout={timeout[0]}s, read_timeout={timeout[1]}s, auth={auth_mode}, "
            f"key_prefix={product_id}:{run_timestamp})"
        )
        try:
            for idx, row in enumerate(rows):
                raw_json = row["value"]
                record_key = _build_record_key(product_id, run_timestamp, idx)
                payload_bytes = build_payload(raw_json, record_key)
                try:
                    response = session.post(
                        url,
                        data=payload_bytes,
                        headers=headers,
                        timeout=timeout,
                        verify=verify,
                        auth=auth,
                    )
                except Exception as exc:  # pylint: disable=broad-except
                    raise SparkExpectationsMiscException(
                        f"error occurred while saving data into kafka (REST) topic "
                        f"'{topic_label}' at '{url}': {exc}"
                    ) from exc

                row_retries = _retry_count_from_response(response)
                total_retries += row_retries
                total_bytes_sent += len(payload_bytes)
                if row_retries:
                    _log.warning(
                        f"kafka REST proxy request to topic '{topic_label}' retried {row_retries} "
                        f"time(s) before status {response.status_code}"
                    )

                self._raise_for_record_errors(response, topic_label, url)
        finally:
            session.close()

        _log.info(
            f"ended writing stats data into kafka REST proxy topic: {topic_label} "
            f"(url_mode={url_mode}, rows={total_rows}, bytes_sent={total_bytes_sent}, "
            f"total_retries={total_retries})"
        )

    @staticmethod
    def _raise_for_record_errors(response: requests.Response, topic: str, url: str) -> None:
        if response.status_code >= 400:
            raise SparkExpectationsMiscException(
                f"REST proxy HTTP {response.status_code} for topic '{topic}' at "
                f"'{url}': {response.text}"
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
                    f"REST proxy record error for topic '{topic}' at '{url}': {offset}"
                )
        if payload.get("error_code") is not None:
            raise SparkExpectationsMiscException(
                f"REST proxy record error for topic '{topic}' at '{url}': {payload}"
            )
