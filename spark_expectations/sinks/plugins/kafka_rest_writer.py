import json
from typing import Any, Dict

import requests

from spark_expectations import _log
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.sinks.plugins.base_writer import (
    SparkExpectationsSinkWriter,
    spark_expectations_writer_impl,
)


_ACCEPT_HEADER = "application/vnd.kafka.v2+json"
_CONTENT_TYPE_JSON_V2 = "application/vnd.kafka.json.v2+json"


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

        timeout = int(rest_options.get("timeout_sec", 30))
        verify = bool(rest_options.get("verify_ssl", True))
        headers = {"Content-Type": _CONTENT_TYPE_JSON_V2, "Accept": _ACCEPT_HEADER}
        url = f"{str(base_url).rstrip('/')}/topics/{topic}"

        stats_df = _write_args.get("stats_df")
        rows = stats_df.selectExpr("to_json(struct(*)) AS value").collect()
        _log.info(f"started write stats data into kafka REST proxy topic: {topic}")
        for row in rows:
            raw_json = row["value"]
            body = {"records": [{"value": json.loads(raw_json)}]}
            try:
                response = requests.post(
                    url,
                    data=json.dumps(body),
                    headers=headers,
                    timeout=timeout,
                    verify=verify,
                )
            except Exception as exc:  # pylint: disable=broad-except
                raise SparkExpectationsMiscException(
                    f"error occurred while saving data into kafka (REST) topic "
                    f"'{topic}' at '{base_url}': {exc}"
                ) from exc

            self._raise_for_record_errors(response, topic, base_url)
        _log.info(f"ended writing stats data into kafka REST proxy topic: {topic}")

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
