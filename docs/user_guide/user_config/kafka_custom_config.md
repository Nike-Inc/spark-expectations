# Kafka Streaming Custom Configuration
See this example config page for context about the user config: [examples page](../../../examples/#configurations).

Spark Expectations can publish DQ stats to Kafka using either the **native Kafka client** (`kafka_native`) or the **Kafka REST Proxy** (`kafka_rest`). Many users configure connection details through Databricks or Cerberus secrets. For local development—or when you are not using secret scopes—you can supply bootstrap servers, REST base URLs, and topic names directly in `user_conf`.

!!! important "Setup Note"
    The target Kafka topic (and, for native transport, the bootstrap server) must already exist when Spark Expectations runs. SE does not create them for you.

## Transport Selection

!!! info "user_config.se_streaming_transport"
    Selects how stats are published to Kafka.

    | Value | Behavior |
    |-------|----------|
    | `kafka_native` | Spark `write.format("kafka")` over the binary protocol (**default**) |
    | `kafka_rest` | HTTP `POST` to a Confluent-compatible Kafka REST Proxy (`/topics/{topic}`) **or** a fully-qualified HTTP-ingress produce URL (e.g. Nike NSP3) |

    Default: `kafka_native` (see `spark_expectations/config/spark-expectations-default-config.yaml`).

!!! tip "Two REST URL shapes"
    The `kafka_rest` transport supports **two** produce-URL styles:

    * **Confluent REST Proxy v2** — configure `se_streaming_rest_base_url` + `se_streaming_rest_topic_name`; SE composes `{base_url}/topics/{topic}`.
    * **Fully-qualified produce URL** — configure `se_streaming_rest_full_url`; SE POSTs to that URL **verbatim**, skipping the `/topics/{topic}` composition. Use this for HTTP-ingress endpoints where the stream URL is already the produce endpoint.

    When `full_url` resolves to a non-empty value it **wins** over the base+topic pair.

## Native Kafka Custom Configuration Parameters

Use these when `user_config.se_streaming_transport` is `kafka_native` (or omitted).

!!! info "user_config.se_streaming_stats_kafka_custom_config_enable"
    Master toggle to enable using custom Kafka parameters for the native client.

!!! info "user_config.se_streaming_stats_kafka_bootstrap_server"
    Kafka bootstrap server (for example, `localhost:9092`).

!!! info "user_config.se_streaming_stats_topic_name"
    Target topic name for native Kafka publishing.

!!! important "Native defaults"
    If **user_config.se_streaming_stats_kafka_custom_config_enable** is set to `True` but the topic and server options are not specified, the defaults from `spark_expectations/config/spark-expectations-default-config.yaml` are used.

## Kafka REST Configuration Parameters

Use these when `user_config.se_streaming_transport` is `kafka_rest`.

Connection details resolve **secret-scope-first** (when `user_config.secret_type` is `databricks` or `cerberus`), then fall back to the direct URL/topic parameters below.

### Direct configuration

!!! info "user_config.se_streaming_rest_base_url"
    Kafka REST Proxy base URL (for example, `https://kafka-rest.example.com` or `http://localhost:8082`). No trailing slash required. Used only when `se_streaming_rest_full_url` is not set.

!!! info "user_config.se_streaming_rest_topic_name"
    Topic name to publish to via the REST proxy. **Required** when using `se_streaming_rest_base_url`. **Optional** when using `se_streaming_rest_full_url` — in that mode it is used only as a logical label in log lines / metrics.

!!! info "user_config.se_streaming_rest_full_url"
    Fully-qualified produce URL. When set, SE POSTs to this URL **verbatim** and skips the `{base_url}/topics/{topic}` composition. Enables HTTP-ingress endpoints that treat the stream URL as the produce endpoint. Trailing slashes are trimmed.

    When both `se_streaming_rest_full_url` and `se_streaming_rest_base_url` are set, `full_url` wins.

### Secret key configuration

When `user_config.secret_type` is `databricks`, SE reads the REST URLs and topic from Databricks secret scope keys:

!!! info "user_config.dbx_rest_base_url"
    Databricks secret **key** whose value is the Kafka REST base URL.

!!! info "user_config.dbx_rest_topic_name"
    Databricks secret **key** whose value is the REST topic name.

!!! info "user_config.dbx_rest_full_url"
    Databricks secret **key** whose value is the fully-qualified produce URL. Same semantics as `se_streaming_rest_full_url` — when this key resolves to a non-empty value, SE POSTs verbatim.

When `user_config.secret_type` is `cerberus`, use the Cerberus equivalents:

!!! info "user_config.cbs_rest_base_url"
    Cerberus secret **key** whose value is the Kafka REST base URL.

!!! info "user_config.cbs_rest_topic_name"
    Cerberus secret **key** whose value is the REST topic name.

!!! info "user_config.cbs_rest_full_url"
    Cerberus secret **key** whose value is the fully-qualified produce URL.

If a secret key is configured for the active `secret_type`, that key is resolved at runtime. Direct `se_streaming_rest_*` values remain available as a fallback when no secret key is set.

!!! note "Secret backend precedence"
    When both Cerberus and Databricks keys are populated in `user_conf`, SE resolves auth-related secret keys **Cerberus-first, then Databricks**. Base URL and topic keys follow the same discipline. `user_config.secret_type` selects which backend performs the lookup at runtime.

### REST client options

!!! info "user_config.se_streaming_rest_embedded_format"
    Embedded record format for the REST produce request. Only `json` is supported today (default: `json`).

!!! info "user_config.se_streaming_rest_api_version"
    Kafka REST API version. Only `v2` is supported today (default: `v2`). Requests use `POST /topics/{topic}` with Confluent v2 media types.

!!! info "user_config.se_streaming_rest_timeout_sec"
    Legacy HTTP timeout in seconds, applied to both connect and read phases when the split timeout keys below are not set. Default: `30`.

!!! info "user_config.se_streaming_rest_connect_timeout_sec"
    Connect-phase timeout in seconds. Defaults to `se_streaming_rest_timeout_sec` when unset.

!!! info "user_config.se_streaming_rest_read_timeout_sec"
    Read-phase timeout in seconds. Defaults to `se_streaming_rest_timeout_sec` when unset.

!!! info "user_config.se_streaming_rest_verify_ssl"
    Whether to verify TLS certificates on REST requests. Default: `True`. Set to `False` for local HTTP endpoints.

!!! info "user_config.se_streaming_rest_max_retries"
    Maximum urllib3 retries for retryable HTTP statuses and connection errors on REST `POST` requests. Default: `3`.

!!! info "user_config.se_streaming_rest_backoff_factor"
    Exponential backoff factor between REST retries. Default: `0.5`.

!!! info "user_config.se_streaming_rest_pool_connections"
    Number of connection pools to cache in the REST HTTP client. Default: `4`.

!!! info "user_config.se_streaming_rest_pool_maxsize"
    Maximum pooled connections per host for the REST HTTP client. Default: `10`.

### Authentication

Kafka REST auth is opt-in. When `auth_type` is `none` (default), no credential keys are read.

!!! info "user_config.se_streaming_rest_auth_type"
    HTTP auth mode used to POST to the REST proxy. One of `none` \| `basic` \| `bearer`. Default: `none`. Unrecognised values log a WARNING and fall back to `none`.

!!! info "user_config.se_streaming_rest_username"
    Direct username for `basic` auth. Required (together with an auth-secret) when `auth_type` is `basic`; ignored otherwise.

!!! info "user_config.dbx_rest_auth_secret"
    Databricks secret **key** whose value is the credential — password for `basic`, bearer token for `bearer`.

!!! info "user_config.cbs_rest_auth_secret"
    Cerberus secret **key** whose value is the credential — password for `basic`, bearer token for `bearer`.

When `auth_type` is `basic` or `bearer`, SE resolves the auth-secret key using the same **Cerberus-first, then Databricks** precedence as base URL / topic. If a credentialed mode is selected but credentials do not resolve to non-empty values, SE raises `SparkExpectationsMiscException` — misconfiguration fails loudly rather than silently degrading to unauthenticated.

## Configuration Examples

### Native Kafka (local)

```python
from typing import Dict, Union
from spark_expectations.config.user_config import Constants as user_config

stats_streaming_config_dict: Dict[str, Union[bool, str]] = {
    user_config.se_enable_streaming: True,
    user_config.se_streaming_transport: "kafka_native",
    user_config.se_streaming_stats_kafka_custom_config_enable: True,
    user_config.se_streaming_stats_topic_name: "dq-sparkexpectations-stats",
    user_config.se_streaming_stats_kafka_bootstrap_server: "localhost:9092",
}
```

### Kafka REST (local, direct URL)

```python
from typing import Dict, Union
from spark_expectations.config.user_config import Constants as user_config

stats_streaming_config_dict: Dict[str, Union[bool, str, int]] = {
    user_config.se_enable_streaming: True,
    user_config.se_streaming_transport: "kafka_rest",
    user_config.se_streaming_rest_base_url: "http://localhost:8082",
    user_config.se_streaming_rest_topic_name: "dq-sparkexpectations-stats",
    user_config.se_streaming_rest_embedded_format: "json",
    user_config.se_streaming_rest_api_version: "v2",
    user_config.se_streaming_rest_timeout_sec: 30,
    user_config.se_streaming_rest_connect_timeout_sec: 10,
    user_config.se_streaming_rest_read_timeout_sec: 60,
    user_config.se_streaming_rest_verify_ssl: False,
    user_config.se_streaming_rest_max_retries: 3,
    user_config.se_streaming_rest_backoff_factor: 0.5,
}
```

### Kafka REST (Databricks secrets)

```python
from typing import Dict, Union
from spark_expectations.config.user_config import Constants as user_config

stats_streaming_config_dict: Dict[str, Union[bool, str]] = {
    user_config.se_enable_streaming: True,
    user_config.se_streaming_transport: "kafka_rest",
    user_config.secret_type: "databricks",
    user_config.dbx_workspace_url: "https://workspace.cloud.databricks.com",
    user_config.dbx_secret_scope: "secret_scope",
    user_config.dbx_rest_base_url: "se_streaming_rest_base_url_secret_key",
    user_config.dbx_rest_topic_name: "se_streaming_rest_topic_secret_key",
}
```

### Kafka REST — fully-qualified produce URL (Nike NSP3 HTTP ingress)

Nike's NSP3 HTTP-ingress bridge exposes the stream URL as the produce endpoint. Do **not** append `/topics/{topic}` — configure `se_streaming_rest_full_url` and SE will POST to the URL verbatim.

```python
from typing import Dict, Union
from spark_expectations.config.user_config import Constants as user_config

stats_streaming_config_dict: Dict[str, Union[bool, str, int]] = {
    user_config.se_enable_streaming: True,
    user_config.se_streaming_transport: "kafka_rest",
    # HTTP-ingress URL — used verbatim. No /topics/{topic} suffix appended.
    user_config.se_streaming_rest_full_url: (
        "https://123.ingest.abc.com/rest"
    ),
    # Optional: logical topic label used ONLY in log lines / metrics.
    user_config.se_streaming_rest_topic_name: "dq-sparkexpectations-stats",
    # NSP3 HTTP ingress typically requires bearer-token auth.
    user_config.se_streaming_rest_auth_type: "bearer",
    user_config.secret_type: "databricks",
    user_config.dbx_secret_scope: "sole_common_prod",
    user_config.dbx_rest_auth_secret: "nsp3_bearer_token_secret_key",
}
```

Fully-qualified URLs can also be sourced from a secret scope. Use `user_config.dbx_rest_full_url` (or `user_config.cbs_rest_full_url` for Cerberus) whose value is the Databricks / Cerberus secret **key** that resolves at runtime to the actual URL.

For end-to-end event shape and transport comparison, see [Metric Events](../../home/metric_events.md).
