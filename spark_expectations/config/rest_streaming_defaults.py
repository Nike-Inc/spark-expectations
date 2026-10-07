"""Default values for Kafka REST streaming.

Keep in sync with ``spark-expectations-default-config.yaml``.
"""

DEFAULT_REST_API_VERSION = "v2"
DEFAULT_REST_EMBEDDED_FORMAT = "json"
DEFAULT_REST_TIMEOUT_SEC = 30
DEFAULT_REST_VERIFY_SSL = True
DEFAULT_REST_MAX_RETRIES = 3
DEFAULT_REST_BACKOFF_FACTOR = 0.5
DEFAULT_REST_POOL_CONNECTIONS = 4
DEFAULT_REST_POOL_MAXSIZE = 10
DEFAULT_REST_CONNECT_TIMEOUT_SEC = 30
DEFAULT_REST_READ_TIMEOUT_SEC = 30

# Kafka REST proxy HTTP auth (``se.streaming.rest.auth.type``).
# ``none`` is the default — unauthenticated POST. ``basic`` and ``bearer`` are
# credentialed modes; credential config is read only when one of those is set.
DEFAULT_REST_AUTH_TYPE = "none"
REST_AUTH_TYPES = frozenset({"none", "basic", "bearer"})
