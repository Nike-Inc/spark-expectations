"""Shared logic for converting rule definitions to flat row dicts.

The supported input format is a **rules-list** dict with top-level
``product_id``, optional ``table_name`` or ``dq_env``, optional
``defaults``, and a ``rules`` list::

    product_id: my_product
    table_name: db.my_table
    defaults:
      action_if_failed: ignore
    rules:
      - rule: col1_not_null
        rule_type: row_dq
        expectation: "col1 IS NOT NULL"

When ``dq_env`` is present instead of ``table_name``/``defaults``,
an ``env`` parameter selects which environment block to use::

    product_id: my_product
    dq_env:
      DEV:
        table_name: catalog.schema.orders
        action_if_failed: ignore
        is_active: true
        priority: medium
      PROD:
        table_name: catalog.schema.orders
        action_if_failed: fail
        priority: high
    rules:
      - rule: col1_not_null
        rule_type: row_dq
        expectation: "col1 IS NOT NULL"
"""

from typing import Any, Dict, List, Optional

from spark_expectations.core.exceptions import SparkExpectationsUserInputOrConfigInvalidException

VALID_RULE_TYPES = {"row_dq", "agg_dq", "query_dq"}

RULES_SCHEMA_COLUMNS = [
    "product_id",
    "table_name",
    "rule_type",
    "rule",
    "column_name",
    "expectation",
    "action_if_failed",
    "tag",
    "description",
    "enable_for_source_dq_validation",
    "enable_for_target_dq_validation",
    "is_active",
    "enable_error_drop_alert",
    "error_drop_threshold",
    "query_dq_delimiter",
    "enable_querydq_custom_output",
    "priority",
]

COLUMN_DEFAULTS: Dict[str, Any] = {
    "column_name": "",
    "expectation": "",
    "action_if_failed": "ignore",
    "tag": "",
    "description": "",
    "enable_for_source_dq_validation": True,
    "enable_for_target_dq_validation": True,
    "is_active": True,
    "enable_error_drop_alert": False,
    "error_drop_threshold": 0,
    "query_dq_delimiter": "",
    "enable_querydq_custom_output": False,
    "priority": "medium",
}

REQUIRED_RULE_FIELDS = {"rule", "expectation"}


def flatten_rules_list(
    data: Dict[str, Any], env: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Convert a rules-list definition into a flat list of row dicts.

    Expected structure (classic)::

        product_id: ...
        table_name: ...          # applied to every rule unless overridden
        defaults:
          rule_type: row_dq      # any column can be defaulted
          action_if_failed: ignore
          ...
        rules:
          - rule: ...
            expectation: ...
            rule_type: row_dq    # can override per-rule

    Expected structure (dq_env)::

        product_id: ...
        dq_env:
          dev:
            table_name: ...
            action_if_failed: ignore
            ...
          prod:
            table_name: ...
            ...
        rules:
          - rule: ...
            expectation: ...

    When ``dq_env`` is present the *env* parameter selects which
    environment block supplies the ``table_name`` and default values.

    Returns:
        List of dicts, each representing one rule row.
    """
    product_id = data.get("product_id")
    if not product_id:
        raise SparkExpectationsUserInputOrConfigInvalidException(
            "'product_id' is required at the top level of the rules file."
        )

    dq_env = data.get("dq_env")
    if dq_env is not None:
        if not isinstance(dq_env, dict) or not dq_env:
            raise SparkExpectationsUserInputOrConfigInvalidException(
                "'dq_env' must be a non-empty mapping of environment names to config."
            )
        if not env:
            raise SparkExpectationsUserInputOrConfigInvalidException(
                "'dq_env' is present in the rules file but no environment was "
                "specified. Pass the environment via options={'dq_env': '<env>'}."
            )
        env_lower = env.lower()
        env_map = {k.lower(): v for k, v in dq_env.items()}
        env_config = env_map.get(env_lower)
        if not env_config or not isinstance(env_config, dict):
            raise SparkExpectationsUserInputOrConfigInvalidException(
                f"Environment '{env}' not found in 'dq_env'. "
                f"Available environments: {sorted(dq_env.keys())}."
            )
        table_name = env_config.get("table_name", "")
        env_defaults = {k: v for k, v in env_config.items() if k != "table_name"}
        user_defaults = {**(data.get("defaults") or {}), **env_defaults}
    else:
        table_name = data.get("table_name", "")
        user_defaults = data.get("defaults") or {}

    merged_defaults = {**COLUMN_DEFAULTS, **user_defaults}

    rules_list = data.get("rules")
    if not rules_list or not isinstance(rules_list, list):
        raise SparkExpectationsUserInputOrConfigInvalidException(
            "'rules' must be a non-empty list of rule definitions."
        )

    rows: List[Dict[str, Any]] = []
    for rule_def in rules_list:
        if not isinstance(rule_def, dict):
            raise SparkExpectationsUserInputOrConfigInvalidException(
                "Each entry in 'rules' must be a dict."
            )

        missing = REQUIRED_RULE_FIELDS - set(rule_def.keys())
        if missing:
            raise SparkExpectationsUserInputOrConfigInvalidException(
                f"Rule '{rule_def.get('rule', '<unknown>')}' is missing required fields: {sorted(missing)}."
            )

        row = {**merged_defaults, **rule_def}
        row["product_id"] = product_id
        if "table_name" not in rule_def and table_name:
            row["table_name"] = table_name

        rule_type = row.get("rule_type", "")
        if rule_type and rule_type not in VALID_RULE_TYPES:
            raise SparkExpectationsUserInputOrConfigInvalidException(
                f"Invalid rule_type '{rule_type}' for rule '{row.get('rule')}'. "
                f"Must be one of {sorted(VALID_RULE_TYPES)}."
            )

        rows.append(_normalise_row(row))

    return rows


# ── helpers ──────────────────────────────────────────────────────────────


def _normalise_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure all schema columns are present and cast booleans/ints to strings."""
    normalised: Dict[str, Any] = {}
    for col in RULES_SCHEMA_COLUMNS:
        value = row.get(col, COLUMN_DEFAULTS.get(col, ""))
        normalised[col] = _to_str(value)
    return normalised


def _to_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value)
    return str(value)
