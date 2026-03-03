"""Shared logic for converting rule definitions to flat row dicts.

Four input formats are supported:

1. **validations** -- a dict with optional top-level ``defaults`` and a
   ``validations`` list, where each entry contains ``product_id``,
   ``table_name``, and a ``rules`` list::

       defaults:
         action_if_failed: ignore
       validations:
         - product_id: my_product
           table_name: db.my_table
           rules:
             - rule: col1_not_null
               rule_type: row_dq
               expectation: "col1 IS NOT NULL"

2. **rules-list** -- a dict with top-level ``product_id``,
   ``table_name``, optional ``defaults``, and a ``rules`` list::

       product_id: my_product
       table_name: db.my_table
       defaults:
         action_if_failed: ignore
       rules:
         - rule: col1_not_null
           rule_type: row_dq
           expectation: "col1 IS NOT NULL"

3. **hierarchical** -- a dict with ``tables`` mapping
   ``table_name -> rule_type -> [rules]``.

4. **flat** -- a bare list of fully-specified rule dicts.
"""

from typing import Any, Dict, List

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


def _detect_format(data: Any) -> str:
    """Return ``'validations'``, ``'rules_list'``, ``'hierarchical'``, or ``'flat'``."""
    if isinstance(data, list):
        return "flat"
    if isinstance(data, dict):
        if "validations" in data:
            return "validations"
        if "rules" in data:
            return "rules_list"
        if "tables" in data:
            return "hierarchical"
    raise SparkExpectationsUserInputOrConfigInvalidException(
        "Rules data must be a dict with a 'validations' key (validations format), "
        "a dict with a 'rules' key (rules-list format), "
        "a dict with a 'tables' key (hierarchical format), "
        "or a bare list of rule dicts (flat format)."
    )


# ── validations format ──────────────────────────────────────────────────


def flatten_validations(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Convert a validations-wrapper definition into a flat list of row dicts.

    Expected structure::

        defaults:                    # shared defaults (optional)
          action_if_failed: ignore
        validations:
          - product_id: ...
            table_name: ...
            rules:
              - rule: ...
                expectation: ...

    Each entry in ``validations`` is processed like a rules-list definition,
    with top-level ``defaults`` merged in (entry-level defaults take precedence).

    Returns:
        List of dicts, each representing one rule row.
    """
    top_defaults = data.get("defaults") or {}

    validations_list = data.get("validations")
    if not validations_list or not isinstance(validations_list, list):
        raise SparkExpectationsUserInputOrConfigInvalidException(
            "'validations' must be a non-empty list."
        )

    rows: List[Dict[str, Any]] = []
    for entry in validations_list:
        if not isinstance(entry, dict):
            raise SparkExpectationsUserInputOrConfigInvalidException(
                "Each entry in 'validations' must be a dict with 'product_id', "
                "'table_name', and 'rules'."
            )
        entry_defaults = entry.get("defaults") or {}
        merged = {**top_defaults, **entry_defaults}
        rules_list_data: Dict[str, Any] = {**entry, "defaults": merged}
        rows.extend(flatten_rules_list(rules_list_data))

    if not rows:
        raise SparkExpectationsUserInputOrConfigInvalidException(
            "No rules found in the validations file. At least one rule must be provided."
        )

    return rows


# ── rules-list format ──────────────────────────────────────────────────


def flatten_rules_list(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Convert a rules-list definition into a flat list of row dicts.

    Expected structure::

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

    Returns:
        List of dicts, each representing one rule row.
    """
    product_id = data.get("product_id")
    if not product_id:
        raise SparkExpectationsUserInputOrConfigInvalidException(
            "'product_id' is required at the top level of the rules file."
        )

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

    if not rows:
        raise SparkExpectationsUserInputOrConfigInvalidException(
            "No rules found in the rules file. At least one rule must be provided."
        )

    return rows


# ── hierarchical format ─────────────────────────────────────────────────


def flatten_hierarchical(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Convert a hierarchical rule definition into a flat list of row dicts.

    Expected structure::

        product_id: ...
        defaults:
          action_if_failed: ignore
          ...
        tables:
          <table_name>:
            <rule_type>:        # row_dq | agg_dq | query_dq
              - rule: ...
                expectation: ...

    Returns:
        List of dicts, each representing one rule row.
    """
    product_id = data.get("product_id")
    if not product_id:
        raise SparkExpectationsUserInputOrConfigInvalidException(
            "'product_id' is required at the top level of the rules file."
        )

    user_defaults = data.get("defaults") or {}
    merged_defaults = {**COLUMN_DEFAULTS, **user_defaults}

    tables = data.get("tables")
    if not tables or not isinstance(tables, dict):
        raise SparkExpectationsUserInputOrConfigInvalidException(
            "'tables' must be a non-empty mapping of table_name -> rule_type -> rules."
        )

    rows: List[Dict[str, Any]] = []
    for table_name, rule_types in tables.items():
        if not isinstance(rule_types, dict):
            raise SparkExpectationsUserInputOrConfigInvalidException(
                f"Expected a mapping of rule_type -> rules under table '{table_name}', "
                f"got {type(rule_types).__name__}."
            )
        for rule_type, rules_list in rule_types.items():
            if rule_type not in VALID_RULE_TYPES:
                raise SparkExpectationsUserInputOrConfigInvalidException(
                    f"Invalid rule_type '{rule_type}' under table '{table_name}'. "
                    f"Must be one of {sorted(VALID_RULE_TYPES)}."
                )
            if not isinstance(rules_list, list):
                raise SparkExpectationsUserInputOrConfigInvalidException(
                    f"Rules under '{table_name}.{rule_type}' must be a list."
                )
            for rule_def in rules_list:
                _validate_rule_fields(rule_def, table_name, rule_type)
                row = {**merged_defaults, **rule_def}
                row["product_id"] = product_id
                row["table_name"] = table_name
                row["rule_type"] = rule_type
                rows.append(_normalise_row(row))

    if not rows:
        raise SparkExpectationsUserInputOrConfigInvalidException(
            "No rules found in the rules file. At least one rule must be provided."
        )

    return rows


# ── flat format ─────────────────────────────────────────────────────────


def flatten_flat(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalise a flat list of rule dicts, filling in defaults for missing columns."""
    if not data:
        raise SparkExpectationsUserInputOrConfigInvalidException(
            "Rules list is empty. At least one rule must be provided."
        )

    rows: List[Dict[str, Any]] = []
    for rule_def in data:
        for field in ("product_id", "table_name", "rule_type", "rule", "expectation"):
            if not rule_def.get(field):
                raise SparkExpectationsUserInputOrConfigInvalidException(
                    f"'{field}' is required in every rule dict (flat format)."
                )
        row = {**COLUMN_DEFAULTS, **rule_def}
        rows.append(_normalise_row(row))

    return rows


# ── auto-detect entry point ─────────────────────────────────────────────


def flatten_rules(data: Any) -> List[Dict[str, Any]]:
    """Auto-detect format and flatten to a list of row dicts."""
    fmt = _detect_format(data)
    if fmt == "validations":
        return flatten_validations(data)
    if fmt == "rules_list":
        return flatten_rules_list(data)
    if fmt == "hierarchical":
        return flatten_hierarchical(data)
    return flatten_flat(data)


# ── helpers ──────────────────────────────────────────────────────────────


def _validate_rule_fields(rule_def: Dict[str, Any], table_name: str, rule_type: str) -> None:
    missing = REQUIRED_RULE_FIELDS - set(rule_def.keys())
    if missing:
        raise SparkExpectationsUserInputOrConfigInvalidException(
            f"Rule under '{table_name}.{rule_type}' is missing required fields: {sorted(missing)}."
        )


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
