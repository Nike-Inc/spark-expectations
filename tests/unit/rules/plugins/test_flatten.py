"""Tests for spark_expectations.rules.plugins._flatten"""

import pytest

from spark_expectations.core.exceptions import SparkExpectationsUserInputOrConfigInvalidException
from spark_expectations.rules.plugins._flatten import (
    COLUMN_DEFAULTS,
    RULES_SCHEMA_COLUMNS,
    _detect_format,
    _to_str,
    flatten_flat,
    flatten_hierarchical,
    flatten_rules,
    flatten_rules_list,
    flatten_validations,
)


@pytest.fixture
def minimal_rules_list():
    return {
        "product_id": "prod1",
        "table_name": "db.table1",
        "rules": [
            {
                "rule": "col1_not_null",
                "rule_type": "row_dq",
                "expectation": "col1 IS NOT NULL",
                "column_name": "col1",
                "tag": "completeness",
            }
        ],
    }


@pytest.fixture
def minimal_hierarchical():
    return {
        "product_id": "prod1",
        "tables": {
            "db.table1": {
                "row_dq": [
                    {
                        "rule": "col1_not_null",
                        "expectation": "col1 IS NOT NULL",
                        "column_name": "col1",
                        "tag": "completeness",
                    }
                ]
            }
        },
    }


def test_detect_format_flat_list():
    assert _detect_format([{"rule": "r1"}]) == "flat"


def test_detect_format_rules_list_dict():
    assert _detect_format({"rules": []}) == "rules_list"


def test_detect_format_hierarchical_dict():
    assert _detect_format({"tables": {}}) == "hierarchical"


def test_detect_format_rules_key_takes_precedence_over_tables():
    assert _detect_format({"rules": [], "tables": {}}) == "rules_list"


def test_detect_format_invalid_dict_no_known_key():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException):
        _detect_format({"product_id": "p1"})


def test_detect_format_invalid_type():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException):
        _detect_format("not a dict or list")


def test_flatten_rules_list_basic(minimal_rules_list):
    rows = flatten_rules_list(minimal_rules_list)
    assert len(rows) == 1
    row = rows[0]
    assert row["product_id"] == "prod1"
    assert row["table_name"] == "db.table1"
    assert row["rule_type"] == "row_dq"
    assert row["rule"] == "col1_not_null"
    assert row["expectation"] == "col1 IS NOT NULL"


def test_flatten_rules_list_all_schema_columns_present(minimal_rules_list):
    rows = flatten_rules_list(minimal_rules_list)
    for col in RULES_SCHEMA_COLUMNS:
        assert col in rows[0], f"Missing column: {col}"


def test_flatten_rules_list_defaults_applied(minimal_rules_list):
    rows = flatten_rules_list(minimal_rules_list)
    row = rows[0]
    assert row["action_if_failed"] == str(COLUMN_DEFAULTS["action_if_failed"])
    assert row["is_active"] == str(COLUMN_DEFAULTS["is_active"])
    assert row["priority"] == str(COLUMN_DEFAULTS["priority"])


def test_flatten_rules_list_user_defaults_override_column_defaults():
    data = {
        "product_id": "prod1",
        "table_name": "t1",
        "defaults": {"action_if_failed": "fail", "priority": "high"},
        "rules": [{"rule": "r1", "rule_type": "row_dq", "expectation": "x > 0"}],
    }
    rows = flatten_rules_list(data)
    assert rows[0]["action_if_failed"] == "fail"
    assert rows[0]["priority"] == "high"


def test_flatten_rules_list_rule_level_overrides_defaults():
    data = {
        "product_id": "prod1",
        "table_name": "t1",
        "defaults": {"action_if_failed": "ignore"},
        "rules": [
            {
                "rule": "r1",
                "rule_type": "row_dq",
                "expectation": "x > 0",
                "action_if_failed": "drop",
            }
        ],
    }
    rows = flatten_rules_list(data)
    assert rows[0]["action_if_failed"] == "drop"


def test_flatten_rules_list_rule_type_from_defaults():
    data = {
        "product_id": "prod1",
        "table_name": "t1",
        "defaults": {"rule_type": "row_dq"},
        "rules": [{"rule": "r1", "expectation": "x > 0"}],
    }
    rows = flatten_rules_list(data)
    assert rows[0]["rule_type"] == "row_dq"


def test_flatten_rules_list_rule_type_per_rule_overrides_default():
    data = {
        "product_id": "prod1",
        "table_name": "t1",
        "defaults": {"rule_type": "row_dq"},
        "rules": [
            {"rule": "r1", "expectation": "x > 0"},
            {"rule": "r2", "rule_type": "agg_dq", "expectation": "count(*) > 0"},
        ],
    }
    rows = flatten_rules_list(data)
    assert rows[0]["rule_type"] == "row_dq"
    assert rows[1]["rule_type"] == "agg_dq"


def test_flatten_rules_list_table_name_override_per_rule():
    data = {
        "product_id": "prod1",
        "table_name": "default_table",
        "rules": [
            {"rule": "r1", "rule_type": "row_dq", "expectation": "x > 0"},
            {"rule": "r2", "rule_type": "row_dq", "expectation": "y > 0", "table_name": "other_table"},
        ],
    }
    rows = flatten_rules_list(data)
    assert rows[0]["table_name"] == "default_table"
    assert rows[1]["table_name"] == "other_table"


def test_flatten_rules_list_mixed_rule_types():
    data = {
        "product_id": "prod1",
        "table_name": "t1",
        "rules": [
            {"rule": "r1", "rule_type": "row_dq", "expectation": "c1 > 0"},
            {"rule": "r2", "rule_type": "agg_dq", "expectation": "count(*) > 10"},
            {"rule": "r3", "rule_type": "query_dq", "expectation": "SELECT 1"},
        ],
    }
    rows = flatten_rules_list(data)
    assert len(rows) == 3
    assert {r["rule_type"] for r in rows} == {"row_dq", "agg_dq", "query_dq"}


def test_flatten_rules_list_missing_product_id_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="product_id"):
        flatten_rules_list({"rules": [{"rule": "r", "expectation": "x"}]})


def test_flatten_rules_list_empty_rules_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="non-empty"):
        flatten_rules_list({"product_id": "p1", "rules": []})


def test_flatten_rules_list_missing_rule_field_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="missing required"):
        flatten_rules_list({
            "product_id": "p1",
            "rules": [{"expectation": "x > 0"}],
        })


def test_flatten_rules_list_missing_expectation_field_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="missing required"):
        flatten_rules_list({
            "product_id": "p1",
            "rules": [{"rule": "r1"}],
        })


def test_flatten_rules_list_invalid_rule_type_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="Invalid rule_type"):
        flatten_rules_list({
            "product_id": "p1",
            "rules": [{"rule": "r1", "rule_type": "bad_type", "expectation": "x > 0"}],
        })


def test_flatten_rules_list_boolean_values_stringified(minimal_rules_list):
    rows = flatten_rules_list(minimal_rules_list)
    row = rows[0]
    assert row["is_active"] in ("True", "False")
    assert row["enable_error_drop_alert"] in ("True", "False")


def test_flatten_rules_list_no_table_name_at_top_or_rule():
    data = {
        "product_id": "p1",
        "rules": [{"rule": "r1", "rule_type": "row_dq", "expectation": "x > 0"}],
    }
    rows = flatten_rules_list(data)
    assert rows[0]["table_name"] == ""


def test_flatten_hierarchical_basic(minimal_hierarchical):
    rows = flatten_hierarchical(minimal_hierarchical)
    assert len(rows) == 1
    row = rows[0]
    assert row["product_id"] == "prod1"
    assert row["table_name"] == "db.table1"
    assert row["rule_type"] == "row_dq"
    assert row["rule"] == "col1_not_null"
    assert row["expectation"] == "col1 IS NOT NULL"
    assert row["column_name"] == "col1"


def test_flatten_hierarchical_all_schema_columns_present(minimal_hierarchical):
    rows = flatten_hierarchical(minimal_hierarchical)
    for col in RULES_SCHEMA_COLUMNS:
        assert col in rows[0], f"Missing column: {col}"


def test_flatten_hierarchical_defaults_applied(minimal_hierarchical):
    rows = flatten_hierarchical(minimal_hierarchical)
    row = rows[0]
    assert row["action_if_failed"] == str(COLUMN_DEFAULTS["action_if_failed"])
    assert row["is_active"] == str(COLUMN_DEFAULTS["is_active"])
    assert row["priority"] == str(COLUMN_DEFAULTS["priority"])


def test_flatten_hierarchical_user_defaults_override_column_defaults():
    data = {
        "product_id": "prod1",
        "defaults": {
            "action_if_failed": "fail",
            "priority": "high",
        },
        "tables": {
            "t1": {
                "row_dq": [
                    {"rule": "r1", "expectation": "x > 0"},
                ]
            }
        },
    }
    rows = flatten_hierarchical(data)
    assert rows[0]["action_if_failed"] == "fail"
    assert rows[0]["priority"] == "high"


def test_flatten_hierarchical_rule_level_overrides_defaults():
    data = {
        "product_id": "prod1",
        "defaults": {"action_if_failed": "ignore"},
        "tables": {
            "t1": {
                "row_dq": [
                    {
                        "rule": "r1",
                        "expectation": "x > 0",
                        "action_if_failed": "drop",
                    },
                ]
            }
        },
    }
    rows = flatten_hierarchical(data)
    assert rows[0]["action_if_failed"] == "drop"


def test_flatten_hierarchical_multiple_tables_and_rule_types():
    data = {
        "product_id": "prod1",
        "tables": {
            "t1": {
                "row_dq": [{"rule": "r1", "expectation": "c1 > 0"}],
                "agg_dq": [{"rule": "r2", "expectation": "count(*) > 10"}],
            },
            "t2": {
                "query_dq": [{"rule": "r3", "expectation": "SELECT 1"}],
            },
        },
    }
    rows = flatten_hierarchical(data)
    assert len(rows) == 3
    assert {r["table_name"] for r in rows} == {"t1", "t2"}
    assert {r["rule_type"] for r in rows} == {"row_dq", "agg_dq", "query_dq"}


def test_flatten_hierarchical_missing_product_id_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="product_id"):
        flatten_hierarchical({"tables": {"t1": {"row_dq": [{"rule": "r", "expectation": "x"}]}}})


def test_flatten_hierarchical_missing_tables_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="tables"):
        flatten_hierarchical({"product_id": "p1"})


def test_flatten_hierarchical_empty_tables_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="tables"):
        flatten_hierarchical({"product_id": "p1", "tables": {}})


def test_flatten_hierarchical_invalid_rule_type_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="Invalid rule_type"):
        flatten_hierarchical({
            "product_id": "p1",
            "tables": {"t1": {"bad_type": [{"rule": "r", "expectation": "x"}]}},
        })


def test_flatten_hierarchical_missing_rule_field_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="missing required"):
        flatten_hierarchical({
            "product_id": "p1",
            "tables": {"t1": {"row_dq": [{"expectation": "x > 0"}]}},
        })


def test_flatten_hierarchical_missing_expectation_field_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="missing required"):
        flatten_hierarchical({
            "product_id": "p1",
            "tables": {"t1": {"row_dq": [{"rule": "r1"}]}},
        })


def test_flatten_hierarchical_boolean_values_stringified(minimal_hierarchical):
    rows = flatten_hierarchical(minimal_hierarchical)
    row = rows[0]
    assert row["is_active"] in ("True", "False")
    assert row["enable_error_drop_alert"] in ("True", "False")


def test_flatten_hierarchical_no_rules_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="No rules"):
        flatten_hierarchical({
            "product_id": "p1",
            "tables": {"t1": {"row_dq": []}},
        })


def test_flatten_flat_basic():
    data = [
        {
            "product_id": "p1",
            "table_name": "t1",
            "rule_type": "row_dq",
            "rule": "r1",
            "expectation": "col1 > 0",
        }
    ]
    rows = flatten_flat(data)
    assert len(rows) == 1
    row = rows[0]
    assert row["product_id"] == "p1"
    assert row["action_if_failed"] == str(COLUMN_DEFAULTS["action_if_failed"])


def test_flatten_flat_all_columns_present():
    data = [
        {
            "product_id": "p1",
            "table_name": "t1",
            "rule_type": "row_dq",
            "rule": "r1",
            "expectation": "col1 > 0",
        }
    ]
    rows = flatten_flat(data)
    for col in RULES_SCHEMA_COLUMNS:
        assert col in rows[0], f"Missing column: {col}"


def test_flatten_flat_missing_required_field_raises():
    for missing_field in ("product_id", "table_name", "rule_type", "rule", "expectation"):
        data = [
            {
                "product_id": "p1",
                "table_name": "t1",
                "rule_type": "row_dq",
                "rule": "r1",
                "expectation": "col1 > 0",
            }
        ]
        del data[0][missing_field]
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match=missing_field):
            flatten_flat(data)


def test_flatten_flat_empty_list_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="empty"):
        flatten_flat([])


def test_flatten_rules_autodetect_flat():
    data = [
        {
            "product_id": "p1",
            "table_name": "t1",
            "rule_type": "row_dq",
            "rule": "r1",
            "expectation": "x > 0",
        }
    ]
    rows = flatten_rules(data)
    assert len(rows) == 1


def test_flatten_rules_autodetect_rules_list():
    data = {
        "product_id": "p1",
        "table_name": "t1",
        "rules": [{"rule": "r1", "rule_type": "row_dq", "expectation": "x > 0"}],
    }
    rows = flatten_rules(data)
    assert len(rows) == 1
    assert rows[0]["table_name"] == "t1"


def test_flatten_rules_autodetect_hierarchical():
    data = {
        "product_id": "p1",
        "tables": {"t1": {"row_dq": [{"rule": "r1", "expectation": "x > 0"}]}},
    }
    rows = flatten_rules(data)
    assert len(rows) == 1
    assert rows[0]["table_name"] == "t1"


# ── validations format ─────────────────────────────────────────────────


def test_detect_format_validations():
    assert _detect_format({"validations": []}) == "validations"


def test_detect_format_validations_takes_precedence_over_rules():
    assert _detect_format({"validations": [], "rules": []}) == "validations"


@pytest.fixture
def minimal_validations():
    return {
        "defaults": {"action_if_failed": "ignore", "priority": "medium"},
        "validations": [
            {
                "product_id": "prod1",
                "table_name": "db.table1",
                "rules": [
                    {
                        "rule": "col1_not_null",
                        "rule_type": "row_dq",
                        "expectation": "col1 IS NOT NULL",
                        "column_name": "col1",
                        "tag": "completeness",
                    }
                ],
            }
        ],
    }


def test_flatten_validations_basic(minimal_validations):
    rows = flatten_validations(minimal_validations)
    assert len(rows) == 1
    row = rows[0]
    assert row["product_id"] == "prod1"
    assert row["table_name"] == "db.table1"
    assert row["rule"] == "col1_not_null"
    assert row["action_if_failed"] == "ignore"
    assert row["priority"] == "medium"


def test_flatten_validations_all_schema_columns_present(minimal_validations):
    rows = flatten_validations(minimal_validations)
    for col in RULES_SCHEMA_COLUMNS:
        assert col in rows[0], f"Missing column: {col}"


def test_flatten_validations_multiple_entries():
    data = {
        "defaults": {"action_if_failed": "ignore"},
        "validations": [
            {
                "product_id": "p1",
                "table_name": "t1",
                "rules": [{"rule": "r1", "rule_type": "row_dq", "expectation": "x > 0"}],
            },
            {
                "product_id": "p2",
                "table_name": "t2",
                "rules": [{"rule": "r2", "rule_type": "agg_dq", "expectation": "count(*) > 0"}],
            },
        ],
    }
    rows = flatten_validations(data)
    assert len(rows) == 2
    assert rows[0]["product_id"] == "p1"
    assert rows[1]["product_id"] == "p2"
    assert rows[0]["action_if_failed"] == "ignore"
    assert rows[1]["action_if_failed"] == "ignore"


def test_flatten_validations_entry_defaults_override_top_defaults():
    data = {
        "defaults": {"action_if_failed": "ignore", "priority": "low"},
        "validations": [
            {
                "product_id": "p1",
                "table_name": "t1",
                "defaults": {"action_if_failed": "fail"},
                "rules": [{"rule": "r1", "rule_type": "row_dq", "expectation": "x > 0"}],
            },
        ],
    }
    rows = flatten_validations(data)
    assert rows[0]["action_if_failed"] == "fail"
    assert rows[0]["priority"] == "low"


def test_flatten_validations_no_top_defaults():
    data = {
        "validations": [
            {
                "product_id": "p1",
                "table_name": "t1",
                "rules": [{"rule": "r1", "rule_type": "row_dq", "expectation": "x > 0"}],
            },
        ],
    }
    rows = flatten_validations(data)
    assert len(rows) == 1
    assert rows[0]["action_if_failed"] == str(COLUMN_DEFAULTS["action_if_failed"])


def test_flatten_validations_empty_list_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="non-empty"):
        flatten_validations({"validations": []})


def test_flatten_validations_not_a_list_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="non-empty"):
        flatten_validations({"validations": "bad"})


def test_flatten_validations_entry_not_a_dict_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="must be a dict"):
        flatten_validations({"validations": ["not_a_dict"]})


def test_flatten_validations_missing_product_id_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="product_id"):
        flatten_validations({
            "validations": [
                {"table_name": "t1", "rules": [{"rule": "r1", "expectation": "x"}]},
            ],
        })


def test_flatten_rules_autodetect_validations():
    data = {
        "validations": [
            {
                "product_id": "p1",
                "table_name": "t1",
                "rules": [{"rule": "r1", "rule_type": "row_dq", "expectation": "x > 0"}],
            },
        ],
    }
    rows = flatten_rules(data)
    assert len(rows) == 1
    assert rows[0]["product_id"] == "p1"


# ── additional coverage for uncovered error paths ──────────────────────


def test_flatten_rules_list_non_dict_rule_entry_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="must be a dict"):
        flatten_rules_list({
            "product_id": "p1",
            "rules": ["not_a_dict"],
        })


def test_flatten_hierarchical_non_dict_rule_types_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="Expected a mapping"):
        flatten_hierarchical({
            "product_id": "p1",
            "tables": {"t1": "not_a_dict"},
        })


def test_flatten_hierarchical_non_list_rules_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="must be a list"):
        flatten_hierarchical({
            "product_id": "p1",
            "tables": {"t1": {"row_dq": "not_a_list"}},
        })


def test_to_str_none_returns_empty():
    assert _to_str(None) == ""


def test_to_str_bool():
    assert _to_str(True) == "True"
    assert _to_str(False) == "False"


def test_to_str_int():
    assert _to_str(42) == "42"


def test_to_str_string():
    assert _to_str("hello") == "hello"


def test_flatten_rules_list_rules_not_a_list_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="non-empty"):
        flatten_rules_list({
            "product_id": "p1",
            "rules": "not_a_list",
        })
