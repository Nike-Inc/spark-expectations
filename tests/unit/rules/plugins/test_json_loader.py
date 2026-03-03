"""Tests for spark_expectations.rules.plugins.json_loader"""

import json
import os
from unittest.mock import patch

import pytest

from spark_expectations.core import get_spark_session
from spark_expectations.core.exceptions import SparkExpectationsUserInputOrConfigInvalidException
from spark_expectations.rules.plugins._flatten import RULES_SCHEMA_COLUMNS
from spark_expectations.rules.plugins.json_loader import SparkExpectationsJsonRuleLoaderImpl

get_spark_session()

SAMPLE_RULES_JSON = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "..", "examples", "resources", "sample_rules.json"
)


@pytest.fixture
def json_loader():
    return SparkExpectationsJsonRuleLoaderImpl()


@pytest.fixture
def rules_list_json_file(tmp_path):
    data = {
        "product_id": "test_product",
        "table_name": "db.test_table",
        "defaults": {
            "action_if_failed": "ignore",
            "is_active": True,
            "priority": "medium",
        },
        "rules": [
            {
                "rule": "col1_not_null",
                "rule_type": "row_dq",
                "column_name": "col1",
                "expectation": "col1 IS NOT NULL",
                "action_if_failed": "drop",
                "tag": "completeness",
                "description": "col1 must not be null",
            },
            {
                "rule": "col2_positive",
                "rule_type": "row_dq",
                "column_name": "col2",
                "expectation": "col2 > 0",
                "tag": "validity",
                "description": "col2 must be positive",
            },
            {
                "rule": "row_count",
                "rule_type": "agg_dq",
                "column_name": "",
                "expectation": "count(*) > 0",
                "action_if_failed": "fail",
                "tag": "completeness",
                "description": "Must have rows",
            },
        ],
    }
    path = tmp_path / "rules.json"
    path.write_text(json.dumps(data, indent=2))
    return str(path)


@pytest.fixture
def hierarchical_json_file(tmp_path):
    data = {
        "product_id": "test_product",
        "defaults": {
            "action_if_failed": "ignore",
            "is_active": True,
            "priority": "medium",
        },
        "tables": {
            "db.test_table": {
                "row_dq": [
                    {
                        "rule": "col1_not_null",
                        "column_name": "col1",
                        "expectation": "col1 IS NOT NULL",
                        "action_if_failed": "drop",
                        "tag": "completeness",
                        "description": "col1 must not be null",
                    },
                ],
                "agg_dq": [
                    {
                        "rule": "row_count",
                        "column_name": "",
                        "expectation": "count(*) > 0",
                        "action_if_failed": "fail",
                        "tag": "completeness",
                        "description": "Must have rows",
                    }
                ],
            }
        },
    }
    path = tmp_path / "rules_hier.json"
    path.write_text(json.dumps(data, indent=2))
    return str(path)


@pytest.fixture
def flat_json_file(tmp_path):
    data = [
        {
            "product_id": "test_product",
            "table_name": "db.test_table",
            "rule_type": "row_dq",
            "rule": "col1_not_null",
            "column_name": "col1",
            "expectation": "col1 IS NOT NULL",
            "action_if_failed": "drop",
            "tag": "completeness",
            "description": "col1 must not be null",
        }
    ]
    path = tmp_path / "rules_flat.json"
    path.write_text(json.dumps(data, indent=2))
    return str(path)


def test_returns_none_for_non_json_format(json_loader):
    result = json_loader.load_rules(path="rules.yaml", format="yaml", options={})
    assert result is None


def test_returns_none_for_auto_non_json_extension(json_loader):
    result = json_loader.load_rules(path="rules.yaml", format="auto", options={})
    assert result is None


def test_handles_json_format_explicit(json_loader, rules_list_json_file):
    df = json_loader.load_rules(path=rules_list_json_file, format="json", options={})
    assert df is not None
    assert df.count() == 3
    assert set(df.columns) == set(RULES_SCHEMA_COLUMNS)


def test_handles_auto_json_extension(json_loader, rules_list_json_file):
    df = json_loader.load_rules(path=rules_list_json_file, format="auto", options={})
    assert df is not None
    assert df.count() == 3


def test_rules_list_values(json_loader, rules_list_json_file):
    df = json_loader.load_rules(path=rules_list_json_file, format="json", options={})
    rows = [r.asDict() for r in df.collect()]
    row_dq_rules = [r for r in rows if r["rule_type"] == "row_dq"]
    agg_dq_rules = [r for r in rows if r["rule_type"] == "agg_dq"]

    assert len(row_dq_rules) == 2
    assert len(agg_dq_rules) == 1

    col1_rule = next(r for r in row_dq_rules if r["rule"] == "col1_not_null")
    assert col1_rule["product_id"] == "test_product"
    assert col1_rule["table_name"] == "db.test_table"
    assert col1_rule["action_if_failed"] == "drop"

    col2_rule = next(r for r in row_dq_rules if r["rule"] == "col2_positive")
    assert col2_rule["action_if_failed"] == "ignore"
    assert col2_rule["priority"] == "medium"


def test_hierarchical_json(json_loader, hierarchical_json_file):
    df = json_loader.load_rules(path=hierarchical_json_file, format="json", options={})
    assert df is not None
    assert df.count() == 2


def test_flat_json(json_loader, flat_json_file):
    df = json_loader.load_rules(path=flat_json_file, format="json", options={})
    assert df is not None
    assert df.count() == 1


def test_file_not_found_raises(json_loader):
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="not found"):
        json_loader.load_rules(path="/nonexistent/rules.json", format="json", options={})


def test_invalid_json_raises(json_loader, tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("{invalid json}")
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="Error parsing"):
        json_loader.load_rules(path=str(bad), format="json", options={})


def test_empty_json_raises(json_loader, tmp_path):
    empty = tmp_path / "empty.json"
    empty.write_text("null")
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="empty"):
        json_loader.load_rules(path=str(empty), format="json", options={})


def test_sample_rules_json_loads(json_loader):
    """Ensure the shipped sample_rules.json example loads correctly."""
    df = json_loader.load_rules(path=SAMPLE_RULES_JSON, format="json", options={})
    assert df is not None
    assert df.count() == 19
    assert set(df.columns) == set(RULES_SCHEMA_COLUMNS)


def test_sample_rules_json_values(json_loader):
    """Verify key fields from the sample JSON are parsed correctly."""
    df = json_loader.load_rules(path=SAMPLE_RULES_JSON, format="json", options={})
    rows = {r["rule"]: r.asDict() for r in df.collect()}
    assert rows["customer_id_not_null"]["action_if_failed"] == "drop"
    assert rows["customer_id_not_null"]["rule_type"] == "row_dq"
    assert rows["customer_id_not_null"]["priority"] == "high"
    assert rows["order_date_not_null"]["action_if_failed"] == "ignore"
    assert rows["table_row_count"]["rule_type"] == "agg_dq"
    assert rows["table_row_count"]["action_if_failed"] == "fail"


def test_validations_format_json(json_loader, tmp_path):
    """Test the validations wrapper format via JSON."""
    data = {
        "defaults": {"action_if_failed": "ignore"},
        "validations": [
            {
                "product_id": "p1",
                "table_name": "t1",
                "rules": [
                    {"rule": "r1", "rule_type": "row_dq", "expectation": "x > 0"},
                ],
            },
            {
                "product_id": "p2",
                "table_name": "t2",
                "rules": [
                    {"rule": "r2", "rule_type": "agg_dq", "expectation": "count(*) > 0",
                     "action_if_failed": "fail"},
                ],
            },
        ],
    }
    path = tmp_path / "validations.json"
    path.write_text(json.dumps(data))
    df = json_loader.load_rules(path=str(path), format="json", options={})
    assert df is not None
    assert df.count() == 2
    rows = {r["rule"]: r.asDict() for r in df.collect()}
    assert rows["r1"]["product_id"] == "p1"
    assert rows["r1"]["action_if_failed"] == "ignore"
    assert rows["r2"]["product_id"] == "p2"
    assert rows["r2"]["action_if_failed"] == "fail"


def test_no_active_spark_session_raises(json_loader, rules_list_json_file):
    """Verify error when no SparkSession is active."""
    with patch("spark_expectations.rules.plugins.json_loader.SparkSession") as mock_spark:
        mock_spark.getActiveSession.return_value = None
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="No active SparkSession"):
            json_loader.load_rules(path=rules_list_json_file, format="json", options={})
