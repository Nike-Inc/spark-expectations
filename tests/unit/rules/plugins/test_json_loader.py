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
def dq_env_json_file(tmp_path):
    data = {
        "product_id": "test_product",
        "dq_env": {
            "DEV": {
                "table_name": "db.test_table",
                "action_if_failed": "ignore",
                "is_active": True,
                "priority": "medium",
            },
            "QA": {
                "table_name": "db_qa.test_table",
                "action_if_failed": "ignore",
                "is_active": True,
                "priority": "medium",
            },
            "PROD": {
                "table_name": "db_prod.test_table",
                "action_if_failed": "fail",
                "is_active": True,
                "priority": "high",
            },
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


def test_returns_none_for_non_json_format(json_loader):
    result = json_loader.load_rules(path="rules.yaml", format="yaml", options={})
    assert result is None


def test_returns_none_for_auto_non_json_extension(json_loader):
    result = json_loader.load_rules(path="rules.yaml", format="auto", options={})
    assert result is None


def test_handles_json_format_explicit(json_loader, dq_env_json_file):
    df = json_loader.load_rules(path=dq_env_json_file, format="json", options={"dq_env": "DEV"})
    assert df is not None
    assert df.count() == 3
    assert set(df.columns) == set(RULES_SCHEMA_COLUMNS)


def test_handles_auto_json_extension(json_loader, dq_env_json_file):
    df = json_loader.load_rules(path=dq_env_json_file, format="auto", options={"dq_env": "DEV"})
    assert df is not None
    assert df.count() == 3


def test_dq_env_values(json_loader, dq_env_json_file):
    df = json_loader.load_rules(path=dq_env_json_file, format="json", options={"dq_env": "DEV"})
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


def test_dq_env_selects_prod(json_loader, dq_env_json_file):
    df = json_loader.load_rules(path=dq_env_json_file, format="json", options={"dq_env": "PROD"})
    rows = [r.asDict() for r in df.collect()]
    col2_rule = next(r for r in rows if r["rule"] == "col2_positive")
    assert col2_rule["table_name"] == "db_prod.test_table"
    assert col2_rule["action_if_failed"] == "fail"
    assert col2_rule["priority"] == "high"


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
    df = json_loader.load_rules(path=SAMPLE_RULES_JSON, format="json", options={"dq_env": "DEV"})
    assert df is not None
    assert df.count() == 16
    assert set(df.columns) == set(RULES_SCHEMA_COLUMNS)


def test_sample_rules_json_values(json_loader):
    """Verify key fields from the sample JSON are parsed correctly."""
    df = json_loader.load_rules(path=SAMPLE_RULES_JSON, format="json", options={"dq_env": "DEV"})
    rows = {r["rule"]: r.asDict() for r in df.collect()}
    assert rows["customer_id_not_null"]["action_if_failed"] == "drop"
    assert rows["customer_id_not_null"]["rule_type"] == "row_dq"
    assert rows["customer_id_not_null"]["priority"] == "high"
    assert rows["order_date_not_null"]["action_if_failed"] == "ignore"
    assert rows["table_row_count"]["rule_type"] == "agg_dq"
    assert rows["table_row_count"]["action_if_failed"] == "fail"


def test_sample_rules_json_env_table_name(json_loader):
    """Verify that table_name changes per environment."""
    df_dev = json_loader.load_rules(path=SAMPLE_RULES_JSON, format="json", options={"dq_env": "DEV"})
    df_qa = json_loader.load_rules(path=SAMPLE_RULES_JSON, format="json", options={"dq_env": "QA"})
    rows_dev = {r["rule"]: r.asDict() for r in df_dev.collect()}
    rows_qa = {r["rule"]: r.asDict() for r in df_qa.collect()}
    assert rows_dev["customer_id_not_null"]["table_name"] == "dq_spark_dev.customer_order"
    assert rows_qa["customer_id_not_null"]["table_name"] == "dq_spark_qa.customer_order"


def test_no_active_spark_session_raises(json_loader, dq_env_json_file):
    """Verify error when no SparkSession is active."""
    with patch("spark_expectations.rules.plugins.json_loader.SparkSession") as mock_spark:
        mock_spark.getActiveSession.return_value = None
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="No active SparkSession"):
            json_loader.load_rules(path=dq_env_json_file, format="json", options={"dq_env": "DEV"})


def test_non_dict_json_raises(json_loader, tmp_path):
    """Verify error when JSON top-level is not an object."""
    list_json = tmp_path / "list.json"
    list_json.write_text('[1, 2, 3]')
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="object at the top level"):
        json_loader.load_rules(path=str(list_json), format="json", options={})
