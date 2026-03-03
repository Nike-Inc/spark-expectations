"""Tests for spark_expectations.rules.plugins.yaml_loader"""

import os
import textwrap
from unittest.mock import patch

import pytest

from spark_expectations.core import get_spark_session
from spark_expectations.core.exceptions import SparkExpectationsUserInputOrConfigInvalidException
from spark_expectations.rules.plugins._flatten import RULES_SCHEMA_COLUMNS
from spark_expectations.rules.plugins.yaml_loader import SparkExpectationsYamlRuleLoaderImpl

get_spark_session()

SAMPLE_RULES_YAML = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "..", "examples", "resources", "sample_rules.yaml"
)


@pytest.fixture
def yaml_loader():
    return SparkExpectationsYamlRuleLoaderImpl()


@pytest.fixture
def rules_list_yaml_file(tmp_path):
    content = textwrap.dedent("""\
        product_id: test_product
        table_name: db.test_table

        defaults:
          action_if_failed: ignore
          is_active: true
          priority: medium

        rules:
          - rule: col1_not_null
            rule_type: row_dq
            column_name: col1
            expectation: "col1 IS NOT NULL"
            action_if_failed: drop
            tag: completeness
            description: "col1 must not be null"
            priority: high
          - rule: col2_positive
            rule_type: row_dq
            column_name: col2
            expectation: "col2 > 0"
            tag: validity
            description: "col2 must be positive"
          - rule: row_count
            rule_type: agg_dq
            expectation: "count(*) > 0"
            action_if_failed: fail
            tag: completeness
            description: "Must have rows"
    """)
    path = tmp_path / "rules.yaml"
    path.write_text(content)
    return str(path)


@pytest.fixture
def hierarchical_yaml_file(tmp_path):
    content = textwrap.dedent("""\
        product_id: test_product

        defaults:
          action_if_failed: ignore
          priority: medium

        tables:
          db.test_table:
            row_dq:
              - rule: col1_not_null
                column_name: col1
                expectation: "col1 IS NOT NULL"
                action_if_failed: drop
                tag: completeness
                description: "col1 must not be null"
                priority: high
              - rule: col2_positive
                column_name: col2
                expectation: "col2 > 0"
                tag: validity
                description: "col2 must be positive"
            agg_dq:
              - rule: row_count
                column_name: ""
                expectation: "count(*) > 0"
                action_if_failed: fail
                tag: completeness
                description: "Must have rows"
    """)
    path = tmp_path / "rules_hier.yaml"
    path.write_text(content)
    return str(path)


@pytest.fixture
def flat_yaml_file(tmp_path):
    content = textwrap.dedent("""\
        - product_id: test_product
          table_name: db.test_table
          rule_type: row_dq
          rule: col1_not_null
          column_name: col1
          expectation: "col1 IS NOT NULL"
          action_if_failed: drop
          tag: completeness
          description: "col1 must not be null"
    """)
    path = tmp_path / "rules_flat.yaml"
    path.write_text(content)
    return str(path)


def test_returns_none_for_non_yaml_format(yaml_loader):
    result = yaml_loader.load_rules(path="rules.json", format="json", options={})
    assert result is None


def test_returns_none_for_auto_non_yaml_extension(yaml_loader):
    result = yaml_loader.load_rules(path="rules.json", format="auto", options={})
    assert result is None


def test_handles_yaml_format_explicit(yaml_loader, rules_list_yaml_file):
    df = yaml_loader.load_rules(path=rules_list_yaml_file, format="yaml", options={})
    assert df is not None
    assert df.count() == 3
    assert set(df.columns) == set(RULES_SCHEMA_COLUMNS)


def test_handles_auto_yaml_extension(yaml_loader, rules_list_yaml_file):
    df = yaml_loader.load_rules(path=rules_list_yaml_file, format="auto", options={})
    assert df is not None
    assert df.count() == 3


def test_handles_auto_yml_extension(yaml_loader, rules_list_yaml_file, tmp_path):
    yml_path = tmp_path / "rules.yml"
    import shutil
    shutil.copy(rules_list_yaml_file, str(yml_path))
    df = yaml_loader.load_rules(path=str(yml_path), format="auto", options={})
    assert df is not None
    assert df.count() == 3


def test_rules_list_values(yaml_loader, rules_list_yaml_file):
    df = yaml_loader.load_rules(path=rules_list_yaml_file, format="yaml", options={})
    rows = [r.asDict() for r in df.collect()]
    row_dq_rules = [r for r in rows if r["rule_type"] == "row_dq"]
    agg_dq_rules = [r for r in rows if r["rule_type"] == "agg_dq"]

    assert len(row_dq_rules) == 2
    assert len(agg_dq_rules) == 1

    col1_rule = next(r for r in row_dq_rules if r["rule"] == "col1_not_null")
    assert col1_rule["product_id"] == "test_product"
    assert col1_rule["table_name"] == "db.test_table"
    assert col1_rule["action_if_failed"] == "drop"
    assert col1_rule["priority"] == "high"

    col2_rule = next(r for r in row_dq_rules if r["rule"] == "col2_positive")
    assert col2_rule["action_if_failed"] == "ignore"
    assert col2_rule["priority"] == "medium"


def test_hierarchical_yaml(yaml_loader, hierarchical_yaml_file):
    df = yaml_loader.load_rules(path=hierarchical_yaml_file, format="yaml", options={})
    assert df is not None
    assert df.count() == 3


def test_flat_yaml(yaml_loader, flat_yaml_file):
    df = yaml_loader.load_rules(path=flat_yaml_file, format="yaml", options={})
    assert df is not None
    assert df.count() == 1


def test_file_not_found_raises(yaml_loader):
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="not found"):
        yaml_loader.load_rules(path="/nonexistent/rules.yaml", format="yaml", options={})


def test_invalid_yaml_raises(yaml_loader, tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("{{invalid: yaml: [}")
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="Error parsing"):
        yaml_loader.load_rules(path=str(bad), format="yaml", options={})


def test_empty_yaml_raises(yaml_loader, tmp_path):
    empty = tmp_path / "empty.yaml"
    empty.write_text("")
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="empty"):
        yaml_loader.load_rules(path=str(empty), format="yaml", options={})


def test_sample_rules_yaml_loads(yaml_loader):
    """Ensure the shipped sample_rules.yaml example loads correctly."""
    df = yaml_loader.load_rules(path=SAMPLE_RULES_YAML, format="yaml", options={})
    assert df is not None
    assert df.count() == 19
    assert set(df.columns) == set(RULES_SCHEMA_COLUMNS)


def test_sample_rules_yaml_values(yaml_loader):
    """Verify key fields from the sample YAML are parsed correctly."""
    df = yaml_loader.load_rules(path=SAMPLE_RULES_YAML, format="yaml", options={})
    rows = {r["rule"]: r.asDict() for r in df.collect()}
    assert rows["customer_id_not_null"]["action_if_failed"] == "drop"
    assert rows["customer_id_not_null"]["rule_type"] == "row_dq"
    assert rows["customer_id_not_null"]["priority"] == "high"
    assert rows["order_date_not_null"]["action_if_failed"] == "ignore"
    assert rows["order_date_not_null"]["priority"] == "medium"
    assert rows["table_row_count"]["rule_type"] == "agg_dq"
    assert rows["table_row_count"]["action_if_failed"] == "fail"


def test_validations_format_yaml(yaml_loader, tmp_path):
    """Test the validations wrapper format with multiple product entries."""
    content = textwrap.dedent("""\
        defaults:
          action_if_failed: ignore
          priority: medium
        validations:
          - product_id: product_a
            table_name: db.table_a
            rules:
              - rule: r1
                rule_type: row_dq
                expectation: "col1 IS NOT NULL"
          - product_id: product_b
            table_name: db.table_b
            rules:
              - rule: r2
                rule_type: agg_dq
                expectation: "count(*) > 0"
                action_if_failed: fail
    """)
    path = tmp_path / "validations.yaml"
    path.write_text(content)
    df = yaml_loader.load_rules(path=str(path), format="yaml", options={})
    assert df is not None
    assert df.count() == 2
    rows = {r["rule"]: r.asDict() for r in df.collect()}
    assert rows["r1"]["product_id"] == "product_a"
    assert rows["r1"]["action_if_failed"] == "ignore"
    assert rows["r2"]["product_id"] == "product_b"
    assert rows["r2"]["action_if_failed"] == "fail"


def test_no_active_spark_session_raises(yaml_loader, rules_list_yaml_file):
    """Verify error when no SparkSession is active."""
    with patch("spark_expectations.rules.plugins.yaml_loader.SparkSession") as mock_spark:
        mock_spark.getActiveSession.return_value = None
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="No active SparkSession"):
            yaml_loader.load_rules(path=rules_list_yaml_file, format="yaml", options={})
