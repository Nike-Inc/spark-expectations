"""Tests for spark_expectations.rules (plugin manager and convenience functions)"""

import json
import textwrap

import pytest

from spark_expectations.core import get_spark_session
from spark_expectations.core.exceptions import SparkExpectationsUserInputOrConfigInvalidException
from spark_expectations.rules import (
    get_rule_loader_hook,
    load_rules,
    load_rules_from_json,
    load_rules_from_yaml,
)
from spark_expectations.rules.plugins._flatten import RULES_SCHEMA_COLUMNS
from spark_expectations.rules.plugins.json_loader import SparkExpectationsJsonRuleLoaderImpl
from spark_expectations.rules.plugins.yaml_loader import SparkExpectationsYamlRuleLoaderImpl

get_spark_session()


def test_hook_returns_plugin_manager():
    pm = get_rule_loader_hook()
    assert pm is not None


def test_yaml_plugin_registered():
    pm = get_rule_loader_hook()
    plugin = pm.get_plugin("spark_expectations_yaml_rule_loader")
    assert isinstance(plugin, SparkExpectationsYamlRuleLoaderImpl)


def test_json_plugin_registered():
    pm = get_rule_loader_hook()
    plugin = pm.get_plugin("spark_expectations_json_rule_loader")
    assert isinstance(plugin, SparkExpectationsJsonRuleLoaderImpl)


def test_two_plugins_registered():
    pm = get_rule_loader_hook()
    plugins = pm.list_name_plugin()
    names = [name for name, _ in plugins]
    assert "spark_expectations_yaml_rule_loader" in names
    assert "spark_expectations_json_rule_loader" in names


@pytest.fixture
def yaml_file(tmp_path):
    content = textwrap.dedent("""\
        product_id: test_product
        dq_env:
          DEV:
            table_name: db.t1
            action_if_failed: ignore
            priority: medium
        rules:
          - rule: r1
            rule_type: row_dq
            expectation: "col1 > 0"
            column_name: col1
            tag: validity
    """)
    path = tmp_path / "rules.yaml"
    path.write_text(content)
    return str(path)


@pytest.fixture
def json_file(tmp_path):
    data = {
        "product_id": "test_product",
        "dq_env": {
            "DEV": {
                "table_name": "db.t1",
                "action_if_failed": "ignore",
                "priority": "medium",
            },
        },
        "rules": [
            {
                "rule": "r1",
                "rule_type": "row_dq",
                "expectation": "col1 > 0",
                "column_name": "col1",
                "tag": "validity",
            }
        ],
    }
    path = tmp_path / "rules.json"
    path.write_text(json.dumps(data))
    return str(path)


def test_load_rules_auto_yaml(yaml_file):
    df = load_rules(yaml_file, options={"dq_env": "DEV"})
    assert df.count() == 1
    assert set(df.columns) == set(RULES_SCHEMA_COLUMNS)


def test_load_rules_auto_json(json_file):
    df = load_rules(json_file, options={"dq_env": "DEV"})
    assert df.count() == 1
    assert set(df.columns) == set(RULES_SCHEMA_COLUMNS)


def test_load_rules_explicit_yaml(yaml_file):
    df = load_rules(yaml_file, format="yaml", options={"dq_env": "DEV"})
    assert df.count() == 1


def test_load_rules_explicit_json(json_file):
    df = load_rules(json_file, format="json", options={"dq_env": "DEV"})
    assert df.count() == 1


def test_load_rules_unsupported_format_raises(tmp_path):
    path = tmp_path / "rules.csv"
    path.write_text("a,b,c")
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="No rule-loader"):
        load_rules(str(path))


def test_load_rules_from_yaml(yaml_file):
    df = load_rules_from_yaml(yaml_file, options={"dq_env": "DEV"})
    assert df.count() == 1


def test_load_rules_from_json(json_file):
    df = load_rules_from_json(json_file, options={"dq_env": "DEV"})
    assert df.count() == 1


def test_load_rules_with_explicit_spark_session(yaml_file):
    """Exercise the `spark is not None` branch in load_rules."""
    spark = get_spark_session()
    df = load_rules(yaml_file, spark=spark, options={"dq_env": "DEV"})
    assert df.count() == 1
    assert set(df.columns) == set(RULES_SCHEMA_COLUMNS)
