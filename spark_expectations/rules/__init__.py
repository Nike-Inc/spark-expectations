"""Rule-loader plugin system and convenience functions.

Usage::

    from spark_expectations.rules import load_rules, load_rules_from_yaml, load_rules_from_json

    rules_df = load_rules("path/to/rules.yaml", spark)
    rules_df = load_rules_from_yaml("path/to/rules.yaml", spark)
    rules_df = load_rules_from_json("path/to/rules.json", spark)
"""

import functools
from typing import Dict, Optional

import pluggy

from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession

from spark_expectations import _log
from spark_expectations.core.exceptions import SparkExpectationsUserInputOrConfigInvalidException
from spark_expectations.rules.plugins.base_rule_loader import (
    SPARK_EXPECTATIONS_RULE_LOADER_PLUGIN,
    SparkExpectationsRuleLoader,
)
from spark_expectations.rules.plugins.json_loader import SparkExpectationsJsonRuleLoaderImpl
from spark_expectations.rules.plugins.yaml_loader import SparkExpectationsYamlRuleLoaderImpl


@functools.lru_cache
def get_rule_loader_hook() -> pluggy.PluginManager:
    """Build and cache the rule-loader PluginManager."""
    pm = pluggy.PluginManager(SPARK_EXPECTATIONS_RULE_LOADER_PLUGIN)
    pm.add_hookspecs(SparkExpectationsRuleLoader)

    pm.load_setuptools_entrypoints(SPARK_EXPECTATIONS_RULE_LOADER_PLUGIN)

    pm.register(SparkExpectationsYamlRuleLoaderImpl(), "spark_expectations_yaml_rule_loader")
    pm.register(SparkExpectationsJsonRuleLoaderImpl(), "spark_expectations_json_rule_loader")

    for name, plugin_instance in pm.list_name_plugin():
        _log.info(f"Loaded rule-loader plugin: {name} ({plugin_instance.__class__.__name__})")

    return pm


_rule_loader_hook = get_rule_loader_hook().hook


def load_rules(
    path: str,
    spark: Optional[SparkSession] = None,
    format: str = "auto",
    options: Optional[Dict[str, str]] = None,
) -> DataFrame:
    """Load rules from *path*, auto-detecting the format from the file extension.

    Args:
        path: File path readable by Python (local, DBFS fuse, mounted volume).
        spark: Optional SparkSession; if ``None`` the active session is used.
        format: ``"auto"`` (detect from extension), ``"yaml"``, or ``"json"``.
        options: Extra options forwarded to the loader plugin.

    Returns:
        A Spark DataFrame with the standard rules schema.

    Raises:
        SparkExpectationsUserInputOrConfigInvalidException: When no plugin can
            handle the requested format or the file is invalid.
    """
    if spark is not None:
        SparkSession.builder.getOrCreate()  # ensure active session for plugins

    result = _rule_loader_hook.load_rules(path=path, format=format, options=options or {})

    if result is None:
        raise SparkExpectationsUserInputOrConfigInvalidException(
            f"No rule-loader plugin could handle format='{format}' for path '{path}'. "
            f"Supported formats: yaml, json."
        )

    return result


def load_rules_from_yaml(
    path: str,
    spark: Optional[SparkSession] = None,
    options: Optional[Dict[str, str]] = None,
) -> DataFrame:
    """Load rules from a YAML file.

    Args:
        path: Path to a ``.yaml`` or ``.yml`` file.
        spark: Optional SparkSession; if ``None`` the active session is used.
        options: Extra options forwarded to the loader plugin.

    Returns:
        A Spark DataFrame with the standard rules schema.
    """
    return load_rules(path=path, spark=spark, format="yaml", options=options)


def load_rules_from_json(
    path: str,
    spark: Optional[SparkSession] = None,
    options: Optional[Dict[str, str]] = None,
) -> DataFrame:
    """Load rules from a JSON file.

    Args:
        path: Path to a ``.json`` file.
        spark: Optional SparkSession; if ``None`` the active session is used.
        options: Extra options forwarded to the loader plugin.

    Returns:
        A Spark DataFrame with the standard rules schema.
    """
    return load_rules(path=path, spark=spark, format="json", options=options)
