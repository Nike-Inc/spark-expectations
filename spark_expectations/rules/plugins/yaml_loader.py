"""Pluggy plugin that loads DQ rules from a YAML file."""

import os
from typing import Any, Dict, Optional

import yaml

from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession

from spark_expectations.core.exceptions import SparkExpectationsUserInputOrConfigInvalidException
from spark_expectations.rules.plugins._flatten import (
    flatten_rules_list,
    rows_to_dataframe,
)
from spark_expectations.rules.plugins.base_rule_loader import spark_expectations_rule_loader_impl

YAML_EXTENSIONS = {".yaml", ".yml"}


def _read_yaml(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
    except FileNotFoundError as exc:
        raise SparkExpectationsUserInputOrConfigInvalidException(
            f"Rules YAML file not found: {path}"
        ) from exc
    except yaml.YAMLError as exc:
        raise SparkExpectationsUserInputOrConfigInvalidException(
            f"Error parsing YAML rules file '{path}': {exc}"
        ) from exc

    if data is None:
        raise SparkExpectationsUserInputOrConfigInvalidException(
            f"Rules YAML file is empty: {path}"
        )
    if not isinstance(data, dict):
        raise SparkExpectationsUserInputOrConfigInvalidException(
            f"Rules file must contain a mapping at the top level, got {type(data).__name__}: {path}"
        )
    return data


class SparkExpectationsYamlRuleLoaderImpl:
    """Loads DQ rules from a YAML file."""

    @spark_expectations_rule_loader_impl
    def load_rules(  # pylint: disable=unused-argument
        self,
        path: str,
        format: str,
        options: Dict[str, str],
        spark: Optional[SparkSession] = None,
    ) -> Optional[DataFrame]:
        if format == "yaml":
            pass
        elif format == "auto" and os.path.splitext(path)[1].lower() in YAML_EXTENSIONS:
            pass
        else:
            return None

        if spark is None:
            spark = SparkSession.getActiveSession()
        if spark is None:
            raise SparkExpectationsUserInputOrConfigInvalidException(
                "No active SparkSession found. Please create a SparkSession before loading rules."
            )

        data = _read_yaml(path)
        env = (options or {}).get("dq_env")
        rows = flatten_rules_list(data, env=env)
        return rows_to_dataframe(rows, spark)
