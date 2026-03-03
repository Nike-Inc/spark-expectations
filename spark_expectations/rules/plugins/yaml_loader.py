"""Pluggy plugin that loads DQ rules from a YAML file."""

from typing import Dict, List, Optional

import yaml

from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from spark_expectations.core.exceptions import SparkExpectationsUserInputOrConfigInvalidException
from spark_expectations.rules.plugins._flatten import RULES_SCHEMA_COLUMNS, flatten_rules
from spark_expectations.rules.plugins.base_rule_loader import spark_expectations_rule_loader_impl

YAML_EXTENSIONS = {".yaml", ".yml"}


def _rules_schema() -> StructType:
    return StructType([StructField(col, StringType(), True) for col in RULES_SCHEMA_COLUMNS])


def _read_yaml(path: str) -> object:
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
    return data


def _rows_to_dataframe(rows: List[Dict[str, str]], spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(rows, schema=_rules_schema())


class SparkExpectationsYamlRuleLoaderImpl:
    """Loads DQ rules from a YAML file (hierarchical or flat format)."""

    @spark_expectations_rule_loader_impl
    def load_rules(  # pylint: disable=unused-argument
        self,
        path: str,
        format: str,
        options: Optional[Dict[str, str]] = None,
    ) -> Optional[DataFrame]:
        import os

        if format == "yaml":
            pass
        elif format == "auto" and os.path.splitext(path)[1].lower() in YAML_EXTENSIONS:
            pass
        else:
            return None

        spark = SparkSession.getActiveSession()
        if spark is None:
            raise SparkExpectationsUserInputOrConfigInvalidException(
                "No active SparkSession found. Please create a SparkSession before loading rules."
            )

        data = _read_yaml(path)
        rows = flatten_rules(data)
        return _rows_to_dataframe(rows, spark)
