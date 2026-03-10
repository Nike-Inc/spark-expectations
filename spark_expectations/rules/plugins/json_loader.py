"""Pluggy plugin that loads DQ rules from a JSON file."""

import json
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import BooleanType, DataType, IntegerType, StringType, StructField, StructType

from spark_expectations.core.exceptions import SparkExpectationsUserInputOrConfigInvalidException
from spark_expectations.rules.plugins._flatten import (
    BOOLEAN_COLUMNS,
    INT_COLUMNS,
    RULES_SCHEMA_COLUMNS,
    flatten_rules_list,
)
from spark_expectations.rules.plugins.base_rule_loader import spark_expectations_rule_loader_impl

JSON_EXTENSIONS = {".json"}


def _col_type(col: str) -> DataType:
    if col in BOOLEAN_COLUMNS:
        return BooleanType()
    if col in INT_COLUMNS:
        return IntegerType()
    return StringType()


def _rules_schema() -> StructType:
    return StructType([StructField(col, _col_type(col), True) for col in RULES_SCHEMA_COLUMNS])


def _read_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError as exc:
        raise SparkExpectationsUserInputOrConfigInvalidException(
            f"Rules JSON file not found: {path}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise SparkExpectationsUserInputOrConfigInvalidException(
            f"Error parsing JSON rules file '{path}': {exc}"
        ) from exc

    if data is None:
        raise SparkExpectationsUserInputOrConfigInvalidException(
            f"Rules JSON file is empty or null: {path}"
        )
    return data


def _rows_to_dataframe(rows: List[Dict[str, str]], spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(rows, schema=_rules_schema())


class SparkExpectationsJsonRuleLoaderImpl:
    """Loads DQ rules from a JSON file (hierarchical or flat format)."""

    @spark_expectations_rule_loader_impl
    def load_rules(  # pylint: disable=unused-argument
        self,
        path: str,
        format: str,
        options: Dict[str, str],
        spark: Optional[SparkSession] = None,
    ) -> Optional[DataFrame]:
        import os

        if format == "json":
            pass
        elif format == "auto" and os.path.splitext(path)[1].lower() in JSON_EXTENSIONS:
            pass
        else:
            return None

        if spark is None:
            spark = SparkSession.getActiveSession()
        if spark is None:
            raise SparkExpectationsUserInputOrConfigInvalidException(
                "No active SparkSession found. Please create a SparkSession before loading rules."
            )

        data = _read_json(path)
        env = (options or {}).get("dq_env")
        rows = flatten_rules_list(data, env=env)
        return _rows_to_dataframe(rows, spark)
