import re
import uuid
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr

from spark_expectations.core.exceptions import (
    SparkExpectationsInvalidAggDQExpectationException,
    SparkExpectationsInvalidQueryDQExpectationException,
    SparkExpectationsInvalidRowDQExpectationException,
    SparkExpectationsInvalidRuleTypeException,
)

import sqlglot
from sqlglot.errors import ParseError

from enum import Enum


class RuleType(Enum):
    ROW_DQ = "row_dq"
    AGG_DQ = "agg_dq"
    QUERY_DQ = "query_dq"


class SparkExpectationsValidateRules:
    """
    Performs validations for data quality rules like row_dq, agg_dq and query_dq.
    """

    @staticmethod
    def extract_table_names_from_sql(sql: str) -> List[str]:
        """
        Extracts table/view names from FROM and JOIN clauses
        in a SQL string using regex matching.

        Args:
            sql (str): SQL string.

        Returns:
            List[str]: Unique table/view names referenced in the query.
        """
        matches = re.findall(
            r"\bFROM\s+([a-zA-Z_][\w\.]*)|\bJOIN\s+([a-zA-Z_][\w\.]*)",
            sql,
            flags=re.IGNORECASE,
        )
        return list({name for pair in matches for name in pair if name})

    @staticmethod
    def validate_row_dq_expectation(df: DataFrame, rule: Dict) -> None:
        """
        Validates a row_dq expectation by ensuring
        1. It is a valid expression.
        2. The expectation runs successfully on a dataframe.
        3. It does NOT use aggregate functions.

        Args:
            df (DataFrame): The input DataFrame.
            rule (Dict): Rule containing the 'expectation' and 'rule'.

        Raises:
            SparkExpectationsInvalidRowDQExpectationException: If aggregate functions are used or expression fails.
        """
        expectation = rule.get("expectation", "")

        # Use sqlglot to detect aggregate functions
        try:
            tree = sqlglot.parse_one(expectation)
            agg_funcs = list({node.key for node in tree.find_all(sqlglot.expressions.AggFunc)})
        except Exception as e:
            raise SparkExpectationsInvalidRowDQExpectationException(
                f"[row_dq] Could not parse expression: {expectation} → {e}"
            )

        if agg_funcs:
            raise SparkExpectationsInvalidRowDQExpectationException(
                f"[row_dq] Rule '{rule.get('rule')}' contains aggregate function(s) (not allowed in row_dq): {agg_funcs}"
            )

        try:
            df.select(expr(expectation)).limit(1)
        except Exception as e:
            raise SparkExpectationsInvalidRowDQExpectationException(
                f"[row_dq] Rule failed validation | rule_type: row_dq | "
                f"rule: '{rule.get('rule')}' | expectation: '{expectation}' → {e}"
            )

    @staticmethod
    def validate_agg_dq_expectation(df: DataFrame, rule: Dict) -> None:
        """
        Validates an agg_dq expectation by ensuring it includes aggregate functions.

        Args:
            df (DataFrame): The input DataFrame.
            rule (Dict): Rule containing the 'expectation' and 'rule'.

        Raises:
            SparkExpectationsInvalidAggDQExpectationException: If no aggregate function is found or expression fails.
        """
        expectation = rule.get("expectation", "")

        # Use sqlglot to detect aggregate functions
        try:
            tree = sqlglot.parse_one(expectation)
            agg_funcs = list({node.key for node in tree.find_all(sqlglot.expressions.AggFunc)})
        except Exception as e:
            raise SparkExpectationsInvalidAggDQExpectationException(
                f"[agg_dq] Could not parse expression: {expectation} → {e}"
            )

        if not agg_funcs:
            raise SparkExpectationsInvalidAggDQExpectationException(
                f"[agg_dq] Rule '{rule.get('rule')}' does not contain a valid aggregate function: {expectation}"
            )

        try:
            df.selectExpr(expectation).limit(1)
        except Exception as e:
            raise SparkExpectationsInvalidAggDQExpectationException(
                f"[agg_dq] Rule failed validation | rule_type: agg_dq | rule: '{rule.get('rule')}' | "
                f"expectation: '{expectation}' → {e}"
            )

    @staticmethod
    def validate_query_dq_expectation(df: DataFrame, rule: Dict, spark: SparkSession) -> None:
        """
        Validates a query_dq expectation by ensuring it is a valid SQL query.
        Args:
            df (DataFrame): The input DataFrame.
            rule (Dict): Rule containing the 'expectation' SQL.
            spark (SparkSession): Spark session.
        Raises:
            SparkExpectationsInvalidQueryDQExpectationException: If the query is not valid SQL or fails to parse.
        """
        query = rule.get("expectation", "")

        # 1. Ensure it's a SELECT ... FROM ... query
        if not re.search(r"\bselect\b.*\bfrom\b", query, re.IGNORECASE):
            raise SparkExpectationsInvalidQueryDQExpectationException(
                f"[query_dq] Expectation does not appear to be a valid SQL SELECT query: '{query}'"
            )

        # 2. Use extract_table_names_from_sql to find all table names/placeholders
        table_names = SparkExpectationsValidateRules.extract_table_names_from_sql(query)
        temp_view_name = "dummy_table"
        query_for_validation = query

        # 3. Replace each table name/placeholder with the dummy table name
        for table_name in table_names:
            query_for_validation = re.sub(rf"\b{re.escape(table_name)}\b", temp_view_name, query_for_validation)

        # 4. Handle {table} and similar placeholders
        query_for_validation = re.sub(r"\{[^\}]+\}", temp_view_name, query_for_validation)

        # 5. Validate SQL syntax using sqlglot
        try:
            sqlglot.parse_one(query_for_validation)
        except ParseError as e:
            raise SparkExpectationsInvalidQueryDQExpectationException(f"[query_dq] Invalid SQL syntax (sqlglot): {e}")

    @staticmethod
    def validate_expectations(df: DataFrame, rules: list, spark: SparkSession) -> dict:
        """
        Validates a list of rules and returns a map of failed rules by rule type.

        Args:
            df (DataFrame): Input DataFrame to validate against.
            rules (list): List of rule dictionaries.
            spark (SparkSession): Spark session.

        Returns:
            dict: {RuleType: [failed_rule_dicts]}
        """
        failed: Dict[RuleType, List[Dict]] = {rt: [] for rt in RuleType}
        for rule in rules:
            try:
                rule_type = RuleType(rule.get("rule_type"))
                if rule_type == RuleType.ROW_DQ:
                    SparkExpectationsValidateRules.validate_row_dq_expectation(df, rule)
                elif rule_type == RuleType.AGG_DQ:
                    SparkExpectationsValidateRules.validate_agg_dq_expectation(df, rule)
                elif rule_type == RuleType.QUERY_DQ:
                    SparkExpectationsValidateRules.validate_query_dq_expectation(df, rule, spark)
            except Exception:
                failed[rule_type].append(rule)
        # Remove empty lists for rule types with no failures
        return {k: v for k, v in failed.items() if v}
