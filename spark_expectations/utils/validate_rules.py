import re
from typing import Dict, List
from enum import Enum

import sqlglot
from sqlglot.errors import ParseError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr

from spark_expectations.core.exceptions import (
    SparkExpectationsInvalidAggDQExpectationException,
    SparkExpectationsInvalidQueryDQExpectationException,
    SparkExpectationsInvalidRowDQExpectationException,
)


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
    def validate_query_dq_expectation(_df: DataFrame, rule: Dict, _spark: SparkSession) -> None:
        """Validate a query_dq expectation.

        Supports two forms:
          1. Simple single SELECT ... FROM ... query.
          2. Composite (delimited) form:
             <boolean_expr_using_{alias}>@alias1@<subquery1>@alias2@<subquery2>...
             Example:
               ((select count(*) from ({source_f1}) a) - (select count(*) from ({target_f1}) b)) < 3@source_f1@select ...@target_f1@select ...

        The boolean expression (first segment) can reference placeholders in Python format style: {source_f1}, {target_f1}.
        Each alias/subquery pair after the first segment supplies the replacement SQL.
        """
        raw = rule.get("expectation", "")
        delimiter = rule.get("query_dq_delimiter") or "@"

        # Split on delimiter to detect composite pattern
        parts = raw.split(delimiter)
        composite_tail = parts[1:] if len(parts) > 1 else []
        is_composite = len(composite_tail) >= 2 and len(composite_tail) % 2 == 0

        if is_composite:
            boolean_expr = parts[0].strip()
            tail = [p.strip() for p in composite_tail]
            alias_query_pairs = {tail[i]: tail[i + 1] for i in range(0, len(tail), 2)}

            # Validate each subquery: must look like SELECT ... FROM ... and parse with sqlglot
            for alias, subquery in alias_query_pairs.items():
                if not re.search(r"\bselect\b.*\bfrom\b", subquery, re.IGNORECASE):
                    raise SparkExpectationsInvalidQueryDQExpectationException(
                        f"[query_dq] Subquery for alias '{alias}' is not a valid SELECT ... FROM: '{subquery}'"
                    )
                # Basic placeholder neutralization within subquery for parsing
                sanitized_subquery = re.sub(r"\{[^}]+\}", "DUMMY_PLACEHOLDER", subquery)
                try:
                    sqlglot.parse_one(sanitized_subquery)
                except ParseError as e:
                    raise SparkExpectationsInvalidQueryDQExpectationException(
                        f"[query_dq] Invalid SQL syntax in subquery '{alias}': {e}"
                    )

            # Format the boolean expression with injected subqueries (wrapped in parentheses if needed)
            try:
                formatted_boolean_expr = boolean_expr.format(
                    **{a: (q if q.lstrip().startswith("(") else f"({q})") for a, q in alias_query_pairs.items()}
                )
            except KeyError as e:
                raise SparkExpectationsInvalidQueryDQExpectationException(
                    f"[query_dq] Missing alias referenced in boolean expression: {e}"
                )
            except Exception as e:
                raise SparkExpectationsInvalidQueryDQExpectationException(
                    f"[query_dq] Error formatting composite expectation: {e}"
                )

            # Replace any remaining {placeholders} (should not be any) to avoid parse errors
            formatted_boolean_expr_sanitized = re.sub(r"\{[^}]+\}", "DUMMY_PLACEHOLDER", formatted_boolean_expr)

            # For robust parsing, wrap expression into a SELECT statement
            wrapper_query = f"SELECT CASE WHEN {formatted_boolean_expr_sanitized} THEN 1 ELSE 0 END AS dq_check"
            try:
                sqlglot.parse_one(wrapper_query)
            except ParseError as e:
                raise SparkExpectationsInvalidQueryDQExpectationException(
                    f"[query_dq] Invalid composite expectation syntax after substitution: {e}"
                )
            return  # Composite path validated successfully

        # --- Simple / legacy path ---
        query = raw
        if not re.search(r"\bselect\b.*\bfrom\b", query, re.IGNORECASE):
            raise SparkExpectationsInvalidQueryDQExpectationException(
                f"[query_dq] Expectation does not appear to be a valid SQL SELECT query: '{query}'"
            )
        table_names = SparkExpectationsValidateRules.extract_table_names_from_sql(query)
        temp_view_name = "dummy_table"
        query_for_validation = query
        for table_name in table_names:
            query_for_validation = re.sub(rf"\b{re.escape(table_name)}\b", temp_view_name, query_for_validation)
        query_for_validation = re.sub(r"\{[^}]+\}", temp_view_name, query_for_validation)
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
                elif (
                    rule_type == RuleType.QUERY_DQ
                ):  # rule[expectation]= '((select count(*) from (select customer_id, count(*) from customer_source group by customer_id) a join (select customer_id, select customer_id, count(*) from order_source group by customer_id) b on a.customer_id = b.customer_id) - (select count(*) from (select customer_id, select customer_id, count(*) from order_source group by customer_id) a join (select customer_id, count(*) from order_target group by customer_id) b on a.customer_id = b.customer_id)) > (select count(*) from order_source)''
                    SparkExpectationsValidateRules.validate_query_dq_expectation(df, rule, spark)
            except Exception:
                failed[rule_type].append(rule)
        # Remove empty lists for rule types with no failures
        return {k: v for k, v in failed.items() if v}
