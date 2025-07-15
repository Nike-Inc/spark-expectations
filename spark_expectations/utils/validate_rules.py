import re
import uuid
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr

from spark_expectations.core.exceptions import (
    SparkExpectationsInvalidAggDQExpectationException,
    SparkExpectationsInvalidQueryDQExpectationException,
    SparkExpectationsInvalidRowDQExpectationException,
    SparkExpectationsInvalidRuleTypeException)


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

        This means the expectation for a row_dq
        1. Cannot include aggregation functions
        2. Cannot be a SQL query

        Args:
            df (DataFrame): The input DataFrame.
            rule (Dict): Rule containing the 'expectation' and 'rule'.

        Raises:
            SparkExpectationsInvalidRowDQExpectationException: If aggregate functions are used or expression fails.
        """
        expectation = rule.get("expectation", "").lower()
        disallowed_funcs = ["sum", "avg", "min", "max", "stddev", "count"]

        if any(func in expectation for func in disallowed_funcs):
            raise SparkExpectationsInvalidRowDQExpectationException(
                f"[row_dq] Rule '{rule.get('rule')}' contains aggregate function (not allowed in row_dq): {expectation}"
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
        expectation = rule.get("expectation", "").lower()
        allowed_funcs = ["sum", "avg", "min", "max", "stddev", "count"]

        # Add this check to catch query-like expectations
        if re.search(r"\bselect\b.*\bfrom\b", expectation, re.IGNORECASE):
            raise SparkExpectationsInvalidAggDQExpectationException(
                f"[agg_dq] Rule '{rule.get('rule')}' contains a SQL query (not allowed in agg_dq): {expectation}"
            )

        if not any(re.search(rf"\\b{func}\\b", expectation) for func in allowed_funcs):
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
        Validates a query_dq expectation by registering temp views and running scalar SQL.

        Args:
            df (DataFrame): The input DataFrame.
            rule (Dict): Rule containing the 'expectation' SQL.
            spark (SparkSession): Spark session.

        Raises:
            SparkExpectationsInvalidQueryDQExpectationException: If the SQL query is invalid or fails to run.
        """
        query = rule.get("expectation", "").lower()
        temp_view_name = f"__dq_validation_temp_{uuid.uuid4().hex}__"
        # Replace {placeholder} table names
        query = re.sub(r"\{[^}]+\}", temp_view_name, query)

        # Extract table names
        table_names = SparkExpectationsValidateRules.extract_table_names_from_sql(query.lower())

        # Replace all extracted table names in the query with the temp view name
        for table_name in table_names:
            query = re.sub(rf"\b{re.escape(table_name)}\b", temp_view_name, query)

        try:
            # Create a temporary view for validation
            df.createOrReplaceTempView(temp_view_name)
            # Validate the query
            spark.sql(f"SELECT {query} AS result").collect()
        except Exception as e:
            raise SparkExpectationsInvalidQueryDQExpectationException(
                f"[query_dq] Rule failed SQL validation | rule_type: query_dq | rule: '{rule.get('rule')}' | "
                f"expectation: '{query}' → {e}"
            )
        finally:
            # Clean up the temporary view
            spark.catalog.dropTempView(temp_view_name)

    @staticmethod
    def validate_expectation(df: DataFrame, rule: Dict, spark: SparkSession) -> None:
        """
        Method to validate expectations based on rule_type.

        Args:
            df (DataFrame): Input DataFrame to validate against.
            rule (Dict): Dictionary representing each rule.
            spark (SparkSession): Spark session.

        Raises:
            SparkExpectationsInvalidRuleTypeException: If rule_type is not supported or validation fails.
        """
        rule_type = rule.get("rule_type")

        if rule_type == "row_dq":
            SparkExpectationsValidateRules.validate_row_dq_expectation(df, rule)
        elif rule_type == "query_dq":
            SparkExpectationsValidateRules.validate_query_dq_expectation(df, rule, spark)
        elif rule_type == "agg_dq":
            SparkExpectationsValidateRules.validate_agg_dq_expectation(df, rule)
        else:
            raise SparkExpectationsInvalidRuleTypeException(f"Unsupported rule_type: {rule_type}")
