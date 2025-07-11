import re
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
        expectation = rule.get("expectation").lower()
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
        expectation = rule.get("expectation").lower()
        allowed_funcs = ["sum", "avg", "min", "max", "stddev", "count"]

        if not any(func in expectation for func in allowed_funcs):
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
            SparkExpectationsInvalidQueryDQExpectationException: If no table is found or SQL execution fails.
        """
        query = rule.get("expectation").lower()
        table_names = SparkExpectationsValidateRules.extract_table_names_from_sql(query)

        if not table_names:
            raise SparkExpectationsInvalidQueryDQExpectationException(f"No table/view name found in query: {query}")
        for table_name in table_names:
            df.createOrReplaceTempView(table_name)
        try:
            spark.sql(f"SELECT {query} AS result").schema
        except Exception as e:
            raise SparkExpectationsInvalidQueryDQExpectationException(
                f"[query_dq] Rule failed SQL validation | rule_type: query_dq | rule: '{rule.get('rule')}' | "
                f"expectation: '{query}' → {e}"
            )

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
