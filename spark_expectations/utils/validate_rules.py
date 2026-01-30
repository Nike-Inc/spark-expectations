import re
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional
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

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of a rule validation."""
    is_valid: bool
    error_message: Optional[str] = None
    rule: Optional[Dict] = None
    rule_type: Optional[str] = None


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
    def _validate_single_subquery(sq: sqlglot.expressions.Subquery) -> None:

        """
        Validates a single sqlglot Subquery node for structural correctness.

        Ensures the subquery:
        - wraps a SELECT statement,
        - contains a FROM clause,
        - the FROM source is one of Table/Subquery/Join,
        - has at least one projection, and each projection is a valid expression
        (Column, AggFunc, Window, or an Alias chain resolving to these).

        Args:
        sq (sqlglot.expressions.Subquery): The subquery node to validate.

        Raises:
        SparkExpectationsInvalidRowDQExpectationException: If any of the checks fail.
        """
        inner = sq.this if sq.this else sq
        if not isinstance(inner, sqlglot.expressions.Select):
            raise SparkExpectationsInvalidRowDQExpectationException(
                    "[row_dq] Subquery does not contain SELECT statement"
                )

        from_node = inner.args.get("from")
        if not isinstance(from_node, sqlglot.expressions.From):
            raise SparkExpectationsInvalidRowDQExpectationException(
                    "[row_dq] Subquery does not contain FROM"
                )
        
        source = from_node.this
        if not isinstance(source, (sqlglot.expressions.Table, sqlglot.expressions.Subquery, sqlglot.expressions.Join)):
            raise SparkExpectationsInvalidRowDQExpectationException(
                    "[row_dq] Subquery does not contain a valid source after FROM"
                )
        
        projections = inner.args.get("expressions") or []
        if not projections:
            raise SparkExpectationsInvalidRowDQExpectationException(
                "[row_dq] Subquery does not contain any valid projections"
            )
        
    
    
    @staticmethod
    def validate_subqueries(tree: sqlglot.Expression) -> None:
        """
        Validates all sqlglot Subquery nodes inside a parsed expression.

        It finds every Subquery in the AST and delegates validation to _validate_single_subquery, ensuring each subquery:
        
        - wraps a SELECT statement,
        - has a valid FROM source (Table/Subquery/Join),
        - and contains at least one valid projection.

        Args:
        tree (sqlglot.Expression): Parsed SQL expression to inspect.

        Raises:
            SparkExpectationsInvalidRowDQExpectationException: If any subquery is
            missing SELECT/FROM, has an invalid source, or invalid projections.
        """

        subqueries = SparkExpectationsValidateRules.get_subqueries(tree)
        
        for subquery in subqueries:
            try:
                SparkExpectationsValidateRules._validate_single_subquery(subquery)
            except Exception as e:
                raise SparkExpectationsInvalidRowDQExpectationException(
                    f"[row_dq] Could not validate subquery: {subquery}: {e}"
                )
    
    @staticmethod
    def get_subqueries(tree: sqlglot.Expression) -> list:
        """
        Extracts all subqueries and query expressions from a parsed SQL expression tree.
        
        This captures Subquery nodes as well as Query types (Select, Union, etc.),
        including SELECT queries embedded in IN expressions. Duplicates are removed
        (e.g., a Select inside a Subquery is not counted twice).
        
        Args:
            tree (sqlglot.Expression): Parsed SQL expression tree.
            
        Returns:
            list: List of unique subquery/query expressions.
        """
        subqueries = []
        for expression in tree.find_all(sqlglot.expressions.Subquery, sqlglot.expressions.Query):
            # Skip Query nodes that are direct children of a Subquery (already captured)
            if isinstance(expression, sqlglot.expressions.Query) and isinstance(expression.parent, sqlglot.expressions.Subquery):
                continue
            subqueries.append(expression)
        return subqueries
        

    @staticmethod        
    def check_query_dq(tree: sqlglot.Expression) -> bool:
        """
        Determines whether a parsed SQL expression represents a SELECT query.
    
        It unwraps nested sqlglot Subquery nodes (via the `this` attribute) and
        returns True if the underlying node is a sqlglot.expressions.Select; otherwise False.
    
        Args:
        tree (sqlglot.Expression): Parsed SQL expression (e.g., from sqlglot.parse_one).
    
        Returns:
        bool: True if the expression resolves to a SELECT, False otherwise.
        """
        if isinstance(tree, sqlglot.expressions.Select):
            return True
        inner = tree.this
        if isinstance(inner, (sqlglot.expressions.Subquery, sqlglot.expressions.Select)):
            return SparkExpectationsValidateRules.check_query_dq(inner)
        return False

    @staticmethod
    def check_agg_outside_subqueries(tree: sqlglot.Expression, agg_funcs: list) -> bool:
        """
        Verifies that all aggregate functions in the expression are contained within subqueries.
        
        This method compares the aggregate functions found in the entire expression tree
        against those found within subqueries. If there are aggregate functions outside
        of subqueries, it raises an exception.
        
        Args:
            tree (sqlglot.Expression): Parsed SQL expression tree.
            agg_funcs (list): List of aggregate function keys found in the entire tree.
            
        Raises:
            SparkExpectationsInvalidRowDQExpectationException: If aggregate functions 
                exist outside of subqueries.
        """
        subqueries = SparkExpectationsValidateRules.get_subqueries(tree)
        subquery_agg = []

        for query in subqueries:
            query_agg = list({node.key for node in query.find_all(sqlglot.expressions.AggFunc)})
            subquery_agg.extend(query_agg)

        return len(agg_funcs) != len(subquery_agg)

    
    @staticmethod
    # pylint: disable=too-many-return-statements
    def validate_row_dq_expectation(
        df: DataFrame, rule: Dict, raise_exception: bool = False
    ) -> ValidationResult:
        """
        Validates a row_dq expectation by ensuring
        1. It is a valid expression.
        2. The expectation runs successfully on a dataframe.
        3. It does NOT use aggregate functions.
        Args:
            df (DataFrame): The input DataFrame.
            rule (Dict): Rule containing the 'expectation' and 'rule'.
            raise_exception (bool): If True, raises exception on invalid rule. 
                                    If False, returns ValidationResult. Default is False.
        Returns:
            ValidationResult: Result containing validation status and error message if any.
        Raises:
            SparkExpectationsInvalidRowDQExpectationException: If raise_exception is True and 
                aggregate functions are used or expression fails.
        """
        expectation = rule.get("expectation", "")
        try:
            tree = sqlglot.parse_one(expectation)
            
            check_query_dq_result = SparkExpectationsValidateRules.check_query_dq(tree)
            agg_funcs = list({node.key for node in tree.find_all(sqlglot.expressions.AggFunc)})
            check_subqueries = bool(SparkExpectationsValidateRules.get_subqueries(tree))
        except Exception as e:
            error_msg = f"[row_dq] Could not parse expression: {expectation} → {e}"
            if raise_exception:
                raise SparkExpectationsInvalidRowDQExpectationException(error_msg)
            return ValidationResult(
                is_valid=False, error_message=error_msg, rule=rule, rule_type="row_dq"
            )
        
        if check_subqueries:
            if agg_funcs:
                agg_subquery_check = SparkExpectationsValidateRules.check_agg_outside_subqueries(tree, agg_funcs)
                if agg_subquery_check:
                    error_msg = "[row_dq] Expectation contains aggregate function(s) outside of subquery/subqueries"
                    if raise_exception:
                        raise SparkExpectationsInvalidRowDQExpectationException(error_msg)
                    return ValidationResult(
                        is_valid=False, error_message=error_msg, rule=rule, rule_type="row_dq"
                    )
            try:
                SparkExpectationsValidateRules.validate_subqueries(tree)
            except SparkExpectationsInvalidRowDQExpectationException as e:
                if raise_exception:
                    raise
                return ValidationResult(
                    is_valid=False, error_message=str(e), rule=rule, rule_type="row_dq"
                )
        
        if agg_funcs and not check_subqueries:
            error_msg = f"[row_dq] Rule '{rule.get('rule')}' contains aggregate function(s) outside of subquery/subqueries: {agg_funcs}"
            if raise_exception:
                raise SparkExpectationsInvalidRowDQExpectationException(error_msg)
            return ValidationResult(
                is_valid=False, error_message=error_msg, rule=rule, rule_type="row_dq"
            )
        
        if check_query_dq_result:
            error_msg = f"[row_dq] Rule '{rule.get('rule')}' contains a query as an expectation. Invalid."
            if raise_exception:
                raise SparkExpectationsInvalidRowDQExpectationException(error_msg)
            return ValidationResult(
                is_valid=False, error_message=error_msg, rule=rule, rule_type="row_dq"
            )
        
        try:    
            df.select(expr(expectation)).limit(1)
        except Exception as e:
            error_msg = (
                f"[row_dq] Rule failed validation | rule_type: row_dq | "
                f"rule: '{rule.get('rule')}' | expectation: '{expectation}' → {e}"
            )
            if raise_exception:
                raise SparkExpectationsInvalidRowDQExpectationException(error_msg)
            return ValidationResult(
                is_valid=False, error_message=error_msg, rule=rule, rule_type="row_dq"
            )
        
        return ValidationResult(is_valid=True, rule=rule, rule_type="row_dq")
        
    @staticmethod
    def validate_agg_dq_expectation(
        df: DataFrame, rule: Dict, raise_exception: bool = False
    ) -> ValidationResult:
        """
        Validates an agg_dq expectation by ensuring it includes aggregate functions.
        Args:
            df (DataFrame): The input DataFrame.
            rule (Dict): Rule containing the 'expectation' and 'rule'.
            raise_exception (bool): If True, raises exception on invalid rule. 
                                    If False, returns ValidationResult. Default is False.
        Returns:
            ValidationResult: Result containing validation status and error message if any.
        Raises:
            SparkExpectationsInvalidAggDQExpectationException: If raise_exception is True and 
                no aggregate function is found or expression fails.
        """
        expectation = rule.get("expectation", "")
        # Use sqlglot to detect aggregate functions
        try:
            tree = sqlglot.parse_one(expectation)
            agg_funcs = list({node.key for node in tree.find_all(sqlglot.expressions.AggFunc)})
        except Exception as e:
            error_msg = f"[agg_dq] Could not parse expression: {expectation} → {e}"
            if raise_exception:
                raise SparkExpectationsInvalidAggDQExpectationException(error_msg)
            return ValidationResult(
                is_valid=False, error_message=error_msg, rule=rule, rule_type="agg_dq"
            )
        if not agg_funcs:
            error_msg = f"[agg_dq] Rule '{rule.get('rule')}' does not contain a valid aggregate function: {expectation}"
            if raise_exception:
                raise SparkExpectationsInvalidAggDQExpectationException(error_msg)
            return ValidationResult(
                is_valid=False, error_message=error_msg, rule=rule, rule_type="agg_dq"
            )
        try:
            df.selectExpr(expectation).limit(1)
        except Exception as e:
            error_msg = (
                f"[agg_dq] Rule failed validation | rule_type: agg_dq | rule: '{rule.get('rule')}' | "
                f"expectation: '{expectation}' → {e}"
            )
            if raise_exception:
                raise SparkExpectationsInvalidAggDQExpectationException(error_msg)
            return ValidationResult(
                is_valid=False, error_message=error_msg, rule=rule, rule_type="agg_dq"
            )
        
        return ValidationResult(is_valid=True, rule=rule, rule_type="agg_dq")
    @staticmethod
    def validate_query_dq_expectation(
        _df: DataFrame, rule: Dict, _spark: SparkSession, raise_exception: bool = False
    ) -> ValidationResult:
        """ 
        Validates a query_dq expectation by ensuring it is a valid SQL query.
        Args:
            _df (DataFrame): The input DataFrame.
            rule (Dict): Rule containing the 'expectation' SQL.
            _spark (SparkSession): Spark session.
            raise_exception (bool): If True, raises exception on invalid rule. 
                                    If False, returns ValidationResult. Default is False.
        Returns:
            ValidationResult: Result containing validation status and error message if any.
        Raises:
            SparkExpectationsInvalidQueryDQExpectationException: If raise_exception is True and 
                the query is not valid SQL or fails to parse.
        
        It validates 2 types of queries:
            1. Simple query like single "SELECT ... FROM ..."
            2. Composite query with place holder(s) {}, delimiter(s), and subqueries like "SELECT ... FROM  {key1}...{key2}@key1@<subquery1@key2@<subquery2..."
            
            Example query: ((select count(*) from ({source_f1}) a) - (select count(*) from ({target_f1}) b)) < 3@source_f1@select ...@target_f1@select ...
            
            First part of the composite query is called static until the first delimiter with place holders {}. ((select count(*) from ({source_f1}) a) - (select count(*) from ({target_f1}) b)) < 3. 
            Second part, the composite query, is called dynamic with delimiter(s) followed with subquerie(s):  @source_f1@select ...@target_f1@select ...
            These subqueries will be placed in the static query for the matching placeholder(s).
        """
        raw = rule.get("expectation", "")
        delimiter = rule.get("query_dq_delimiter") or "@"
        # Split on delimiter to detect composite pattern
        delimited_query_list = raw.split(delimiter)
        dynamic_query_list = delimited_query_list[1:] if len(delimited_query_list) > 1 else []
        # List should be even length as each key should have a corresponding subquery
        is_composite_query = len(dynamic_query_list) >= 2 and len(dynamic_query_list) % 2 == 0
        if is_composite_query:
            # Split the dynamic sql into key-value pairs
            dynamic_query_kv_pairs = {dynamic_query_list[i].strip(): dynamic_query_list[i + 1].strip() for i in range(0, len(dynamic_query_list), 2)}
            # Validate each subquery and Ensure it's a SELECT ... FROM ... query
            for key, subquery in dynamic_query_kv_pairs.items():
                if not re.search(r"\bselect\b.*\bfrom\b", subquery, re.IGNORECASE):
                    error_msg = f"[query_dq] Subquery '{subquery}' for key '{key}' is not a valid SELECT ... FROM:"
                    if raise_exception:
                        raise SparkExpectationsInvalidQueryDQExpectationException(error_msg)
                    return ValidationResult(
                        is_valid=False, error_message=error_msg, rule=rule, rule_type="query_dq"
                    )
            # Insert subqueries into first part(static query) of the composite query for the matching placeholder(s). 
            try:
                static_query = delimited_query_list[0].strip().format(
                    **{k: (v if v.lstrip().startswith("(") else f"({v})") for k, v in dynamic_query_kv_pairs.items()}
                )
            except KeyError as e:
                error_msg = f"[query_dq] Missing key referenced in composite static query: {e}"
                if raise_exception:
                    raise SparkExpectationsInvalidQueryDQExpectationException(error_msg)
                return ValidationResult(
                    is_valid=False, error_message=error_msg, rule=rule, rule_type="query_dq"
                )
            except Exception as e:
                error_msg = f"[query_dq] Error formatting composite expectation: {e}"
                if raise_exception:
                    raise SparkExpectationsInvalidQueryDQExpectationException(error_msg)
                return ValidationResult(
                    is_valid=False, error_message=error_msg, rule=rule, rule_type="query_dq"
                )
            # Replace any remaining {placeholders} (should not be any) to avoid parse errors
            query = re.sub(r"\{[^}]+\}", "DUMMY_PLACEHOLDER", static_query) 
        
        else:
            # --- Simple Query path with no place holders {} and delimiter(s) ---
            query = raw
            if not re.search(r"\bselect\b.*\bfrom\b", query, re.IGNORECASE):
                error_msg = f"[query_dq] Expectation does not appear to be a valid SQL SELECT query: '{query}'"
                if raise_exception:
                    raise SparkExpectationsInvalidQueryDQExpectationException(error_msg)
                return ValidationResult(
                    is_valid=False, error_message=error_msg, rule=rule, rule_type="query_dq"
                )
        
        # Use extract_table_names_from_sql to find all table names/placeholders
        table_names = SparkExpectationsValidateRules.extract_table_names_from_sql(query)
        temp_view_name = "dummy_table"
        query_for_validation = query
        
        # Replace each table name/placeholder with the dummy table name
        for table_name in table_names:
            query_for_validation = re.sub(rf"\b{re.escape(table_name)}\b", temp_view_name, query_for_validation)
        
        # Handle {table} and similar placeholders
        query_for_validation = re.sub(r"\{[^}]+\}", temp_view_name, query_for_validation)
        
        # Validate SQL syntax using sqlglot
        try:
            sqlglot.parse_one(query_for_validation)
        except ParseError as e:
            error_msg = f"[query_dq] Invalid SQL syntax (sqlglot): {e}"
            if raise_exception:
                raise SparkExpectationsInvalidQueryDQExpectationException(error_msg)
            return ValidationResult(
                is_valid=False, error_message=error_msg, rule=rule, rule_type="query_dq"
            )
        
        return ValidationResult(is_valid=True, rule=rule, rule_type="query_dq")
    @staticmethod
    def validate_expectations(
        df: DataFrame, rules: list, spark: SparkSession, raise_exception: bool = False
    ) -> Dict[str, List[ValidationResult]]:
        """
        Validates a list of rules and returns a map of validation results by rule type.
        
        Invalid rules are logged as warnings but do not raise exceptions by default.
        
        Args:
            df (DataFrame): Input DataFrame to validate against.
            rules (list): List of rule dictionaries.
            spark (SparkSession): Spark session.
            raise_exception (bool): If True, raises exception on first invalid rule.
                                    If False (default), logs warnings and continues.
            
        Returns:
            Dict[str, List[ValidationResult]]: A dictionary mapping rule type strings
                to lists of ValidationResult objects for invalid rules only. Empty 
                dict if all rules are valid.
        """
        invalid_results: Dict[str, List[ValidationResult]] = {}
        valid_count = 0
        
        for rule in rules:
            rule_type_str = rule.get("rule_type", "unknown")
            try:
                rule_type = RuleType(rule_type_str)
            except ValueError:
                result = ValidationResult(
                    is_valid=False,
                    error_message=f"Unknown rule_type: '{rule_type_str}'",
                    rule=rule,
                    rule_type=rule_type_str
                )
                # pylint: disable=logging-too-many-args
                logger.warning(
                    "Invalid rule detected: rule='%s', error='%s'",
                    rule.get("rule", "unknown"),
                    result.error_message
                )
                invalid_results.setdefault(rule_type_str, []).append(result)
                continue
            
            if rule_type == RuleType.ROW_DQ:
                result = SparkExpectationsValidateRules.validate_row_dq_expectation(
                    df, rule, raise_exception=raise_exception
                )
            elif rule_type == RuleType.AGG_DQ:
                result = SparkExpectationsValidateRules.validate_agg_dq_expectation(
                    df, rule, raise_exception=raise_exception
                )
            elif rule_type == RuleType.QUERY_DQ:
                result = SparkExpectationsValidateRules.validate_query_dq_expectation(
                    df, rule, spark, raise_exception=raise_exception
                )
            
            if not result.is_valid:
                # pylint: disable=logging-too-many-args
                logger.warning(
                    "Invalid rule detected: rule='%s', rule_type='%s', error='%s'",
                    rule.get("rule", "unknown"),
                    rule_type_str,
                    result.error_message
                )
                invalid_results.setdefault(rule_type_str, []).append(result)
            else:
                valid_count += 1
        
        # Log summary
        total_invalid = sum(len(v) for v in invalid_results.values())
        # pylint: disable=logging-too-many-args
        if total_invalid > 0:
            logger.warning(
                "Rule validation complete: %d valid, %d invalid out of %d total rules",
                valid_count,
                total_invalid,
                len(rules)
            )
            for rule_type_str, results in invalid_results.items():
                logger.warning(
                    "  - %s: %d invalid rule(s)",
                    rule_type_str,
                    len(results)
                )
        else:
            logger.info(
                "Rule validation complete: all %d rules are valid",
                len(rules)
            )
        
        return invalid_results