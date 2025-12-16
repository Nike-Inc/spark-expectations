import re
from typing import Dict, List, Tuple, Optional, Any
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
    def check_agg_dq(expectation: str) -> Tuple[bool, Optional[str]]:
        """
        Returns True if the expression starts with a known aggregate function.
        This is a conservative regex-based check to avoid needing AST positions.
        """
        agg_start_pattern = re.compile(
            r"^\s*(sum|count|avg|mean|min|max|stddev|stddev_samp|stddev_pop|variance|var_samp|var_pop|"
            r"collect_list|collect_set)\s*\(",
            flags=re.IGNORECASE,
        )

        match = agg_start_pattern.match(expectation)
        check = bool(match)
        matched_pattern = match.group(1) if check else None
        return check, matched_pattern
    
    
    @staticmethod
    def _validate_single_subquery(sq: sqlglot.expressions.Subquery) -> None:

        inner = sq.this
        if not isinstance(inner,sqlglot.expressions.Select):
            raise SparkExpectationsInvalidRowDQExpectationException(
                    f"[row_dq] Subquery does not contain SELECT statement"
                )

        from_node = inner.args.get("from")
        if not isinstance(from_node, sqlglot.expressions.From):
            raise SparkExpectationsInvalidRowDQExpectationException(
                    f"[row_dq] Subquery does not contain FROM"
                )
        
        source = from_node.this
        if not isinstance(source, (sqlglot.expressions.Table, sqlglot.expressions.Subquery, sqlglot.expressions.Join)):
            raise SparkExpectationsInvalidRowDQExpectationException(
                    f"[row_dq] Subquery does not contain a valid source after FROM"
                )
        
        projections = inner.args.get("expressions") or []
        if not projections:
            raise SparkExpectationsInvalidRowDQExpectationException(
                f"[row_dq] Subquery does not contain any valid projections"
            )
        
        def _validate_projections(expr: sqlglot.Expression) -> bool:
            
            if isinstance(expr, (sqlglot.expressions.AggFunc, sqlglot.expressions.Window, sqlglot.expressions.Column)):
                return True         
            if isinstance(expr, sqlglot.expressions.Alias):
                return _validate_projections(expr.this)
            return isinstance(expr, sqlglot.expressions.Expression)
        
        proj_result = [_validate_projections(expr) for expr in projections]
        if not all(proj_result):
            raise SparkExpectationsInvalidRowDQExpectationException(
                    f"[row_dq] Subquery does not contain a valid projection"
                )
    
    
    @staticmethod
    def check_subquery(tree:sqlglot.Expression) -> None:

        subqueries = list(tree.find_all(sqlglot.expressions.Subquery))
        if not subqueries:
            return
        
        for subquery in subqueries:
            try:
                SparkExpectationsValidateRules._validate_single_subquery(subquery)
            except Exception as e:
                raise SparkExpectationsInvalidRowDQExpectationException(
                    f"[row_dq] Could not validate subquery: {subquery}: {e}"
                )
    
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
        try:
            tree = sqlglot.parse_one(expectation)
            agg_funcs = list({node.key for node in tree.find_all(sqlglot.expressions.AggFunc)})
            if agg_funcs:
                is_agg, agg_func = SparkExpectationsValidateRules.check_agg_dq(expectation)
        except Exception as e:
            raise SparkExpectationsInvalidRowDQExpectationException(
                f"[row_dq] Could not parse expression: {expectation} → {e}"
            )
        if is_agg:
            raise SparkExpectationsInvalidRowDQExpectationException(
                f"[row_dq] Rule '{rule.get('rule')}' contains aggregate function(s) (not allowed in row_dq): {agg_func}"
            )
        
        try:
            SparkExpectationsValidateRules.check_subquery(tree)
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
        """ 
        Validates a query_dq expectation by ensuring it is a valid SQL query.
        Args:
            df (DataFrame): The input DataFrame.
            rule (Dict): Rule containing the 'expectation' SQL.
            spark (SparkSession): Spark session.
        Raises:
            SparkExpectationsInvalidQueryDQExpectationException: If the query is not valid SQL or fails to parse.
        
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
                    raise SparkExpectationsInvalidQueryDQExpectationException(
                        f"[query_dq] Subquery '{subquery}' for key '{key}' is not a valid SELECT ... FROM:"
                    )
            # Insert subqueries into first part(static query) of the composite query for the matching placeholder(s). 
            try:
                static_query = delimited_query_list[0].strip().format(
                    **{k: (v if v.lstrip().startswith("(") else f"({v})") for k, v in dynamic_query_kv_pairs.items()}
                )
            except KeyError as e:
                raise SparkExpectationsInvalidQueryDQExpectationException(
                    f"[query_dq] Missing key referenced in composite static query: {e}"
                )
            except Exception as e:
                raise SparkExpectationsInvalidQueryDQExpectationException(
                    f"[query_dq] Error formatting composite expectation: {e}"
                )
            # Replace any remaining {placeholders} (should not be any) to avoid parse errors
            query = re.sub(r"\{[^}]+\}", "DUMMY_PLACEHOLDER", static_query) 
        
        else:
            # --- Simple Query path with no place holders {} and delimiter(s) ---
            query = raw
            if not re.search(r"\bselect\b.*\bfrom\b", query, re.IGNORECASE):
                raise SparkExpectationsInvalidQueryDQExpectationException(
                    f"[query_dq] Expectation does not appear to be a valid SQL SELECT query: '{query}'"
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