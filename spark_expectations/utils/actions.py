import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    array,
    array_append,
    array_contains,
    col,
    expr,
    lit,
    map_from_entries,
    struct,
    when,
)

from spark_expectations.config.user_config import Constants as constant_config
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core.exceptions import (
    SparkExpectationsMiscException,
    SparkExpectOrFailException,
)
from spark_expectations.utils.udf import get_actions_list, remove_empty_maps


class SparkExpectationsActions:
    """
    This class implements/supports applying data quality rules on given dataframe and performing required action
    """

    @staticmethod
    def get_rule_is_active(
        _context: SparkExpectationsContext,
        rule: dict,
        _rule_type_name: str,
        _source_dq_enabled: bool = False,
        _target_dq_enabled: bool = False,
    ) -> bool:
        """
        Args:
            _context: SparkExpectationsContext class object
            rule: dict with rule properties
            _rule_type_name: which determines the type of the rule
            _source_dq_enabled: Mark it as True when dq running for source dataframe
            _target_dq_enabled: Mark it as True when dq running for target dataframe

        Returns:

        """
        _is_active: bool = False
        if (
            _rule_type_name
            in [
                _context.get_query_dq_rule_type_name,
                _context.get_agg_dq_rule_type_name,
            ]
            and _source_dq_enabled is True
        ):
            _is_active = rule["enable_for_source_dq_validation"]
        elif (
            _rule_type_name
            in [
                _context.get_query_dq_rule_type_name,
                _context.get_agg_dq_rule_type_name,
            ]
            and _target_dq_enabled is True
        ):
            _is_active = rule["enable_for_target_dq_validation"]

        return _is_active

    @staticmethod
    def create_rules_map(_rule_map: Dict[str, str]) -> Any:
        """
        This function helps to extract the selected rules properties and returns array of dict with key and value

        Args:
            _rule_map: dict with rules properties
        Returns: Array of tuple with expectations rule properties

        """
        return array(
            [
                struct(lit(elem), lit(_rule_map.get(elem)))
                for elem in [
                    "rule_type",
                    "rule",
                    "action_if_failed",
                    "tag",
                    "description",
                ]
            ]
        )

    @staticmethod
    def match_parentheses(dq_query_string: str) -> bool:
        """
        Check if the parentheses in the given query string are properly matched.

        Args:
            dq_query_string (str): The query string to check.

        Returns:
            bool: True if all parentheses are properly matched, False otherwise.
        """
        _parentheses_branch_check: List = []
        for _index_val, _dq_query_string_char in enumerate(dq_query_string):
            if _dq_query_string_char == "(":
                _parentheses_branch_check.append(_index_val)
            elif _dq_query_string_char == ")":
                if not _parentheses_branch_check:
                    return False
                _parentheses_branch_check.pop()
        return not _parentheses_branch_check  # return True if no unmatched left parentheses remain

    @staticmethod
    def agg_query_dq_detailed_result(
        _context: SparkExpectationsContext,
        _dq_rule: Dict[str, str],
        df: DataFrame,
        querydq_output: List[
            Tuple[
                str,
                str,
                str,
                str,
                Any,
                str,
                str,
                dict,
                str,
            ]
        ],
        _source_dq_status: bool = False,
        _target_dq_status: bool = False,
    ) -> Any:
        """
        Executes detailed result aggregation for query-based data quality rules.

        Args:
            _context (SparkExpectationsContext): The context object containing Spark session and other information.
            _dq_rule (Dict[str, str]): The dictionary containing the data quality rule details.
            df (DataFrame): The input DataFrame to be evaluated against the data quality rule.
            querydq_output (List[Tuple[str, str, str, str, Any, str, dict, str]]):
            The list to store the querydq output.
            _source_dq_status (bool, optional):
            The flag indicating if the rule is for source data quality. Defaults to False.
            _target_dq_status (bool, optional):
            The flag indicating if the rule is for target data quality. Defaults to False.

        Returns:
            Any: The querydq_output and detailed result of the data quality rule.
        """

        try:
            if (
                _dq_rule["rule_type"] == _context.get_agg_dq_rule_type_name
                and _context.get_agg_dq_detailed_stats_status is True
            ):
                if not (">" in _dq_rule["expectation"] and "<" in _dq_rule["expectation"]):
                    _pattern = rf"{constant_config.se_agg_dq_expectation_regex_pattern}"
                    _re_compile = re.compile(_pattern)
                    _agg_dq_expectation_match = re.match(_re_compile, _dq_rule["expectation"])
                    _agg_dq_expectation_expr = None
                    if _agg_dq_expectation_match:
                        _agg_dq_expectation_aggstring = _agg_dq_expectation_match.group(1)
                        _agg_dq_expectation_expr = _agg_dq_expectation_match.group(2)
                        _agg_dq_expectation_cond_expr = expr(_agg_dq_expectation_aggstring)

                        _agg_dq_actual_count_value = int(df.agg(_agg_dq_expectation_cond_expr).collect()[0][0])

                        _agg_dq_expression_str = str(_agg_dq_actual_count_value) + _agg_dq_expectation_expr

                        _agg_dq_expr_condition = []

                        _agg_dq_expr_condition.append(
                            when(expr(_agg_dq_expression_str), True).otherwise(False).alias("agg_dq_aggregation_check")
                        )

                        _df_agg_dq_expr_result = df.select(*_agg_dq_expr_condition)

                        # status = "pass" if eval(_agg_dq_expression_str) else "fail"

                        status = (
                            "pass"
                            if _df_agg_dq_expr_result.filter(_df_agg_dq_expr_result["agg_dq_aggregation_check"]).count()
                            > 0
                            else "fail"
                        )

                        if _source_dq_status:
                            row_count = _context.get_input_count
                        elif _target_dq_status:
                            row_count = _context.get_output_count
                        else:
                            row_count = None

                        actual_row_count = row_count if status == "pass" else None
                        error_row_count = 0 if status == "pass" else row_count

                        actual_outcome = (
                            _agg_dq_actual_count_value if (_agg_dq_actual_count_value is not None) else None
                        )
                        expected_outcome = (
                            str(_agg_dq_expectation_expr) if (_agg_dq_expectation_expr is not None) else None
                        )
                else:
                    pattern = rf"{constant_config.se_agg_dq_expectation_range_regex_pattern}"
                    matches = re.match(pattern, _dq_rule["expectation"])
                    result = None
                    if matches:
                        result = matches.groups()
                        _agg_dq_expectation_aggstring = result[0]
                        _agg_dq_expectation_expr_lowerbound = result[1]
                        _agg_dq_expectation_expr_upperbound = result[4]

                        _agg_dq_expectation_cond_expr = expr(_agg_dq_expectation_aggstring)

                        _agg_dq_actual_count_value = int(df.agg(_agg_dq_expectation_cond_expr).collect()[0][0])

                        _agg_dq_expression_str_lower = (
                            str(_agg_dq_actual_count_value) + _agg_dq_expectation_expr_lowerbound
                        )

                        _agg_dq_expression_str_upper = (
                            str(_agg_dq_actual_count_value) + _agg_dq_expectation_expr_upperbound
                        )

                        _agg_dq_expr_condition = []

                        _agg_dq_expression_str = (
                            f"{str(_agg_dq_expression_str_lower)}" " and " f"{str(_agg_dq_expression_str_upper)}"
                        )

                        _agg_dq_expr_condition.append(
                            when(expr(_agg_dq_expression_str), True).otherwise(False).alias("agg_dq_aggregation_check")
                        )

                        _df_agg_dq_expr_result = df.select(*_agg_dq_expr_condition)

                        status = (
                            "pass"
                            if _df_agg_dq_expr_result.filter(_df_agg_dq_expr_result["agg_dq_aggregation_check"]).count()
                            > 0
                            else "fail"
                        )

                        if _source_dq_status:
                            row_count = _context.get_input_count
                        elif _target_dq_status:
                            row_count = _context.get_output_count
                        else:
                            row_count = None
                        actual_row_count = row_count if status == "pass" else None
                        error_row_count = 0 if status == "pass" else row_count

                        actual_outcome = (
                            _agg_dq_actual_count_value if (_agg_dq_actual_count_value is not None) else None
                        )

                        expected_outcome = (
                            _agg_dq_expression_str
                            if (_agg_dq_expression_str_lower is not None and _agg_dq_expression_str_upper is not None)
                            else None
                        )
            elif (
                _dq_rule["rule_type"] == _context.get_query_dq_rule_type_name
                and _context.get_query_dq_detailed_stats_status is True
            ):
                _querydq_secondary_query = _context.get_querydq_secondary_queries

                if _source_dq_status is True:
                    _query_prefix = "_source_dq"
                elif _target_dq_status is True:
                    _query_prefix = "_target_dq"
                else:
                    _query_prefix = ""

                if (_dq_rule["enable_querydq_custom_output"]) and (
                    sub_key_value := _querydq_secondary_query.get(
                        _dq_rule["product_id"] + "|" + _dq_rule["table_name"] + "|" + _dq_rule["rule"],
                        {},
                    )
                ):
                    for _key, _querydq_query in sub_key_value.items():
                        _querydq_df = _context.spark.sql(_dq_rule["expectation" + "_" + _key])
                        querydq_output.append(
                            (
                                _context.get_run_id,
                                _dq_rule["product_id"],
                                _dq_rule["table_name"],
                                _dq_rule["rule"],
                                _dq_rule["column_name"],
                                _key,
                                _query_prefix,
                                dict(
                                    [
                                        (
                                            _key,
                                            [row.asDict() for row in _querydq_df.collect()],
                                        )
                                    ]
                                ),
                                _context.get_run_date,
                            )
                        )

                if SparkExpectationsActions.match_parentheses(_dq_rule["expectation"]):
                    pattern = r"(\(.*\))\s*([<>!=]=?)\s*((\d+)|(\(.*\)))|(\(.*\))"
                    match = re.search(pattern, _dq_rule["expectation"])
                    if match:
                        # function to execute SQL and get the result
                        def execute_sql_and_get_result(_se_context: SparkExpectationsContext, query: str) -> int:
                            return _se_context.spark.sql(f"SELECT ({query}) AS OUTPUT").collect()[0][0] if query else 0

                        # function to get the query outputs
                        _querydq_source_query_output = execute_sql_and_get_result(_context, match.group(1))
                        _querydq_target_query_output = match.group(4) or execute_sql_and_get_result(
                            _context, match.group(5)
                        )

                        # assignment of actual_outcome and expected_outcome
                        actual_outcome = _querydq_source_query_output
                        expected_outcome = (
                            str(match.group(2)) + str(_querydq_target_query_output)
                            if _querydq_target_query_output
                            else None
                        )

                else:
                    raise SparkExpectationsMiscException(
                        """Sql query is invalid. Parentheses are missing in the sql query."""
                    )

                _querydq_status_query = "SELECT (" + str(_dq_rule["expectation"]) + ") AS OUTPUT"

                _query_dq_result = int(_context.spark.sql(_querydq_status_query).collect()[0][0])

                status = "pass" if _query_dq_result else "fail"

                if _source_dq_status:
                    row_count = _context.get_input_count
                elif _target_dq_status:
                    row_count = _context.get_output_count
                else:
                    row_count = None

                actual_row_count = row_count if _query_dq_result else None

                error_row_count = 0 if _query_dq_result else row_count

            else:
                status = None
                actual_row_count = None
                error_row_count = None
                row_count = None
                actual_outcome = None
                expected_outcome = None

            return querydq_output, (
                _context.get_run_id,
                _dq_rule["product_id"],
                _dq_rule["table_name"],
                _dq_rule["rule_type"],
                _dq_rule["rule"],
                _dq_rule["column_name"],
                _dq_rule["expectation"],
                _dq_rule["tag"],
                _dq_rule["description"],
                status,
                actual_outcome,
                expected_outcome,
                actual_row_count,
                error_row_count,
                row_count,
            )
        except Exception as e:
            raise SparkExpectationsMiscException(f"error occurred while running agg_query_dq_detailed_result {e}")

    @staticmethod
    def create_agg_dq_results(
        _context: SparkExpectationsContext, _df: DataFrame, _rule_type_name: str
    ) -> Optional[List[Dict[str, str]]]:
        """
        This function helps to collect the aggregation results in to the list
         Args:
             _context: SparkContext object
             _df: dataframe which contains agg data quality results
             _rule_type_name: which determines the type of the rule

         Returns:
                List with dict of agg rules property name and value


        """
        try:
            first_row = _df.first()
            if first_row is not None and f"meta_{_rule_type_name}_results" in _df.columns:
                meta_results = first_row[f"meta_{_rule_type_name}_results"]
                if meta_results is not None and len(meta_results) > 0:
                    return meta_results
            return None
        except Exception as e:
            raise SparkExpectationsMiscException(f"error occurred while running create agg dq results {e}")

    @staticmethod
    def run_dq_rules(
        _context: SparkExpectationsContext,
        df: DataFrame,
        expectations: Dict[str, List[dict]],
        rule_type: str,
        _source_dq_enabled: bool = False,
        _target_dq_enabled: bool = False,
    ) -> DataFrame:
        """
        This function builds the expressions for the data quality rules and returns the dataframe with results

        Args:
            _context: Provide SparkExpectationsContext
            df: Input dataframe on which data quality rules need to be applied
            expectations: Provide the dict which has all the rules
            rule_type: identifier for the type of rule to be applied in processing
            _source_dq_enabled: Mark it as True when dq running for source dataframe
            _target_dq_enabled: Mark it as True when dq running for target dataframe

        Returns:
            DataFrame: Returns a dataframe with all the rules run the input dataframe

        """
        try:
            condition_expressions: List = []
            _agg_query_dq_results: List = []
            querydq_output: List = []
            if len(expectations) <= 0:
                raise SparkExpectationsMiscException("no rules found to process")

            if f"{rule_type}_rules" not in expectations or len(expectations[f"{rule_type}_rules"]) <= 0:
                raise SparkExpectationsMiscException(
                    f"zero expectations to process for {rule_type}_rules from the `dq_rules` table, "
                    f"please configure rules or avoid this error by setting {rule_type} to False"
                )

            for rule in expectations[f"{rule_type}_rules"]:
                _rule_is_active = SparkExpectationsActions.get_rule_is_active(
                    _context,
                    rule,
                    rule_type,
                    _source_dq_enabled=_source_dq_enabled,
                    _target_dq_enabled=_target_dq_enabled,
                )

                if _rule_is_active or rule_type == _context.get_row_dq_rule_type_name:
                    rules_map = SparkExpectationsActions.create_rules_map(rule)
                    column = f"{rule_type}_{rule['rule']}"
                    condition_expressions.append(
                        when(
                            expr(rule["expectation"]),
                            map_from_entries(array_append(rules_map, struct(lit("status"), lit("pass")))),
                        )
                        .otherwise(map_from_entries(array_append(rules_map, struct(lit("status"), lit("fail")))))
                        .alias(column)
                    )
                    if (
                        rule_type
                        in (
                            _context.get_agg_dq_rule_type_name,
                            _context.get_query_dq_rule_type_name,
                        )
                    ) and (
                        _context.get_agg_dq_detailed_stats_status is True
                        or _context.get_query_dq_detailed_stats_status is True
                    ):
                        current_date = datetime.now()
                        dq_start_time = datetime.strftime(current_date, "%Y-%m-%d %H:%M:%S")

                        (
                            _querydq_output_list,
                            _agg_query_dq_output_tuple,
                        ) = SparkExpectationsActions.agg_query_dq_detailed_result(
                            _context,
                            rule,
                            df,
                            querydq_output,
                            _source_dq_status=_source_dq_enabled,
                            _target_dq_status=_target_dq_enabled,
                        )
                        current_date = datetime.now()
                        dq_end_time = datetime.strftime(current_date, "%Y-%m-%d %H:%M:%S")
                        _agg_query_dq_output_list = list(_agg_query_dq_output_tuple)
                        _agg_query_dq_output_list.extend([dq_start_time, dq_end_time])
                        _agg_query_dq_output_tuple = tuple(_agg_query_dq_output_list)
                        _agg_query_dq_results.append(_agg_query_dq_output_tuple)

            if (
                rule_type == _context.get_agg_dq_rule_type_name
                and _context.get_agg_dq_detailed_stats_status is True
                and _source_dq_enabled
            ):
                _context.set_source_agg_dq_detailed_stats(_agg_query_dq_results)

            elif (
                rule_type == _context.get_agg_dq_rule_type_name
                and _context.get_agg_dq_detailed_stats_status is True
                and _target_dq_enabled
            ):
                _context.set_target_agg_dq_detailed_stats(_agg_query_dq_results)

            elif (
                rule_type == _context.get_query_dq_rule_type_name
                and _context.get_query_dq_detailed_stats_status is True
                and _source_dq_enabled
            ):
                _context.set_source_query_dq_detailed_stats(_agg_query_dq_results)

                _context.set_source_query_dq_output(_querydq_output_list)

            elif (
                rule_type == _context.get_query_dq_rule_type_name
                and _context.get_query_dq_detailed_stats_status is True
                and _target_dq_enabled
            ):
                _context.set_target_query_dq_detailed_stats(_agg_query_dq_results)

                _context.set_target_query_dq_output(_querydq_output_list)

            if len(condition_expressions) > 0:
                if rule_type in [
                    _context.get_query_dq_rule_type_name,
                    _context.get_agg_dq_rule_type_name,
                ]:
                    df = df if rule_type == _context.get_agg_dq_rule_type_name else _context.get_supported_df_query_dq

                    df = df.select(*condition_expressions)

                    df = df.withColumn(f"meta_{rule_type}_results", array(*list(df.columns)))

                    df = df.withColumn(
                        f"meta_{rule_type}_results",
                        remove_empty_maps(df[f"meta_{rule_type}_results"]),
                    ).drop(*[_col for _col in df.columns if _col != f"meta_{rule_type}_results"])

                    _context.print_dataframe_with_debugger(df)

                elif rule_type == _context.get_row_dq_rule_type_name:
                    df = df.select(col("*"), *condition_expressions)

            else:
                raise SparkExpectationsMiscException(
                    f"zero active expectations to process for {rule_type}_rules from the `dq_rules` table, "
                    f"at {f'final_{rule_type}' if _source_dq_enabled else f'final_{rule_type}' }"
                    f", please configure rules or avoid this error by setting final_{rule_type} to False"
                )

            return df

        except Exception as e:
            raise SparkExpectationsMiscException(f"error occurred while running expectations {e}")

    @staticmethod
    def action_on_rules(
        _context: SparkExpectationsContext,
        _df_dq: DataFrame,
        _input_count: int,
        _error_count: int = 0,
        _output_count: int = 0,
        _rule_type: Optional[str] = None,
        _row_dq_flag: bool = False,
        _source_agg_dq_flag: bool = False,
        _final_agg_dq_flag: bool = False,
        _source_query_dq_flag: bool = False,
        _final_query_dq_flag: bool = False,
    ) -> DataFrame:
        """
        This function takes necessary action set by the user on the rules and returns the dataframe with results
        Args:
            _context: Provide SparkExpectationsContext
            _df_dq: Input dataframe on which data quality rules need to be applied
            _input_count: input dataset count
            _error_count: error count in the dataset
            _output_count: output count in the dataset
            _rule_type: type of rule expectations
            _row_dq_flag: Mark it as True when dq running for row level expectations
            _source_agg_dq_flag: Mark it as True when dq running for agg level expectations on source dataframe
            _final_agg_dq_flag: Mark it as True when dq running for agg level expectations on final dataframe
            _source_query_dq_flag: Mark it as True when dq running for query level expectations on source dataframe
            _final_query_dq_flag: Mark it as True when dq running for query level expectations on final dataframe
        Returns:
                DataFrame: Returns a dataframe after dropping the error from the dataset

        """
        try:
            _df_dq_columns = [
                dq_column
                for dq_column in _df_dq.columns
                if (dq_column.startswith(f"meta_{_rule_type}_results")) is False
            ]
            _df_dq_columns.append("action_if_failed")
            _df_dq = _df_dq.withColumn("action_if_failed", get_actions_list(col(f"meta_{_rule_type}_results"))).select(
                _df_dq_columns
            )

            if not _df_dq.filter(array_contains(_df_dq.action_if_failed, "fail")).count() > 0:
                _df_dq = _df_dq.filter(~array_contains(_df_dq.action_if_failed, "drop"))
            else:
                if _row_dq_flag:
                    _context.set_row_dq_status("Failed")
                elif _source_agg_dq_flag:
                    _context.set_source_agg_dq_status("Failed")
                elif _final_agg_dq_flag:
                    _context.set_final_agg_dq_status("Failed")
                elif _source_query_dq_flag:
                    _context.set_source_query_dq_status("Failed")
                elif _final_query_dq_flag:
                    _context.set_final_query_dq_status("Failed")

                raise SparkExpectOrFailException(
                    f"Job failed, as there is a data quality issue at {_rule_type} "
                    f"expectations and the action_if_failed "
                    "suggested to fail"
                )
            return _df_dq.select(_df_dq_columns[:-1])

        except Exception as e:
            raise SparkExpectationsMiscException(f"error occurred while taking action on given rules {e}")
