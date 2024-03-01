from typing import Dict, List, Any, Optional
from pyspark.sql import DataFrame

# from pyspark.sql.types import (
#     StringType,
#     ArrayType,
#     MapType,
# )
from pyspark.sql.functions import (
    create_map,
    expr,
    when,
    array,
    col,
    lit,
    struct,
    map_from_entries,
    array_contains,
)
from spark_expectations import _log
from spark_expectations.utils.udf import remove_empty_maps, get_actions_list
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.config.user_config import Constants as constant_config
from spark_expectations.core.exceptions import (
    SparkExpectationsMiscException,
    SparkExpectOrFailException,
)


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
    def _agg_dq_expectation_pattern_regex(
        _context: SparkExpectationsContext, _dq_rule: Dict[str, str], df: DataFrame
    ) -> Any:
        import re
        from ast import literal_eval

        try:
            if (
                _dq_rule["rule_type"] == _context.get_agg_dq_rule_type_name
                and _context.get_agg_dq_detailed_stats_status is True
            ):
                _pattern = f"{constant_config.se_agg_dq_expectation_regex_pattern}"
                _re_compile = re.compile(_pattern)
                _agg_dq_expectation_match = re.match(
                    _re_compile, _dq_rule["expectation"]
                )
                if _agg_dq_expectation_match:
                    print("inside match in agg_query_dq_detailed_result in actions.py")
                    _agg_dq_expectation_aggstring = _agg_dq_expectation_match.group(1)
                    _agg_dq_expectation_expr = _agg_dq_expectation_match.group(2)
                    _agg_dq_expectation_cond_expr = expr(_agg_dq_expectation_aggstring)

                    _agg_dq_actual_count_value = int(
                        df.agg(_agg_dq_expectation_cond_expr).collect()[0][0]
                    )
                    _agg_dq_expression_str = (
                        str(_agg_dq_actual_count_value) + _agg_dq_expectation_expr
                    )
                    status = "pass" if literal_eval(_agg_dq_expression_str) else "fail"
                    actual_value = [
                        _agg_dq_actual_count_value
                        if (_agg_dq_actual_count_value is not None)
                        else None
                    ]
                    expected_value = [
                        str(_agg_dq_expectation_expr)
                        if (_agg_dq_expectation_expr is not None)
                        else None
                    ]
            else:
                print(
                    "Detailed stats not enabled for the rule type: ",
                    _dq_rule["rule_type"],
                )
                status = None
                expected_value = None
                actual_value = None

            return (
                _context.get_run_id,
                _dq_rule["product_id"],
                _dq_rule["table_name"],
                _dq_rule["rule_type"],
                _dq_rule["rule"],
                _dq_rule["expectation"],
                status,
                actual_value,
                expected_value,
                _context.get_input_count,
            )
        except Exception as e:
            raise SparkExpectationsMiscException(
                f"error occurred while running _agg_dq_expectation_pattern_regex {e}"
            )

    @staticmethod
    def agg_query_dq_detailed_result(
        _context: SparkExpectationsContext,
        _dq_rule: Dict[str, str],
        df: DataFrame,
        _source_dq_status: bool = False,
        _target_dq_status: bool = False,
    ) -> Any:
        try:
            import re

            if (
                _dq_rule["rule_type"] == _context.get_agg_dq_rule_type_name
                and _context.get_agg_dq_detailed_stats_status is True
            ):
                _pattern = rf"{constant_config.se_agg_dq_expectation_regex_pattern}"
                _re_compile = re.compile(_pattern)
                _agg_dq_expectation_match = re.match(
                    _re_compile, _dq_rule["expectation"]
                )

                if _agg_dq_expectation_match:
                    print(
                        "inside match in agg_query_dq_detailed_result in actions.py:",
                        _agg_dq_expectation_match,
                    )
                    _agg_dq_expectation_aggstring = _agg_dq_expectation_match.group(1)
                    _agg_dq_expectation_expr = _agg_dq_expectation_match.group(2)
                    _agg_dq_expectation_cond_expr = expr(_agg_dq_expectation_aggstring)

                    _agg_dq_actual_count_value = int(
                        df.agg(_agg_dq_expectation_cond_expr).collect()[0][0]
                    )

                    _agg_dq_expression_str = (
                        str(_agg_dq_actual_count_value) + _agg_dq_expectation_expr
                    )

                    _agg_dq_expr_condition = []

                    _agg_dq_expr_condition.append(
                        when(expr(_agg_dq_expression_str), True)
                        .otherwise(False)
                        .alias("agg_dq_aggregation_check")
                    )

                    df_test = df.select(*_agg_dq_expr_condition)

                    # status = "pass" if eval(_agg_dq_expression_str) else "fail"

                    status = (
                        "pass"
                        if df_test.filter(df_test["agg_dq_aggregation_check"]).count()
                        > 0
                        else "fail"
                    )
                    actual_value = [
                        _agg_dq_actual_count_value
                        if (_agg_dq_actual_count_value is not None)
                        else None
                    ]
                    expected_value = [
                        str(_agg_dq_expectation_expr)
                        if (_agg_dq_expectation_expr is not None)
                        else None
                    ]

            elif (
                _dq_rule["rule_type"] == _context.get_query_dq_rule_type_name
                and _context.get_query_dq_detailed_stats_status is True
            ):
                if not (
                    _dq_rule["expectation_query_dq_source_query"] is not None
                    and _dq_rule["expectation_query_dq_target_query"] is not None
                ):
                    raise SparkExpectationsMiscException(
                        """detailed_stats_enabled query_dq expectation should have 3 parts - boolean query, 
                        source query and target query"""
                    )
                _querydq_status_query = (
                    "SELECT (" + str(_dq_rule["expectation"]) + ") AS OUTPUT"
                )
                print("querydq_status_query", _querydq_status_query)
                _querydq_source_query = _dq_rule["expectation_query_dq_source_query"]
                _querydq_target_query = _dq_rule["expectation_query_dq_target_query"]
                _query_dq_result = int(
                    _context.spark.sql(_querydq_status_query).collect()[0][0]
                )

                print("_query_dq_result check:", _query_dq_result)

                status = "pass" if _query_dq_result else "fail"
                _querydq_source_query_output = (
                    _context.spark.sql(_querydq_source_query).toJSON().collect()
                )

                _querydq_target_query_output = (
                    _context.spark.sql(_querydq_target_query).toJSON().collect()
                )
                if (
                    len(_querydq_source_query_output)
                    or len(_querydq_target_query_output) > 300
                ):
                    _log.info(
                        """The query output for query_dq [source and target] is more than the designated 300 rows. 
                        Writing only 300 records to detailed stats"""
                    )
                    _querydq_source_query_output = _querydq_source_query_output[0:300]
                    _querydq_target_query_output = _querydq_target_query_output[0:300]

                print("source_query_output", _querydq_source_query_output)
                print("target_query_output", _querydq_target_query_output)

                expected_value = (
                    _querydq_source_query_output
                    if (_querydq_source_query_output is not None)
                    else None
                )
                actual_value = (
                    _querydq_target_query_output
                    if (_querydq_target_query_output is not None)
                    else None
                )

            else:
                print(
                    "Detailed stats not enabled for the rule type: ",
                    _dq_rule["rule_type"],
                )
                status = None
                expected_value = None
                actual_value = None

            print(
                "result",
                _context.get_run_id,
                _dq_rule["product_id"],
                _dq_rule["table_name"],
                _dq_rule["rule_type"],
                _dq_rule["rule"],
                _dq_rule["expectation"],
                _dq_rule["tag"],
                _dq_rule["description"],
                status,
                actual_value,
                expected_value,
                _context.get_input_count,
            )

            return (
                _context.get_run_id,
                _dq_rule["product_id"],
                _dq_rule["table_name"],
                _dq_rule["rule_type"],
                _dq_rule["rule"],
                _dq_rule["expectation"],
                _dq_rule["tag"],
                _dq_rule["description"],
                status,
                actual_value,
                expected_value,
                _context.get_input_count,
            )
        except Exception as e:
            raise SparkExpectationsMiscException(
                f"error occurred while running agg_query_dq_detailed_result {e}"
            )

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
            if (
                first_row is not None
                and f"meta_{_rule_type_name}_results" in _df.columns
            ):
                meta_results = first_row[f"meta_{_rule_type_name}_results"]
                if meta_results is not None and len(meta_results) > 0:
                    return meta_results
            return None
        except Exception as e:
            raise SparkExpectationsMiscException(
                f"error occurred while running create agg dq results {e}"
            )

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
            print("rule_type in actions.py", rule_type)
            print("_source_dq_enabled in actions.py", _source_dq_enabled)
            print("_target_dq_enabled in actions.py", _target_dq_enabled)
            print("Initilized run_dq_rules in execute_dq_process in actions.py")
            condition_expressions = []
            _agg_query_dq_results = []
            if len(expectations) <= 0:
                raise SparkExpectationsMiscException("no rules found to process")

            if (
                f"{rule_type}_rules" not in expectations
                or len(expectations[f"{rule_type}_rules"]) <= 0
            ):
                raise SparkExpectationsMiscException(
                    f"zero expectations to process for {rule_type}_rules from the `dq_rules` table, "
                    f"please configure rules or avoid this error by setting {rule_type} to False"
                )
            print("###################################################################")
            print("Iterating through run_dq_rules in execute_dq_process in actions.py")
            print("###################################################################")
            print("_source_dq_enabled", _source_dq_enabled)
            print("_target_dq_enabled", _target_dq_enabled)
            for rule in expectations[f"{rule_type}_rules"]:
                _rule_is_active = SparkExpectationsActions.get_rule_is_active(
                    _context,
                    rule,
                    rule_type,
                    _source_dq_enabled=_source_dq_enabled,
                    _target_dq_enabled=_target_dq_enabled,
                )

                if _rule_is_active or rule_type == _context.get_row_dq_rule_type_name:
                    column = f"{rule_type}_{rule['rule']}"
                    condition_expressions.append(
                        when(expr(rule["expectation"]), create_map())
                        .otherwise(
                            map_from_entries(
                                SparkExpectationsActions.create_rules_map(rule)
                            )
                        )
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
                        print(
                            "###################################################################"
                        )
                        print(
                            "inside line 263 - calling agg_query_dq_detailed_result in actions.py"
                        )
                        _agg_query_dq_results.append(
                            SparkExpectationsActions.agg_query_dq_detailed_result(
                                _context,
                                rule,
                                df,
                                _source_dq_status=_source_dq_enabled,
                                _target_dq_status=_target_dq_enabled,
                            )
                        )

            print("###################################################################")
            print("agg_query_dq_result in actions.py 287", _agg_query_dq_results)

            if (
                rule_type == _context.get_agg_dq_rule_type_name
                and _context.get_agg_dq_detailed_stats_status is True
                and _source_dq_enabled
            ):
                _context.set_source_agg_dq_detailed_stats(_agg_query_dq_results)

                print(
                    "###################################################################"
                )
                print(
                    "source_agg_dq_detailed_stats in actions.py", _agg_query_dq_results
                )

            elif (
                rule_type == _context.get_agg_dq_rule_type_name
                and _context.get_agg_dq_detailed_stats_status is True
                and _target_dq_enabled
            ):
                _context.set_target_agg_dq_detailed_stats(_agg_query_dq_results)

                print(
                    "###################################################################"
                )
                print(
                    "target_agg_dq_detailed_stats in actions.py", _agg_query_dq_results
                )

            elif (
                rule_type == _context.get_query_dq_rule_type_name
                and _context.get_query_dq_detailed_stats_status is True
                and _source_dq_enabled
            ):
                _context.set_source_query_dq_detailed_stats(_agg_query_dq_results)

                print(
                    "###################################################################"
                )
                print(
                    "source_query_dq_detailed_stats in actions.py",
                    _agg_query_dq_results,
                )

            elif (
                rule_type == _context.get_query_dq_rule_type_name
                and _context.get_query_dq_detailed_stats_status is True
                and _target_dq_enabled
            ):
                _context.set_target_query_dq_detailed_stats(_agg_query_dq_results)

                print(
                    "###################################################################"
                )
                print(
                    "target_query_dq_detailed_stats in actions.py",
                    _agg_query_dq_results,
                )

            print("condition_expressions in actions.py", condition_expressions)

            if len(condition_expressions) > 0:
                if rule_type in [
                    _context.get_query_dq_rule_type_name,
                    _context.get_agg_dq_rule_type_name,
                ]:
                    df = (
                        df
                        if rule_type == _context.get_agg_dq_rule_type_name
                        else _context.get_supported_df_query_dq
                    )
                    print(
                        "###################################################################"
                    )
                    print(
                        "dataframe before select * expressions in actions.py",
                        df.show(truncate=False),
                    )

                    df = df.select(*condition_expressions)
                    print(
                        "###################################################################"
                    )
                    print(
                        "dataframe after select * expressions in actions.py",
                        df.show(truncate=False),
                    )

                    df = df.withColumn(
                        f"meta_{rule_type}_results", array(*list(df.columns))
                    )

                    print(
                        "###################################################################"
                    )
                    print(
                        "dataframe after adding meta_rulr_type_results in actions.py",
                        df.show(truncate=False),
                    )

                    df = df.withColumn(
                        f"meta_{rule_type}_results",
                        remove_empty_maps(df[f"meta_{rule_type}_results"]),
                    ).drop(
                        *[
                            _col
                            for _col in df.columns
                            if _col != f"meta_{rule_type}_results"
                        ]
                    )
                    print(
                        "###################################################################"
                    )
                    print(
                        "dataframe after remove_empty_maps in actions.py",
                        df.show(truncate=False),
                    )

                    _context.print_dataframe_with_debugger(df)

                elif rule_type == _context.get_row_dq_rule_type_name:
                    df = df.select(col("*"), *condition_expressions)
                    print(
                        "###################################################################"
                    )
                    print(
                        "dataframe after select condiditon_expression with row_dq  in actions.py",
                        df.show(truncate=False),
                    )

            else:
                raise SparkExpectationsMiscException(
                    f"zero active expectations to process for {rule_type}_rules from the `dq_rules` table, "
                    f"at {f'final_{rule_type}' if _source_dq_enabled else f'final_{rule_type}' }"
                    f", please configure rules or avoid this error by setting final_{rule_type} to False"
                )

            return df

        except Exception as e:
            raise SparkExpectationsMiscException(
                f"error occurred while running expectations {e}"
            )

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
            _df_dq = _df_dq.withColumn(
                "action_if_failed", get_actions_list(col(f"meta_{_rule_type}_results"))
            ).drop(f"meta_{_rule_type}_results")

            if (
                not _df_dq.filter(
                    array_contains(_df_dq.action_if_failed, "fail")
                ).count()
                > 0
            ):
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

            return _df_dq.drop(_df_dq.action_if_failed)

        except Exception as e:
            raise SparkExpectationsMiscException(
                f"error occured while taking action on given rules {e}"
            )
