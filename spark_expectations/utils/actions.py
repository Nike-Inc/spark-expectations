from typing import Dict, List, Any, Optional, Tuple
from pyspark.sql import DataFrame
from datetime import  datetime

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
    monotonically_increasing_id
)
from spark_expectations.utils.udf import remove_empty_maps, get_actions_list
from spark_expectations.core.context import SparkExpectationsContext
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
    def agg_dq_result_relational(_context, agg_rule: Dict[str, str], df: DataFrame) -> Any:
        import re
        from datetime import datetime
        current_date = datetime.now()
        dq_agg_date = current_date.strftime("%Y-%m-%d")
        dq_agg_start = datetime.strftime(current_date, "%Y-%m-%d %H:%M:%S")
        row_count = _context.get_input_count
        run_id = _context.get_run_id

        if agg_rule['rule_type'] == 'agg_dq':
            pattern = r'(\w+\(.+?\))([><=!]+.+)$'
            match = re.match(pattern, agg_rule["expectation"])
            if match:
                part1 = match.group(1)
                part2 = match.group(2)
                condition_expression = expr(part1)
                actual_count_value = int(df.agg(condition_expression).collect()[0][0])
                expression_str = str(actual_count_value) + part2
                status = 'pass' if eval(expression_str) else 'fail'
                actual_count = row_count if eval(expression_str) else None
                error_count = 0 if eval(expression_str) else row_count
        elif agg_rule['rule_type'] == 'query_dq':
            query = "SELECT (" + str(agg_rule['expectation']) + ") AS RESULT"
            output = int(_context.spark.sql(query).collect()[0][0])
            status = 'pass' if output == True else 'fail'
            actual_count = row_count if output == True else None
            error_count = 0 if output == True else row_count
        else:
            print("No match")
            status = None
            actual_count = None
            error_count = None

        dq_agg_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        return (run_id, agg_rule["product_id"], agg_rule["table_name"], agg_rule["rule_type"], agg_rule["rule"],
                agg_rule["expectation"], status, error_count, actual_count, row_count, dq_agg_date, dq_agg_start,
                dq_agg_end)


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
            condition_expressions = []
            agg_dq_results = []
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
                    
                    if _context.get_se_stats_relational_format and rule_type in ['agg_dq', 'query_dq']:
                        agg_dq_results.append(SparkExpectationsActions.agg_dq_result_relational(_context, rule, df))

            if _context.get_se_stats_relational_format:
                if rule['rule_type'] == 'agg_dq':
                    _context.set_agg_dq_result_relational(agg_dq_result_relational = agg_dq_results)
                elif rule['rule_type'] == 'query_dq':
                    _context.set_query_dq_result_relational(query_dq_result_relational=agg_dq_results)

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

                    df = df.select(*condition_expressions)
                    df = df.withColumn(
                        f"meta_{rule_type}_results", array(*list(df.columns))
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
            _df_dq_main = _df_dq
            if 'sequence_number' in [column for column in _df_dq.columns]:
                _df_dq_seq = _df_dq.select("sequence_number", f"meta_{_rule_type}_results")
                _df_dq = _df_dq_seq.withColumn(
                    "action_if_failed", get_actions_list(col(f"meta_{_rule_type}_results"))
                ).drop(f"meta_{_rule_type}_results")

            else:
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
                #added below line to raise expection after executing dq rules
                _context._set_action_if_failed_fail = True
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

                # raise SparkExpectOrFailException(
                #     f"Job failed, as there is a data quality issue at {_rule_type} "
                #     f"expectations and the action_if_failed "
                #     "suggested to fail"
                # )

            if 'sequence_number' in [column for column in _df_dq.columns]:
                _df_dq = _df_dq_main.join(_df_dq, _df_dq.sequence_number == _df_dq_main.sequence_number, "left").select(
                    *[dq_column for dq_column in _df_dq_main.columns if
                      dq_column.startswith("sequence_number") == False])
            else:
                _df_dq = _df_dq.select(
                    *[dq_column for dq_column in _df_dq.columns if dq_column.startswith("action_if_failed") == False])
            return _df_dq

        except Exception as e:
            raise SparkExpectationsMiscException(
                f"error occurred while taking action on given rules {e}"
            )
