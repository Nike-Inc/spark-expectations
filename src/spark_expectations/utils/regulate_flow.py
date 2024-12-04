from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Any
from pyspark.sql import DataFrame
from spark_expectations.core.exceptions import (
    SparkExpectationsMiscException,
)
from spark_expectations import _log
from spark_expectations.notifications.push.spark_expectations_notify import (
    SparkExpectationsNotify,
)
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.sinks.utils.writer import SparkExpectationsWriter
from spark_expectations.utils.actions import SparkExpectationsActions


@dataclass
class SparkExpectationsRegulateFlow:
    """
    This is helper class and implements/supports running data quality flow
    """

    product_id: str

    @staticmethod
    def execute_dq_process(
        _context: SparkExpectationsContext,
        _actions: SparkExpectationsActions,
        _writer: SparkExpectationsWriter,
        _notification: SparkExpectationsNotify,
        expectations: Dict[str, List[dict]],
        table_name: str,
        _input_count: int = 0,
    ) -> Any:
        """
        This functions takes required static variable and returns the function
        Args:
            _context: SparkExpectationsContext class object
            _actions: SparkExpectationsActions class object
            _writer: SparkExpectationsWriter class object
            _notification: SparkExpectationsNotify class object
            expectations: expectations dictionary which contains rules
            table_name: name of the table
            _input_count: number of records in the source dataframe
        Returns:
               Any: returns function

        """

        def func_process(
            df: DataFrame,
            _rule_type: str,
            row_dq_flag: bool = False,
            source_agg_dq_flag: bool = False,
            final_agg_dq_flag: bool = False,
            source_query_dq_flag: bool = False,
            final_query_dq_flag: bool = False,
            error_count: int = 0,
            output_count: int = 0,
        ) -> Tuple[DataFrame, Optional[List[Dict[str, str]]], int, str]:
            """
            This inner function helps to process data quality rules based on different rules types
            Args:
                df: dataframe for data quality
                _rule_type: type of the rule
                row_dq_flag: default false, Mark True tp process row level data quality
                source_agg_dq_flag: default false, Mark True tp process agg level data quality on source dataframe
                final_agg_dq_flag: default false, Mark True tp process agg level data quality on final dataframe
                source_query_dq_flag: default false, Mark True tp process query level data quality on source dataframe
                final_query_dq_flag: default false, Mark True tp process query level data quality on final dataframe
                error_count: number of records error records (default zero)
                output_count: number of output records from expectations (default zero)

            Returns:
                   Tuples with data frame which contains dq result, agg result in list, error count and
                   status of the flow

            """
            try:
                _error_df: Optional[DataFrame] = None
                _error_count: int = error_count

                _running_rule_type_name = (
                    _context.get_row_dq_rule_type_name
                    if row_dq_flag
                    else (
                        _context.get_agg_dq_rule_type_name
                        if (source_agg_dq_flag or final_agg_dq_flag)
                        else _context.get_query_dq_rule_type_name
                    )
                )

                _log.info(
                    "The data quality dataframe is getting created for expectations"
                )

                _df_dq: DataFrame = _actions.run_dq_rules(
                    _context,
                    df,
                    expectations,
                    _running_rule_type_name,
                    _source_dq_enabled=(
                        source_query_dq_flag is True or source_agg_dq_flag is True
                    ),
                    _target_dq_enabled=(
                        final_query_dq_flag is True or final_agg_dq_flag is True
                    ),
                )

                _log.info("The data quality dataframe is created for expectations")
                _context.print_dataframe_with_debugger(_df_dq)

                agg_dq_res = (
                    _actions.create_agg_dq_results(
                        _context, _df_dq, _running_rule_type_name
                    )
                    if row_dq_flag is False
                    else None
                )

                if row_dq_flag:
                    _log.info("Writing error records into the table started")

                    _error_count, _error_df = _writer.write_error_records_final(
                        _df_dq,
                        f"{table_name}_error",
                        _context.get_row_dq_rule_type_name,
                    )
                    if _context.get_summarized_row_dq_res:
                        _notification.notify_rules_exceeds_threshold(expectations)
                        _writer.generate_rules_exceeds_threshold(expectations)

                    _context.print_dataframe_with_debugger(_error_df)

                    # set the error count
                    _context.set_error_count(_error_count)

                # set agg result
                if source_agg_dq_flag:
                    _context.set_source_agg_dq_result(agg_dq_res)
                elif final_agg_dq_flag:
                    _context.set_final_agg_dq_result(agg_dq_res)
                elif source_query_dq_flag:
                    _context.set_source_query_dq_result(agg_dq_res)
                elif final_query_dq_flag:
                    _context.set_final_query_dq_result(agg_dq_res)

                df = _actions.action_on_rules(
                    _context,
                    _error_df if row_dq_flag else _df_dq,
                    _input_count,
                    _error_count=_error_count,
                    _output_count=output_count,
                    _rule_type=_running_rule_type_name,
                    _row_dq_flag=row_dq_flag,
                    _source_agg_dq_flag=source_agg_dq_flag,
                    _final_agg_dq_flag=final_agg_dq_flag,
                    _source_query_dq_flag=source_query_dq_flag,
                    _final_query_dq_flag=final_query_dq_flag,
                )
                _context.print_dataframe_with_debugger(df)

                return df, agg_dq_res, _error_count, "Passed"

            except Exception as e:
                raise SparkExpectationsMiscException(
                    f"error occurred while executing func_process {e}"
                )

        return func_process
