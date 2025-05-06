import functools
import importlib
from dataclasses import dataclass
from typing import Dict, Optional, Any, Union, List

from pyspark.version import __version__ as spark_version
from pyspark import StorageLevel
from pyspark import sql


from spark_expectations import _log
from spark_expectations.config.user_config import Constants as user_config
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core.exceptions import (
    SparkExpectationsMiscException,
    SparkExpectationsDataframeNotReturnedException,
)
from spark_expectations.notifications.push.spark_expectations_notify import (
    SparkExpectationsNotify,
)
from spark_expectations.sinks.utils.collect_statistics import (
    SparkExpectationsCollectStatistics,
)
from spark_expectations.sinks.utils.writer import SparkExpectationsWriter
from spark_expectations.utils.actions import SparkExpectationsActions
from spark_expectations.utils.reader import SparkExpectationsReader
from spark_expectations.utils.regulate_flow import SparkExpectationsRegulateFlow


def get_spark_minor_version() -> float:
    """Returns the minor version of the spark instance.

    For example, if the spark version is 3.3.2, this function would return 3.3
    """
    return float(".".join(spark_version.split(".")[:2]))


MIN_SPARK_VERSION_FOR_CONNECT: float = 3.4
SPARK_MINOR_VERSION: float = get_spark_minor_version()


def check_if_pyspark_connect_is_supported() -> bool:
    """Check if the current version of PySpark supports the connect module"""
    result = False
    module_name: str = "pyspark"
    if SPARK_MINOR_VERSION >= MIN_SPARK_VERSION_FOR_CONNECT:
        try:
            importlib.import_module(f"{module_name}.sql.connect")
            from pyspark.sql.connect.column import Column

            _col: Column
            result = True
        except (ModuleNotFoundError, ImportError):
            result = False
    return result


# pylint: disable=ungrouped-imports
if check_if_pyspark_connect_is_supported():
    # Import the connect module if the current version of PySpark supports it
    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
    from pyspark.sql.connect.session import SparkSession as ConnectSparkSession

    DataFrame = Union[sql.DataFrame, ConnectDataFrame]  # type: ignore
    SparkSession = Union[sql.SparkSession, ConnectSparkSession]  # type: ignore
else:
    # Otherwise, use the default PySpark classes
    from pyspark.sql.dataframe import DataFrame  # type: ignore
    from pyspark.sql.session import SparkSession  # type: ignore


__all__ = [
    "SparkExpectations",
    "WrappedDataFrameWriter",
]


@dataclass
class SparkExpectations:
    """
    This class implements/supports running the data quality rules on a dataframe returned by a function

    Args:
        product_id: Name of the product
        rules_df: DataFrame which contains the rules. User is responsible for reading
            the rules_table in which ever system it is
        stats_table: Name of the table where the stats/audit-info need to be written
        debugger: Mark it as "True" if the debugger mode need to be enabled, by default is False
        stats_streaming_options: Provide options to override the defaults, while writing into the stats streaming table
    """

    product_id: str
    rules_df: DataFrame
    stats_table: str
    target_and_error_table_writer: "WrappedDataFrameWriter"
    stats_table_writer: "WrappedDataFrameWriter"
    debugger: bool = False
    stats_streaming_options: Optional[Dict[str, Union[str, bool]]] = None
    spark: Optional[SparkSession] = None

    def __post_init__(self) -> None:
        if isinstance(self.rules_df, DataFrame):  # type: ignore
            try:
                self.spark = self.rules_df.sparkSession
            except AttributeError:
                # we need to ensure to use the correct SparkSession type based
                # on if pyspark[connect] is supported or not
                if check_if_pyspark_connect_is_supported():
                    from pyspark.sql.connect.session import (
                        SparkSession as _ConnectSparkSession,
                    )

                    session = _ConnectSparkSession.getActiveSession() or sql.SparkSession.getActiveSession()  # type: ignore
                else:
                    session = sql.SparkSession.getActiveSession()  # type: ignore

                self.spark = session

            if self.spark is None:
                raise SparkExpectationsMiscException(
                    "Spark session is not available, please initialize a spark session before calling SE"
                )

        else:
            raise SparkExpectationsMiscException("Input rules_df is not of dataframe type")

        self.actions: SparkExpectationsActions = SparkExpectationsActions()
        self._context: SparkExpectationsContext = SparkExpectationsContext(product_id=self.product_id, spark=self.spark)

        self._writer = SparkExpectationsWriter(_context=self._context)
        self._process = SparkExpectationsRegulateFlow(product_id=self.product_id)
        self._notification: SparkExpectationsNotify = SparkExpectationsNotify(_context=self._context)
        self._statistics_decorator = SparkExpectationsCollectStatistics(
            _context=self._context,
            _writer=self._writer,
        )
        self.reader = SparkExpectationsReader(_context=self._context)

        self._context.set_target_and_error_table_writer_config(self.target_and_error_table_writer.build())
        self._context.set_stats_table_writer_config(self.stats_table_writer.build())
        self._context.set_detailed_stats_table_writer_config(self.stats_table_writer.build())
        self._context.set_debugger_mode(self.debugger)
        self._context.set_dq_stats_table_name(self.stats_table)
        self._context.set_dq_detailed_stats_table_name(f"{self.stats_table}_detailed")
        self.rules_df = self.rules_df.persist(StorageLevel.MEMORY_AND_DISK)

    # TODO Add target_error_table_writer and stats_table_writer as parameters to this function so this takes precedence
    #  if user provides it
    def with_expectations(
        self,
        target_table: str,
        write_to_table: bool = False,
        write_to_temp_table: bool = False,
        user_conf: Optional[Dict[str, Union[str, int, bool, Dict[str, str]]]] = None,
        target_table_view: Optional[str] = None,
        target_and_error_table_writer: Optional["WrappedDataFrameWriter"] = None,
    ) -> Any:
        """
        This decorator helps to wrap a function which returns dataframe and apply dataframe rules on it

        Args:
            target_table: Name of the table where the final dataframe need to be written
            write_to_table: Mark it as "True" if the dataframe need to be written as table
            write_to_temp_table: Mark it as "True" if the input dataframe need to be written to the temp table to break
                                the spark plan
            user_conf: Provide options to override the defaults, while writing into the stats streaming table
            target_table_view: This view is created after the _row_dq process to run the target agg_dq and query_dq.
                If value is not provided, defaulted to {target_table}_view
            target_and_error_table_writer: Provide the writer to write the target and error table,
                this will take precedence over the class level writer

        Returns:
            Any: Returns a function which applied the expectations on dataset
        """

        def _except(func: Any) -> Any:
            # variable used for enabling notification at different level

            _default_notification_dict: Dict[str, Union[str, int, bool, Dict[str, str], None]] = {
                user_config.se_notifications_on_start: False,
                user_config.se_notifications_on_completion: True,
                user_config.se_notifications_on_fail: True,
                user_config.se_notifications_on_error_drop_exceeds_threshold_breach: False,
                user_config.se_notifications_on_rules_action_if_failed_set_ignore: False,
                user_config.se_notifications_on_error_drop_threshold: 100,
                user_config.se_enable_agg_dq_detailed_result: False,
                user_config.se_enable_query_dq_detailed_result: False,
                user_config.se_job_metadata: None,
                user_config.querydq_output_custom_table_name: f"{self.stats_table}_querydq_output",
            }

            _notification_dict: Dict[str, Union[str, int, bool, Dict[str, str], None]] = (
                {**_default_notification_dict, **user_conf} if user_conf else _default_notification_dict
            )
            _default_stats_streaming_dict: Dict[str, Union[bool, str]] = {
                user_config.se_enable_streaming: True,
                user_config.secret_type: "databricks",
                user_config.dbx_workspace_url: "https://workspace.cloud.databricks.com",
                user_config.dbx_secret_scope: "secret_scope",
                user_config.dbx_kafka_server_url: "se_streaming_server_url_secret_key",
                user_config.dbx_secret_token_url: "se_streaming_auth_secret_token_url_key",
                user_config.dbx_secret_app_name: "se_streaming_auth_secret_appid_key",
                user_config.dbx_secret_token: "se_streaming_auth_secret_token_key",
                user_config.dbx_topic_name: "se_streaming_topic_name",
            }
            _se_stats_streaming_dict: Dict[str, Any] = (
                {**self.stats_streaming_options} if self.stats_streaming_options else _default_stats_streaming_dict
            )

            enable_error_table = _notification_dict.get(user_config.se_enable_error_table, True)
            self._context.set_se_enable_error_table(
                enable_error_table if isinstance(enable_error_table, bool) else True
            )

            dq_rules_params = _notification_dict.get(user_config.se_dq_rules_params, {})
            self._context.set_dq_rules_params(dq_rules_params if isinstance(dq_rules_params, dict) else {})

            # Overwrite the writers if provided by the user in the with_expectations explicitly
            if target_and_error_table_writer:
                self._context.set_target_and_error_table_writer_config(target_and_error_table_writer.build())

            _agg_dq_detailed_stats: bool = (
                bool(_notification_dict[user_config.se_enable_agg_dq_detailed_result])
                if isinstance(
                    _notification_dict[user_config.se_enable_agg_dq_detailed_result],
                    bool,
                )
                else False
            )

            _query_dq_detailed_stats: bool = (
                bool(_notification_dict[user_config.se_enable_query_dq_detailed_result])
                if isinstance(
                    _notification_dict[user_config.se_enable_query_dq_detailed_result],
                    bool,
                )
                else False
            )

            if _agg_dq_detailed_stats or _query_dq_detailed_stats:
                if _agg_dq_detailed_stats:
                    self._context.set_agg_dq_detailed_stats_status(_agg_dq_detailed_stats)

                if _query_dq_detailed_stats:
                    self._context.set_query_dq_detailed_stats_status(_query_dq_detailed_stats)

                self._context.set_query_dq_output_custom_table_name(
                    str(_notification_dict[user_config.querydq_output_custom_table_name])
                )

            # need to call the get_rules_frm_table function to get the rules from the table as expectations
            (
                dq_queries_dict,
                expectations,
                rules_execution_settings,
            ) = self.reader.get_rules_from_df(self.rules_df, target_table, params=self._context.get_dq_rules_params)

            _row_dq: bool = rules_execution_settings.get("row_dq", False)
            _source_agg_dq: bool = rules_execution_settings.get("source_agg_dq", False)
            _target_agg_dq: bool = rules_execution_settings.get("target_agg_dq", False)
            _source_query_dq: bool = rules_execution_settings.get("source_query_dq", False)
            _target_query_dq: bool = rules_execution_settings.get("target_query_dq", False)
            _target_table_view: str = target_table_view if target_table_view else f"{target_table.split('.')[-1]}_view"

            _notification_on_start: bool = (
                bool(_notification_dict[user_config.se_notifications_on_start])
                if isinstance(
                    _notification_dict[user_config.se_notifications_on_start],
                    bool,
                )
                else False
            )
            _notification_on_completion: bool = (
                bool(_notification_dict[user_config.se_notifications_on_completion])
                if isinstance(
                    _notification_dict[user_config.se_notifications_on_completion],
                    bool,
                )
                else False
            )
            _notification_on_fail: bool = (
                bool(_notification_dict[user_config.se_notifications_on_fail])
                if isinstance(
                    _notification_dict[user_config.se_notifications_on_fail],
                    bool,
                )
                else False
            )
            _notification_on_error_drop_exceeds_threshold_breach: bool = (
                bool(_notification_dict[user_config.se_notifications_on_error_drop_exceeds_threshold_breach])
                if isinstance(
                    _notification_dict[user_config.se_notifications_on_error_drop_exceeds_threshold_breach],
                    bool,
                )
                else False
            )
            _notifications_on_rules_action_if_failed_set_ignore: bool = (
                bool(_notification_dict[user_config.se_notifications_on_rules_action_if_failed_set_ignore])
                if isinstance(
                    _notification_dict[user_config.se_notifications_on_rules_action_if_failed_set_ignore],
                    bool,
                )
                else False
            )

            # _job_metadata: str = user_config.se_job_metadata
            _job_metadata: Optional[str] = (
                str(_notification_dict[user_config.se_job_metadata])
                if isinstance(_notification_dict[user_config.se_job_metadata], str)
                else None
            )

            notifications_on_error_drop_threshold = _notification_dict.get(
                user_config.se_notifications_on_error_drop_threshold, 100
            )
            _error_drop_threshold: int = (
                notifications_on_error_drop_threshold if isinstance(notifications_on_error_drop_threshold, int) else 100
            )

            self.reader.set_notification_param(user_conf)
            self._context.set_notification_on_start(_notification_on_start)
            self._context.set_notification_on_completion(_notification_on_completion)
            self._context.set_notification_on_fail(_notification_on_fail)

            self._context.set_se_streaming_stats_dict(_se_stats_streaming_dict)
            self._context.set_dq_expectations(expectations)
            self._context.set_rules_execution_settings_config(rules_execution_settings)
            self._context.set_querydq_secondary_queries(dq_queries_dict)
            self._context.set_job_metadata(_job_metadata)

            @self._notification.send_notification_decorator
            @self._statistics_decorator.collect_stats_decorator
            @functools.wraps(func)
            def wrapper(*args: tuple, **kwargs: dict) -> DataFrame:
                try:
                    _log.info("The function dataframe is getting created")
                    # _df: DataFrame = func(*args, **kwargs)
                    _df: DataFrame = func(*args, **kwargs)
                    table_name: str = self._context.get_table_name

                    _input_count = _df.count()
                    _log.info(f"data frame input record count: {_input_count}")
                    _output_count: int = 0
                    _error_count: int = 0
                    _source_dq_df: Optional[DataFrame] = None
                    _source_query_dq_df: Optional[DataFrame] = None
                    failed_ignored_row_dq_res: List[Dict[str, str]] = []
                    _row_dq_df: DataFrame = _df
                    _final_dq_df: Optional[DataFrame] = None
                    _final_query_dq_df: Optional[DataFrame] = None
                    _ignore_rules_result: List[Optional[List[Dict[str, str]]]] = []

                    # initialize variable with default values through set
                    self._context.set_dq_run_status()
                    self._context.set_source_agg_dq_status()
                    self._context.set_source_query_dq_status()
                    self._context.set_row_dq_status()
                    self._context.set_final_agg_dq_status()
                    self._context.set_final_query_dq_status()
                    self._context.set_input_count()
                    self._context.set_error_count()
                    self._context.set_output_count()
                    self._context.set_source_agg_dq_result()
                    self._context.set_final_agg_dq_result()
                    self._context.set_source_query_dq_result()
                    self._context.set_final_query_dq_result()
                    self._context.set_summarized_row_dq_res()
                    self._context.set_dq_expectations(expectations)

                    # initialize variables of start and end time with default values
                    self._context._source_agg_dq_start_time = None
                    self._context._final_agg_dq_start_time = None
                    self._context._source_query_dq_start_time = None
                    self._context._final_query_dq_start_time = None
                    self._context._row_dq_start_time = None

                    self._context._source_agg_dq_end_time = None
                    self._context._final_agg_dq_end_time = None
                    self._context._source_query_dq_end_time = None
                    self._context._final_query_dq_end_time = None
                    self._context._row_dq_end_time = None

                    self._context.set_input_count(_input_count)
                    self._context.set_error_drop_threshold(_error_drop_threshold)

                    _log.info(f"Spark Expectations run id for this run: {self._context.get_run_id}")

                    if isinstance(_df, DataFrame):  # type: ignore
                        _log.info("The function dataframe is created")
                        self._context.set_table_name(table_name)
                        if write_to_temp_table:
                            _log.info("Dropping to temp table started")
                            self.spark.sql(f"drop table if exists {table_name}_temp")  # type: ignore
                            _log.info("Dropping to temp table completed")
                            _log.info("Writing to temp table started")
                            source_columns = _df.columns
                            self._writer.save_df_as_table(
                                _df,
                                f"{table_name}_temp",
                                self._context.get_target_and_error_table_writer_config,
                            )
                            _log.info("Read from temp table started")
                            _df = self.spark.sql(f"select * from {table_name}_temp")  # type: ignore
                            _df = _df.select(source_columns)
                            _log.info("Read from temp table completed")

                        func_process = self._process.execute_dq_process(
                            _context=self._context,
                            _actions=self.actions,
                            _writer=self._writer,
                            _notification=self._notification,
                            expectations=expectations,
                            table_name=table_name,
                            _input_count=_input_count,
                        )

                        if _source_agg_dq is True:
                            _log.info(
                                "started processing data quality rules for agg level expectations on soure dataframe"
                            )
                            self._context.set_source_agg_dq_status("Failed")
                            self._context.set_source_agg_dq_start_time()
                            # In this steps source agg data quality expectations runs on raw_data
                            # returns:
                            #        _source_dq_df: applied data quality dataframe,
                            #        _dq_source_agg_results: source aggregation result in dictionary
                            #        _: place holder for error data at row level
                            #        status: status of the execution

                            (
                                _source_dq_df,
                                _dq_source_agg_results,
                                _,
                                status,
                            ) = func_process(
                                _df,
                                self._context.get_agg_dq_rule_type_name,
                                source_agg_dq_flag=True,
                            )
                            self._context.set_source_agg_dq_status(status)
                            self._context.set_source_agg_dq_end_time()

                            _log.info(
                                "ended processing data quality rules for agg level expectations on source dataframe"
                            )

                        if _source_query_dq is True:
                            _log.info(
                                "started processing data quality rules for query level expectations on soure dataframe"
                            )
                            self._context.set_source_query_dq_status("Failed")
                            self._context.set_source_query_dq_start_time()
                            # In this steps source query data quality expectations runs on raw_data
                            # returns:
                            #        _source_query_dq_df: applied data quality dataframe,
                            #        _dq_source_query_results: source query dq results in dictionary
                            #        _: place holder for error data at row level
                            #        status: status of the execution

                            (
                                _source_query_dq_df,
                                _dq_source_query_results,
                                _,
                                status,
                            ) = func_process(
                                _df,
                                self._context.get_query_dq_rule_type_name,
                                source_query_dq_flag=True,
                            )
                            self._context.set_source_query_dq_status(status)
                            self._context.set_source_query_dq_end_time()
                            _log.info(
                                "ended processing data quality rules for query level expectations on source dataframe"
                            )

                        if _row_dq is True:
                            _log.info("started processing data quality rules for row level expectations")
                            self._context.set_row_dq_status("Failed")
                            self._context.set_row_dq_start_time()
                            # In this steps row level data quality expectations runs on raw_data
                            # returns:
                            #        _row_dq_df: applied data quality dataframe at row level on raw dataframe,
                            #        _: place holder for aggregation
                            #        _error_count: number of error records
                            #        status: status of the execution
                            (_row_dq_df, _, _error_count, status) = func_process(
                                _df,
                                self._context.get_row_dq_rule_type_name,
                                row_dq_flag=True,
                            )
                            self._context.set_error_count(_error_count)

                            _row_dq_df.createOrReplaceTempView(_target_table_view)

                            _output_count = _row_dq_df.count() if _row_dq_df else 0
                            self._context.set_output_count(_output_count)

                            self._context.set_row_dq_status(status)
                            self._context.set_row_dq_end_time()

                            if (
                                _notification_on_error_drop_exceeds_threshold_breach is True
                                and (100 - self._context.get_output_percentage) >= _error_drop_threshold
                            ):
                                self._notification.notify_on_exceeds_of_error_threshold()

                            if (
                                _notifications_on_rules_action_if_failed_set_ignore is True
                                and isinstance(self._context.get_error_percentage, (int, float))
                                and self._context.get_error_percentage >= _error_drop_threshold
                            ):
                                failed_ignored_row_dq_res = [
                                    rule
                                    for rule in self._context.get_summarized_row_dq_res or []
                                    if rule["action_if_failed"] == "ignore"
                                    and isinstance(rule["failed_row_count"], int)
                                    and rule["failed_row_count"] > 0
                                ]
                                # raise SparkExpectationsErrorThresholdExceedsException(
                                #     "An error has taken place because"
                                #     " the set limit for acceptable"
                                #     " errors, known as the error"
                                #     " threshold, has been surpassed"
                                # )
                            _log.info("ended processing data quality rules for row level expectations")

                        if _row_dq is True and _target_agg_dq is True:
                            _log.info(
                                "started processing data quality rules for agg level expectations on final dataframe"
                            )
                            self._context.set_final_agg_dq_status("Failed")
                            self._context.set_final_agg_dq_start_time()
                            # In this steps final agg data quality expectations run on final dataframe
                            # returns:
                            #        _final_dq_df: applied data quality dataframe at row level on raw dataframe,
                            #        _dq_final_agg_results: final agg dq result in dictionary
                            #        _: number of error records
                            #        status: status of the execution

                            (
                                _final_dq_df,
                                _dq_final_agg_results,
                                _,
                                status,
                            ) = func_process(
                                _row_dq_df,
                                self._context.get_agg_dq_rule_type_name,
                                final_agg_dq_flag=True,
                                error_count=_error_count,
                                output_count=_output_count,
                            )
                            self._context.set_final_agg_dq_status(status)
                            self._context.set_final_agg_dq_end_time()
                            _log.info(
                                "ended processing data quality rules for agg level expectations on final dataframe"
                            )

                        if _row_dq is True and _target_query_dq is True:
                            _log.info(
                                "started processing data quality rules for query level expectations on final dataframe"
                            )
                            self._context.set_final_query_dq_status("Failed")
                            self._context.set_final_query_dq_start_time()
                            # In this steps final query dq data quality expectations run on final dataframe
                            # returns:
                            #        _final_query_dq_df: applied data quality dataframe at row level on raw dataframe,
                            #        _dq_final_query_results: final query dq result in dictionary
                            #        _: number of error records
                            #        status: status of the execution

                            _row_dq_df.createOrReplaceTempView(_target_table_view)

                            (
                                _final_query_dq_df,
                                _dq_final_query_results,
                                _,
                                status,
                            ) = func_process(
                                _row_dq_df,
                                self._context.get_query_dq_rule_type_name,
                                final_query_dq_flag=True,
                                error_count=_error_count,
                                output_count=_output_count,
                            )
                            self._context.set_final_query_dq_status(status)
                            self._context.set_final_query_dq_end_time()

                            _log.info(
                                "ended processing data quality rules for query level expectations on final dataframe"
                            )

                        if _notifications_on_rules_action_if_failed_set_ignore and (
                            failed_ignored_row_dq_res
                            or self._context.get_final_query_dq_result
                            or self._context.get_final_agg_dq_result
                            or self._context.get_source_query_dq_result
                            or self._context.get_source_agg_dq_result
                        ):
                            _ignore_rules_result.extend(
                                [
                                    failed_ignored_row_dq_res,
                                    self._context.get_final_query_dq_result,
                                    self._context.get_final_agg_dq_result,
                                    self._context.get_source_query_dq_result,
                                    self._context.get_source_agg_dq_result,
                                ]
                            )

                        if _ignore_rules_result:
                            flattened_ignore_rules_result: List[Dict[str, str]] = [
                                item for sublist in filter(None, _ignore_rules_result) for item in sublist
                            ]
                            self._notification.notify_on_ignore_rules(flattened_ignore_rules_result)

                        # TODO if row_dq is False and source_agg/source_query is True then we need to write the
                        #  dataframe into the target table
                        if write_to_table:
                            _log.info("Writing into the final table started")
                            self._writer.save_df_as_table(
                                _row_dq_df,
                                f"{table_name}",
                                self._context.get_target_and_error_table_writer_config,
                            )
                            _log.info("Writing into the final table ended")

                    else:
                        raise SparkExpectationsDataframeNotReturnedException(
                            "error occurred while processing spark "
                            "expectations due to given dataframe is not type of dataframe"
                        )
                    # self.spark.catalog.clearCache()

                    return _row_dq_df

                except Exception as e:
                    raise SparkExpectationsMiscException(f"error occurred while processing spark expectations {e}")

            return wrapper

        return _except


class WrappedDataFrameWriter:
    """
    A builder pattern class that mimics the functions of PySpark's DataFrameWriter.

    This class allows for chaining methods to set configurations like mode, format, partitioning,
    options, and bucketing. It does not require a DataFrame object and is designed purely to
    collect and return configurations.

    Example usage:
    --------------
    writer = WrappedDataFrameWriter().mode("overwrite")\
                                   .format("parquet")\
                                   .partitionBy("date", "region")\
                                   .option("compression", "gzip")\
                                   .options(path="/path/to/output", inferSchema="true")\
                                   .bucketBy(4, "country", "city")

    config = writer.build()
    print(config)

    Attributes:
    -----------
    _mode : str
        The mode for writing (e.g., "overwrite", "append").
    _format : str
        The format for writing (e.g., "parquet", "csv").
    _partition_by : list
        Columns by which the data should be partitioned.
    _options : dict
        Additional options for writing.
    _bucket_by : dict
        Configuration for bucketing, including number of buckets and columns.
    """

    def __init__(self) -> None:
        self._mode: Optional[str] = None
        self._format: Optional[str] = None
        self._partition_by: list = []
        self._options: dict[str, str] = {}
        self._bucket_by: Dict[str, Union[int, tuple]] = {}
        self._sort_by: list = []

    def mode(self, saveMode: str) -> "WrappedDataFrameWriter":  # noqa: N803
        """Set the mode for writing."""
        self._mode = saveMode
        return self

    def format(self, source: str) -> "WrappedDataFrameWriter":
        """Set the format for writing."""
        self._format = source
        return self

    def partitionBy(self, *columns: str) -> "WrappedDataFrameWriter":  # noqa: N802
        """Set the columns by which the data should be partitioned."""
        self._partition_by.extend(columns)
        return self

    def option(self, key: str, value: str) -> "WrappedDataFrameWriter":
        """Set a single option for writing."""
        self._options[key] = value
        return self

    def options(self, **options: str) -> "WrappedDataFrameWriter":
        """Set multiple options for writing."""
        self._options.update(options)
        return self

    def bucketBy(self, num_buckets: int, *columns: str) -> "WrappedDataFrameWriter":  # noqa: N802
        """Set the configuration for bucketing."""
        self._bucket_by["num_buckets"] = num_buckets
        self._bucket_by["columns"] = columns
        return self

    def sortBy(self, *columns: str) -> "WrappedDataFrameWriter":  # noqa: N802
        """Set the configuration for bucketing."""
        self._sort_by.extend(columns)
        return self

    def build(self) -> Dict[str, Union[str, list, dict, tuple, int, None]]:
        """Return the collected configurations."""
        if self._format is not None and self._format.lower() == "delta":
            if self._bucket_by is not None and self._bucket_by:
                raise SparkExpectationsMiscException("Bucketing is not supported for delta tables yet...")

        return {
            "mode": self._mode,
            "format": self._format,
            "partitionBy": self._partition_by,
            "options": self._options,
            "bucketBy": self._bucket_by,
            "sortBy": self._sort_by,
        }

        # config = {}
        #
        # if cls._mode:
        #     config["mode"] = cls._mode
        # if cls._format:
        #     config["format"] = cls._format
        # if cls._partition_by:
        #     config["partitionBy"] = cls._partition_by
        # if cls._options:
        #     config["options"] = cls._options
        # if cls._bucket_by:
        #     config["bucketBy"] = cls._bucket_by
        # if cls._sort_by:
        #     config["sortBy"] = cls._sort_by
        #
        # return config
