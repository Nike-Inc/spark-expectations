import functools
import importlib
from dataclasses import dataclass
from typing import Dict, Optional, Any, Union, List, TypeAlias, overload, Callable, Tuple

from pyspark.version import __version__ as spark_version
from pyspark import StorageLevel
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql.functions import md5, concat_ws, col, lit, coalesce, trim


from spark_expectations import _log
from spark_expectations.config.user_config import Constants as user_config
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core.exceptions import (
    SparkExpectationsMiscException,
    SparkExpectationsDataframeNotReturnedException,
    SparkExpectationsUserInputOrConfigInvalidException,
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
from spark_expectations.utils.validate_rules import SparkExpectationsValidateRules
from spark_expectations.core import get_config_dict


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
    DataFrame: TypeAlias = Union[sql.DataFrame, ConnectDataFrame]
    SparkSession: TypeAlias = Union[sql.SparkSession, ConnectSparkSession]
else:
    DataFrame: TypeAlias = sql.DataFrame  # type: ignore
    SparkSession: TypeAlias = sql.SparkSession  # type: ignore

# Type alias for the DQ process function returned by execute_dq_process
DQProcessFunc: TypeAlias = Callable[..., Tuple["DataFrame", Optional[List[Dict[str, str]]], int, str]]

__all__ = [
    "SparkExpectations",
    "WrappedDataFrameWriter",
    "WrappedDataFrameStreamWriter"
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
    rules_df: "DataFrame"
    stats_table: str
    target_and_error_table_writer: Union["WrappedDataFrameWriter", "WrappedDataFrameStreamWriter"]
    stats_table_writer: "WrappedDataFrameWriter"
    debugger: bool = False
    stats_streaming_options: Optional[Dict[str, Union[str, bool]]] = None

    def _add_hash_columns(self, df: "DataFrame") -> "DataFrame":
        """
        Add hash columns to the rules DataFrame.

        Args:
            df: The rules DataFrame

        Returns:
            DataFrame with id_hash and expectation_hash columns added
        """
        return df.withColumn(
            "id_hash",
            md5(concat_ws("|", 
                coalesce(trim(col("product_id")), lit("")), 
                coalesce(trim(col("table_name")), lit("")), 
                coalesce(trim(col("rule")), lit("")),
                coalesce(trim(col("rule_type")), lit(""))))
        ).withColumn(
            "expectation_hash",
            md5(col("expectation"))
        )
    
    def _check_missing_columns(self, required_columns: set) -> None:
        """
        Validate that the rules DataFrame contains all required columns.

        Raises:
            SparkExpectationsUserInputOrConfigInvalidException: If any required columns are
                missing from rules_df.
        """
        actual_columns = set(self.rules_df.columns)
        missing_columns = required_columns - actual_columns
        
        if missing_columns:
            raise SparkExpectationsUserInputOrConfigInvalidException(
                f"rules_df is missing required columns: {sorted(missing_columns)}"
            )

    def _check_null_or_empty_values(self, columns: set) -> None:
        """
        Check that the specified columns don't have NULL or empty string values.

        Args:
            columns: Set of column names to validate.

        Raises:
            SparkExpectationsUserInputOrConfigInvalidException: If any columns contain
                NULL or empty string values.
        """
        null_or_empty_check_expr = [
            F.sum(
                F.when(
                    F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1
                ).otherwise(0)
            ).alias(c)
            for c in columns if c in self.rules_df.columns
        ]
        invalid_counts = self.rules_df.select(null_or_empty_check_expr).collect()[0]
        columns_with_invalid = [c for c in columns if c in invalid_counts and invalid_counts[c] > 0]
        
        if columns_with_invalid:
            raise SparkExpectationsUserInputOrConfigInvalidException(
                f"rules_df contains NULL or empty values in required columns: {sorted(columns_with_invalid)}"
            )

    def _validate_rules(self) -> None:
        """
        Validate the rules DataFrame for required structure and content.

        Raises:
            SparkExpectationsUserInputOrConfigInvalidException: If the rules DataFrame is empty
                or fails other validation checks.
        """
        if self.rules_df.isEmpty():
            raise SparkExpectationsUserInputOrConfigInvalidException(
                "rules_df is empty. At least one rule must be provided."
            )
        
        required_columns = {"product_id", "table_name", "rule", "rule_type"}
        self._check_missing_columns(required_columns)
        self._check_null_or_empty_values(required_columns)

    def _check_serverless_config(
        self, 
        user_conf: Optional[Dict[str, Union[str, int, bool, Dict[str, str]]]]
    ) -> None:
        """
        Check if running in serverless mode and persist rules DataFrame if not.

        In non-serverless environments, the rules DataFrame is persisted to
        MEMORY_AND_DISK storage level to optimize repeated access during
        data quality processing.

        Args:
            user_conf: User configuration dictionary that may contain serverless setting.
        """
        is_serverless = user_conf.get("spark.expectations.is.serverless", False) if user_conf else False
        if not is_serverless:
            self.rules_df = self.rules_df.persist(StorageLevel.MEMORY_AND_DISK)

    def _build_config_dicts(
        self,
        user_conf: Optional[Dict[str, Union[str, int, bool, Dict[str, str]]]]
    ) -> Tuple[Dict[str, Union[str, int, bool, Dict[str, str]]], Dict[str, Any]]:
        """
        Build notification and stats streaming configuration dictionaries.

        Merges default configurations with user-provided configurations to create
        the final notification and stats streaming settings.

        Args:
            user_conf: User configuration dictionary to merge with defaults.

        Returns:
            A tuple containing:
                - notification_dict: Configuration for notifications including
                  email, Slack, and other notification settings.
                - se_stats_streaming_dict: Configuration for stats streaming
                  including Kafka settings.
        """
        _default_notification_dict, _default_stats_streaming_dict = get_config_dict(self.spark, user_conf)
        _default_notification_dict[
            user_config.querydq_output_custom_table_name
        ] = f"{self.stats_table}_querydq_output"

        _notification_dict: Dict[str, Union[str, int, bool, Dict[str, str]]] = (
            {**_default_notification_dict, **user_conf} if user_conf else _default_notification_dict
        )

        _se_stats_streaming_dict: Dict[str, Any] = (
            {**self.stats_streaming_options} if self.stats_streaming_options else _default_stats_streaming_dict
        )
        return _notification_dict, _se_stats_streaming_dict

    def _set_kafka_configs(self, se_stats_streaming_dict: Dict[str, Any]) -> None:
        """
        Configure Kafka settings for stats streaming in the context.

        Sets up Kafka-related configurations including custom config enablement
        and bootstrap server settings based on the provided streaming dictionary.

        Args:
            se_stats_streaming_dict: Dictionary containing Kafka streaming
                configuration options such as custom config enable flag and
                bootstrap server address.
        """
        enable_kafka_custom_config = se_stats_streaming_dict.get('se.streaming.stats.kafka.custom.config.enable', False)
        self._context.set_se_streaming_stats_kafka_custom_config_enable(
            enable_kafka_custom_config if isinstance(enable_kafka_custom_config, bool) else False
        )

        if 'se.streaming.stats.kafka.bootstrap.server' in se_stats_streaming_dict:
            self._context.set_se_streaming_stats_kafka_bootstrap_server(str(se_stats_streaming_dict['se.streaming.stats.kafka.bootstrap.server']))

    def _set_notification_configs(self, notification_dict: Dict[str, Union[str, int, bool, Dict[str, str]]]) -> None:
        """
        Configure notification-related settings in the context.

        Sets up error table enablement, DQ rules parameters, and job metadata
        based on the provided notification configuration dictionary.

        Args:
            notification_dict: Dictionary containing notification configuration
                options including error table settings, DQ rules parameters,
                and job metadata.
        """
        enable_error_table = notification_dict.get(user_config.se_enable_error_table, True)
        self._context.set_se_enable_error_table(
            enable_error_table if isinstance(enable_error_table, bool) else True
        )

        dq_rules_params = notification_dict.get(user_config.se_dq_rules_params, {})
        self._context.set_dq_rules_params(dq_rules_params if isinstance(dq_rules_params, dict) else {})

        self._context.set_se_job_metadata(notification_dict.get(user_config.se_job_metadata))

    def _set_agg_query_detailed_stats(self, notification_dict: Dict[str, Union[str, int, bool, Dict[str, str]]]) -> None:
        """
        Configure detailed statistics settings for aggregate and query DQ.

        Enables detailed result collection for aggregate DQ and/or query DQ
        based on the notification configuration. When enabled, sets up the
        custom table name for storing detailed query DQ output.

        Args:
            notification_dict: Dictionary containing notification configuration
                options including flags for enabling detailed aggregate and
                query DQ results, and the custom table name for query DQ output.
        """
        _agg_dq_detailed_stats: bool = (
            bool(notification_dict[user_config.se_enable_agg_dq_detailed_result])
            if isinstance(
                notification_dict[user_config.se_enable_agg_dq_detailed_result],
                bool,
            )
            else False
        )

        _query_dq_detailed_stats: bool = (
            bool(notification_dict[user_config.se_enable_query_dq_detailed_result])
            if isinstance(
                notification_dict[user_config.se_enable_query_dq_detailed_result],
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
                str(notification_dict[user_config.querydq_output_custom_table_name])
            )

    def _get_configs_notification_context(
        self,
        notification_dict: Dict[str, Union[str, int, bool, Dict[str, str]]],
        se_stats_streaming_dict: Dict[str, Any]
    ) -> Tuple[bool, bool, int]:
        """
        Configure notification context settings and extract threshold parameters.

        Sets up notification triggers (on start, completion, fail) and streaming
        stats configuration in the context. Also extracts and returns error
        threshold-related settings for use in the DQ processing workflow.

        Args:
            notification_dict: Dictionary containing notification configuration
                options including triggers for start, completion, and failure
                notifications, job metadata, and error threshold settings.
            se_stats_streaming_dict: Dictionary containing stats streaming
                configuration for Kafka and other streaming options.

        Returns:
            A tuple containing:
                - notification_on_error_drop_exceeds_threshold_breach: Whether to
                  notify when error drop exceeds threshold.
                - notifications_on_rules_action_if_failed_set_ignore: Whether to
                  notify on rules with action_if_failed set to ignore.
                - error_drop_threshold: The threshold percentage for error drops.
        """
        _notification_on_start: bool = (
            bool(notification_dict[user_config.se_notifications_on_start])
            if isinstance(
                notification_dict[user_config.se_notifications_on_start],
                bool,
            )
            else False
        )
        _notification_on_completion: bool = (
            bool(notification_dict[user_config.se_notifications_on_completion])
            if isinstance(
                notification_dict[user_config.se_notifications_on_completion],
                bool,
            )
            else False
        )
        _notification_on_fail: bool = (
            bool(notification_dict[user_config.se_notifications_on_fail])
            if isinstance(
                notification_dict[user_config.se_notifications_on_fail],
                bool,
            )
            else False
        )
        _notification_on_error_drop_exceeds_threshold_breach: bool = (
            bool(notification_dict[user_config.se_notifications_on_error_drop_exceeds_threshold_breach])
            if isinstance(
                notification_dict[user_config.se_notifications_on_error_drop_exceeds_threshold_breach],
                bool,
            )
            else False
        )
        _notifications_on_rules_action_if_failed_set_ignore: bool = (
            bool(notification_dict[user_config.se_notifications_on_rules_action_if_failed_set_ignore])
            if isinstance(
                notification_dict[user_config.se_notifications_on_rules_action_if_failed_set_ignore],
                bool,
            )
            else False
        )

        _job_metadata: Optional[str] = (
            str(notification_dict[user_config.se_job_metadata])
            if isinstance(notification_dict[user_config.se_job_metadata], str)
            else None
        )

        notifications_on_error_drop_threshold = notification_dict.get(
            user_config.se_notifications_on_error_drop_threshold, 100
        )
        _error_drop_threshold: int = (
            notifications_on_error_drop_threshold if isinstance(notifications_on_error_drop_threshold, int) else 100
        )

        min_priority_slack = user_config.se_notifications_min_priority_slack

        self.reader.set_notification_param(notification_dict)
        self._context.set_notification_on_start(_notification_on_start)
        self._context.set_notification_on_completion(_notification_on_completion)
        self._context.set_notification_on_fail(_notification_on_fail)

        self._context.set_se_streaming_stats_dict(se_stats_streaming_dict)
        self._context.set_job_metadata(_job_metadata)
        self._context.set_min_priority_slack(
            str(notification_dict[min_priority_slack])
        )
        return (_notification_on_error_drop_exceeds_threshold_breach, _notifications_on_rules_action_if_failed_set_ignore, _error_drop_threshold)

    def _check_invalid_rules(self, df: "DataFrame", rules: List[Dict]) -> None:
        """
        Validate rules and log any invalid ones.

        Args:
            df: The DataFrame to validate rules against.
            rules: List of rule dictionaries to validate.
        """
        invalid_results = SparkExpectationsValidateRules.validate_expectations(
            df=df,
            rules=rules,
            spark=self.spark,
        )
        if invalid_results:
            failed_rules = [
                result.rule.get("rule") if result.rule else "unknown"
                for results_list in invalid_results.values()
                for result in results_list
            ]
            # pylint: disable=logging-too-many-args
            _log.warning(
                "Some rules failed validation: %s. "
                "Check earlier log messages for details on each invalid rule.",
                failed_rules
            )
        else:
            _log.info("Validation for rules completed successfully - all rules are valid")

    def _init_default_values(
        self,
        input_count: int,
        error_drop_threshold: int,
        expectations: Dict[str, List[Dict[str, str]]]
    ) -> None:
        """
        Initialize context variables with default values before each DQ run.

        Resets all DQ status flags, counts, results, and timing variables to their
        default states to ensure a clean slate for each data quality processing run.

        Args:
            input_count: The number of input records in the DataFrame.
            error_drop_threshold: The threshold percentage for error drops.
            expectations: Dictionary containing the DQ expectations/rules to be applied.
        """
        _log.info("initialize variable with default values before next run")
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
        self._context.set_rules_exceeds_threshold()
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

        self._context.set_input_count(input_count)
        self._context.set_error_drop_threshold(error_drop_threshold)
    
    def _use_temp_table(self, table_name: str, df: "DataFrame") -> "DataFrame":
        """
        Write DataFrame to a temporary table and read it back to break the Spark plan.

        This method is used to materialize intermediate results by writing the DataFrame
        to a temporary table and reading it back, which can help optimize complex Spark
        execution plans.

        Args:
            table_name: The base name for the temporary table (will be suffixed with '_temp').
            df: The DataFrame to write to the temporary table.

        Returns:
            A new DataFrame read from the temporary table with original column ordering.
        """
        _log.info("Dropping temp table started")
        self.spark.sql(f"drop table if exists {table_name}_temp")  # type: ignore
        _log.info("Dropping temp table completed")
        _log.info("Writing to temp table started")
        source_columns = df.columns
        self._writer.save_df_as_table(
            df,
            f"{table_name}_temp",
            self._context.get_target_and_error_table_writer_config,
        )
        _log.info("Read from temp table started")
        _df = self.spark.sql(f"select * from {table_name}_temp")  # type: ignore
        _df = _df.select(source_columns)
        _log.info("Read from temp table completed")
        return _df
    
    def _check_streaming_agg_query_dq(
        self,
        df: "DataFrame",
        source_agg_dq: bool,
        source_query_dq: bool
    ) -> None:
        """
        Log warnings if aggregate or query DQ rules are provided for streaming DataFrames.

        Streaming DataFrames only support row-level DQ checks. This method logs
        informational messages when aggregate or query DQ expectations are provided
        but cannot be applied to streaming data.

        Args:
            df: The DataFrame to check for streaming status.
            source_agg_dq: Whether source aggregate DQ rules are enabled.
            source_query_dq: Whether source query DQ rules are enabled.
        """
        if df.isStreaming:
            _log.info("Streaming dataframe detected. Only row_dq checks applicable.")
            if source_agg_dq is True:
                _log.info("agg_dq expectations provided. Not applicable for streaming dataframe.")
            if source_query_dq:
                _log.info("query_dq expectations provided. Not applicable for streaming dataframe.")
    
    def _run_source_agg_dq_batch(
        self,
        df: "DataFrame",
        func_process: DQProcessFunc
    ) -> None:
        """
        Execute aggregate-level data quality rules on the source DataFrame.

        Processes aggregate DQ expectations on the source data before any row-level
        transformations. Updates the context with start/end times and status.

        Args:
            df: The source DataFrame to validate.
            func_process: The DQ process function that executes the validation rules.
        """
        _log.info(
            "started processing data quality rules for agg level expectations on source dataframe"
        )
        self._context.set_source_agg_dq_status("Failed")
        self._context.set_source_agg_dq_start_time()
        (
            _source_dq_df,
            _dq_source_agg_results,
            _,
            status,
        ) = func_process(
            df,
            self._context.get_agg_dq_rule_type_name,
            source_agg_dq_flag=True,
        )
        self._context.set_source_agg_dq_status(status)
        self._context.set_source_agg_dq_end_time()

        _log.info(
            "ended processing data quality rules for agg level expectations on source dataframe"
        )

    def _run_source_query_dq_batch(
        self,
        df: "DataFrame",
        func_process: DQProcessFunc
    ) -> None:
        """
        Execute query-level data quality rules on the source DataFrame.

        Processes query DQ expectations on the source data before any row-level
        transformations. Query DQ rules can execute custom SQL queries for validation.
        Updates the context with start/end times and status.

        Args:
            df: The source DataFrame to validate.
            func_process: The DQ process function that executes the validation rules.
        """
        _log.info(
            "started processing data quality rules for query level expectations on source dataframe"
        )
        self._context.set_source_query_dq_status("Failed")
        self._context.set_source_query_dq_start_time()

        (
            _source_query_dq_df,
            _dq_source_query_results,
            _,
            status,
        ) = func_process(
            df,
            self._context.get_query_dq_rule_type_name,
            source_query_dq_flag=True,
        )
        self._context.set_source_query_dq_status(status)
        self._context.set_source_query_dq_end_time()
        _log.info(
            "ended processing data quality rules for query level expectations on source dataframe"
        )

    def _run_row_dq(
        self,
        df: "DataFrame",
        func_process: DQProcessFunc,
        target_table_view: str
    ) -> Tuple["DataFrame", int, int]:
        """
        Execute row-level data quality rules on the DataFrame.

        Processes row DQ expectations which validate individual records against
        defined rules. Creates a temporary view for subsequent query/aggregate DQ
        processing and updates context with counts and status.

        Args:
            df: The DataFrame to validate at row level.
            func_process: The DQ process function that executes the validation rules.
            target_table_view: Name of the temporary view to create for the result DataFrame.

        Returns:
            A tuple containing:
                - The DataFrame after row DQ processing (with DQ columns added).
                - The count of error/failed records.
                - The count of output records (0 for streaming DataFrames).
        """
        _log.info("started processing data quality rules for row level expectations")
        self._context.set_row_dq_status("Failed")
        self._context.set_row_dq_start_time()
    
        (_row_dq_df, _, _error_count, status) = func_process(
            df,
            self._context.get_row_dq_rule_type_name,
            row_dq_flag=True,
        )
        self._context.set_error_count(_error_count)

        _row_dq_df.createOrReplaceTempView(target_table_view)

        _output_count = _row_dq_df.count() if not _row_dq_df.isStreaming else 0
        self._context.set_output_count(_output_count)

        self._context.set_row_dq_status(status)
        self._context.set_row_dq_end_time()

        return _row_dq_df, _error_count, _output_count

    def _call_row_dq_notifications(
        self,
        notification_on_error_drop_exceeds_threshold_breach: bool,
        error_drop_threshold: int,
        notifications_on_rules_action_if_failed_set_ignore: bool
    ) -> List[Dict[str, Any]]:
        """
        Send notifications based on row DQ results and error thresholds.

        Triggers notifications when error drop exceeds the configured threshold
        and collects failed rules with 'ignore' action for potential notification.

        Args:
            notification_on_error_drop_exceeds_threshold_breach: Whether to notify
                when error drop exceeds threshold.
            error_drop_threshold: The threshold percentage for triggering notifications.
            notifications_on_rules_action_if_failed_set_ignore: Whether to collect
                and return rules with action_if_failed set to 'ignore'.

        Returns:
            List of failed rule dictionaries with action_if_failed='ignore' that
            have failed_row_count > 0, or empty list if conditions aren't met.
        """
        if (
            notification_on_error_drop_exceeds_threshold_breach is True
            and (100 - self._context.get_output_percentage) >= error_drop_threshold
        ):
            self._notification.notify_on_exceeds_of_error_threshold()

        if (
            notifications_on_rules_action_if_failed_set_ignore is True
            and isinstance(self._context.get_error_percentage, (int, float))
            and self._context.get_error_percentage >= error_drop_threshold
        ):
            failed_ignored_row_dq_res = [
                rule
                for rule in self._context.get_summarized_row_dq_res or []
                if rule["action_if_failed"] == "ignore"
                and isinstance(rule["failed_row_count"], int)
                and rule["failed_row_count"] > 0
            ]
            return failed_ignored_row_dq_res
        else:
            return []

    def _run_target_agg_dq_batch(
        self,
        func_process: DQProcessFunc,
        row_dq_df: "DataFrame",
        error_count: int,
        output_count: int
    ) -> None:
        """
        Execute aggregate-level data quality rules on the final/target DataFrame.

        Processes aggregate DQ expectations on the data after row-level DQ processing.
        Uses the error and output counts from row DQ for aggregate validations.
        Updates the context with start/end times and status.

        Args:
            func_process: The DQ process function that executes the validation rules.
            row_dq_df: The DataFrame after row DQ processing.
            error_count: The count of error records from row DQ.
            output_count: The count of output records from row DQ.
        """
        _log.info(
            "started processing data quality rules for agg level expectations on final dataframe"
        )
        self._context.set_final_agg_dq_status("Failed")
        self._context.set_final_agg_dq_start_time()
    
        (
            _final_dq_df,
            _dq_final_agg_results,
            _,
            status,
        ) = func_process(
            row_dq_df,
            self._context.get_agg_dq_rule_type_name,
            final_agg_dq_flag=True,
            error_count=error_count,
            output_count=output_count,
        )
        self._context.set_final_agg_dq_status(status)
        self._context.set_final_agg_dq_end_time()
        _log.info(
            "ended processing data quality rules for agg level expectations on final dataframe"
        )

    def _run_target_query_dq_batch(
        self,
        func_process: DQProcessFunc,
        row_dq_df: "DataFrame",
        target_table_view: str,
        error_count: int,
        output_count: int
    ) -> None:
        """
        Execute query-level data quality rules on the final/target DataFrame.

        Processes query DQ expectations on the data after row-level DQ processing.
        Creates a temporary view for SQL-based query validations. Uses the error
        and output counts from row DQ for query validations. Updates the context
        with start/end times and status.

        Args:
            func_process: The DQ process function that executes the validation rules.
            row_dq_df: The DataFrame after row DQ processing.
            target_table_view: Name of the temporary view to create for query execution.
            error_count: The count of error records from row DQ.
            output_count: The count of output records from row DQ.
        """
        _log.info(
            "started processing data quality rules for query level expectations on final dataframe"
        )
        self._context.set_final_query_dq_status("Failed")
        self._context.set_final_query_dq_start_time()

        row_dq_df.createOrReplaceTempView(target_table_view)

        (
            _final_query_dq_df,
            _dq_final_query_results,
            _,
            status,
        ) = func_process(
            row_dq_df,
            self._context.get_query_dq_rule_type_name,
            final_query_dq_flag=True,
            error_count=error_count,
            output_count=output_count,
        )
        self._context.set_final_query_dq_status(status)
        self._context.set_final_query_dq_end_time()

        _log.info(
            "ended processing data quality rules for query level expectations on final dataframe"
        )

    def _check_ignore_rules_result(
        self,
        notifications_on_rules_action_if_failed_set_ignore: bool,
        failed_ignored_row_dq_res: List[Dict[str, Any]],
        ignore_rules_result: List[Optional[List[Dict[str, Any]]]]
    ) -> List[Dict[str, Any]]:
        """
        Collect and flatten all ignored rule results from various DQ stages.

        Aggregates failed rules with 'ignore' action from row DQ, source/target
        aggregate DQ, and source/target query DQ stages into a single flattened list.

        Args:
            notifications_on_rules_action_if_failed_set_ignore: Whether to collect
                ignored rules for notification.
            failed_ignored_row_dq_res: List of failed row DQ rules with ignore action.
            ignore_rules_result: Accumulator list for collecting all ignored rule results.

        Returns:
            Flattened list of all ignored rule dictionaries from all DQ stages,
            or empty list if no ignored rules exist.
        """
        if notifications_on_rules_action_if_failed_set_ignore and (
            failed_ignored_row_dq_res
            or self._context.get_final_query_dq_result
            or self._context.get_final_agg_dq_result
            or self._context.get_source_query_dq_result
            or self._context.get_source_agg_dq_result
        ):
            ignore_rules_result.extend(
                [
                    failed_ignored_row_dq_res,
                    self._context.get_final_query_dq_result,
                    self._context.get_final_agg_dq_result,
                    self._context.get_source_query_dq_result,
                    self._context.get_source_agg_dq_result,
                ]
            )
        
        if ignore_rules_result:
            flattened_ignore_rules_result: List[Dict[str, Any]] = [
                item for sublist in filter(None, ignore_rules_result) for item in sublist
            ]
            return flattened_ignore_rules_result
        else:
            return []

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

        if isinstance(self.target_and_error_table_writer, WrappedDataFrameStreamWriter):
            self._context.set_target_and_error_table_writer_type("streaming")

        self._context.set_target_and_error_table_writer_config(self.target_and_error_table_writer.build())
        self._context.set_stats_table_writer_config(self.stats_table_writer.build())
        self._context.set_detailed_stats_table_writer_config(self.stats_table_writer.build())
        self._context.set_debugger_mode(self.debugger)
        self._context.set_dq_stats_table_name(self.stats_table)
        self._context.set_dq_detailed_stats_table_name(f"{self.stats_table}_detailed")
        self._validate_rules()
        self.rules_df = self._add_hash_columns(self.rules_df)
        # self.rules_df = self.rules_df.persist(StorageLevel.MEMORY_AND_DISK)

    def with_expectations(
        self,
        target_table: str,
        write_to_table: bool = False,
        write_to_temp_table: bool = False,
        user_conf: Optional[Dict[str, Union[str, int, bool, Dict[str, str]]]] = None,
        target_table_view: Optional[str] = None,
        target_and_error_table_writer: Optional[Union["WrappedDataFrameWriter", "WrappedDataFrameStreamWriter"]] = None,
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

            self._check_serverless_config(user_conf)
            _notification_dict, _se_stats_streaming_dict = self._build_config_dicts(user_conf)
            self._set_kafka_configs(_se_stats_streaming_dict)
            self._set_notification_configs(_notification_dict)
            if target_and_error_table_writer:
                self._context.set_target_and_error_table_writer_config(target_and_error_table_writer.build())
            self._set_agg_query_detailed_stats(_notification_dict)

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

            (
                _notification_on_error_drop_exceeds_threshold_breach,
                _notifications_on_rules_action_if_failed_set_ignore,
                _error_drop_threshold
            ) = self._get_configs_notification_context(_notification_dict, _se_stats_streaming_dict)
        
            self._context.set_dq_expectations(expectations)
            self._context.set_rules_execution_settings_config(rules_execution_settings)
            self._context.set_querydq_secondary_queries(dq_queries_dict)
            
            @self._notification.send_notification_decorator
            @self._statistics_decorator.collect_stats_decorator
            @functools.wraps(func)
            def wrapper(*args: tuple, **kwargs: dict) -> DataFrame:
                try:
                    _log.info("The function dataframe is getting created")
                    _df: DataFrame = func(*args, **kwargs)
                    table_name: str = self._context.get_table_name

                    rules = [row.asDict() for row in self.rules_df.collect()]
                    self._check_invalid_rules(_df, rules)

                    _input_count = _df.count() if not _df.isStreaming else 0
                    _log.info(f"data frame input record count: {_input_count}")
                    _output_count: int = 0
                    _error_count: int = 0
                    failed_ignored_row_dq_res: List[Dict[str, Any]] = []
                    _row_dq_df: DataFrame = _df
                    _ignore_rules_result: List[Optional[List[Dict[str, Any]]]] = []

                    self._init_default_values(_input_count, _error_drop_threshold, expectations)

                    _log.info(f"Spark Expectations run id for this run: {self._context.get_run_id}")

                    if isinstance(_df, DataFrame):  # type: ignore
                        _log.info("The function dataframe is created")
                        self._context.set_table_name(table_name)
                        if write_to_temp_table:
                            _df = self._use_temp_table(table_name, _df)
                            
                        func_process = self._process.execute_dq_process(
                            _context=self._context,
                            _actions=self.actions,
                            _writer=self._writer,
                            _notification=self._notification,
                            expectations=expectations,
                            _input_count=_input_count,
                        )

                        self._check_streaming_agg_query_dq(_df, _source_agg_dq, _source_query_dq)

                        if _source_agg_dq is True and not _df.isStreaming:
                            self._run_source_agg_dq_batch(_df, func_process)
                           
                        if _source_query_dq is True and not _df.isStreaming:
                            self._run_source_query_dq_batch(_df, func_process)
                           
                        if _row_dq is True:                            
                            _row_dq_df, _error_count, _output_count = self._run_row_dq(_df, func_process, _target_table_view)
                            failed_ignored_row_dq_res = self._call_row_dq_notifications(
                                                        _notification_on_error_drop_exceeds_threshold_breach,
                                                        _error_drop_threshold,
                                                        _notifications_on_rules_action_if_failed_set_ignore
                                                    )
                        _log.info("ended processing data quality rules for row level expectations")

                        if _row_dq is True and _target_agg_dq is True and not _df.isStreaming:
                            self._run_target_agg_dq_batch(func_process, _row_dq_df, _error_count, _output_count)
                            
                        if _row_dq is True and _target_query_dq is True and not _df.isStreaming:
                            self._run_target_query_dq_batch(func_process, _row_dq_df, _target_table_view, _error_count, _output_count)
                            
                        flattened_ignore_rules_result = self._check_ignore_rules_result(_notifications_on_rules_action_if_failed_set_ignore, failed_ignored_row_dq_res, _ignore_rules_result)
                        if flattened_ignore_rules_result:
                            self._notification.notify_on_ignore_rules(flattened_ignore_rules_result)

                        streaming_query = None
                        if write_to_table:
                            _log.info("Writing into the final table started")
                            streaming_query = self._writer.save_df_as_table(
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
                        
                    return streaming_query if streaming_query is not None else _row_dq_df

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


class WrappedDataFrameStreamWriter:
    """
    A builder pattern class that mimics the functions of PySpark's DataStreamWriter.

    This class allows for chaining methods to set configurations for streaming writes like
    output mode, format, query name, trigger, options, and partitioning. It does not require
    a DataFrame object and is designed purely to collect and return configurations.

    Example usage:
    --------------
    stream_writer = WrappedDataFrameStreamWriter().outputMode("append")\
                                   .format("delta")\
                                   .queryName("my_streaming_query")\
                                   .trigger(processingTime="10 seconds")\
                                   .option("checkpointLocation", "/path/to/checkpoint")\
                                   .partitionBy("date")

    config = stream_writer.build()
    print(config)

    Attributes:
    -----------
    _output_mode : str
        The output mode for streaming (e.g., "append", "complete", "update").
    _format : str
        The format for writing (e.g., "delta", "parquet", "kafka").
    _query_name : str
        The name of the streaming query.
    _trigger : dict
        The trigger configuration (e.g., {"processingTime": "10 seconds"}).
    _partition_by : list
        Columns by which the data should be partitioned.
    _options : dict
        Additional options for writing (e.g., checkpointLocation).
    """

    def __init__(self) -> None:
        self._output_mode: Optional[str] = None
        self._format: Optional[str] = None
        self._query_name: Optional[str] = None
        self._trigger: Dict[str, str] = {}
        self._partition_by: list = []
        self._options: dict[str, str] = {}

    def outputMode(self, output_mode: str) -> "WrappedDataFrameStreamWriter":  # noqa: N802
        """Set the output mode for streaming (append, complete, or update)."""
        self._output_mode = output_mode
        return self

    def format(self, source: str) -> "WrappedDataFrameStreamWriter":
        """Set the format for writing."""
        self._format = source
        return self

    def queryName(self, query_name: str) -> "WrappedDataFrameStreamWriter":  # noqa: N802
        """Set the name of the streaming query."""
        self._query_name = query_name
        return self

    def trigger(self, **trigger_options: str) -> "WrappedDataFrameStreamWriter":
        """
        Set the trigger for the streaming query.
        
        Common trigger types:
        - processingTime: "10 seconds" (process every 10 seconds)
        - once: True (process available data once and stop)
        - continuous: "1 second" (continuous processing with 1 second checkpoints)
        """
        self._trigger.update(trigger_options)
        return self
    @overload  
    def partitionBy(self, *cols: str) -> "WrappedDataFrameStreamWriter": ... # noqa: N802
    
    @overload  # type: ignore   
    def partitionBy(self, __cols: list[str]) -> "WrappedDataFrameStreamWriter": ... # noqa: N802
    
    def partitionBy(self, *columns: str | List[str]) -> "WrappedDataFrameStreamWriter":  # noqa: N802
        """Set the columns by which the data should be partitioned."""
        # Handle case where a single list is passed: partitionBy(["col1", "col2"])
        if len(columns) == 1 and isinstance(columns[0], list):
            self._partition_by.extend(columns[0])
        else:
            # Handle case where multiple strings are passed: partitionBy("col1", "col2")
            self._partition_by.extend(columns)
        return self

    def option(self, key: str, value: str) -> "WrappedDataFrameStreamWriter":
        """Set a single option for writing."""
        self._options[key] = value
        return self

    def options(self, **options: str) -> "WrappedDataFrameStreamWriter":
        """Set multiple options for writing."""
        self._options.update(options)
        return self

    def build(self) -> Dict[str, Union[str, list, dict, None]]:
        """Return the collected configurations."""
        return {
            "outputMode": self._output_mode,
            "format": self._format,
            "queryName": self._query_name,
            "trigger": self._trigger if self._trigger else None,
            "partitionBy": self._partition_by,
            "options": self._options,
        }