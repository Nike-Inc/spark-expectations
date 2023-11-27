# pylint: disable=too-many-lines
import os
from datetime import timezone
from datetime import datetime
from dataclasses import dataclass
from uuid import uuid1
from typing import Dict, Optional, List
from pyspark.sql import DataFrame, SparkSession
from spark_expectations.config.user_config import Constants as user_config
from spark_expectations.core.exceptions import SparkExpectationsMiscException


# TODO: Add exceptions to follow standardized naming conventions for _run_id, product_id and _dq_stats_table_name in
#       the future
@dataclass
class SparkExpectationsContext:
    """
    This class provides the context for SparkExpectations
    """

    product_id: str
    spark: SparkSession

    def __post_init__(self) -> None:
        self._run_id: str = f"{self.product_id}_{uuid1()}"
        self._run_date: str = self.set_run_date()
        self._dq_stats_table_name: Optional[str] = None
        self._final_table_name: Optional[str] = None
        self._error_table_name: Optional[str] = None
        self._row_dq_rule_type_name: str = "row_dq"
        self._agg_dq_rule_type_name: str = "agg_dq"
        self._query_dq_rule_type_name: str = "query_dq"
        self._row_dq_status: str = "Skipped"
        self._source_agg_dq_status: str = "Skipped"
        self._final_agg_dq_status: str = "Skipped"
        self._source_query_dq_status: str = "Skipped"
        self._final_query_dq_status: str = "Skipped"
        self._dq_run_status: str = "Failed"

        # above configuration variable value has to be set to python
        self._dq_project_env_name = "spark_expectations"
        self._dq_config_file_name = "dq_spark_expectations_config.ini"
        self._dq_config_abs_path: Optional[str] = None

        self._enable_mail: bool = False
        self._to_mail: Optional[str] = None
        self._mail_subject: Optional[str] = None
        self._mail_from: Optional[str] = None
        self._mail_smtp_server: str
        self._mail_smtp_port: int

        self._enable_slack: bool = False
        self._slack_webhook_url: Optional[str] = None

        self._enable_teams: bool = False
        self._teams_webhook_url: Optional[str] = None

        self._table_name: Optional[str] = None
        self._input_count: int = 0
        self._error_count: int = 0
        self._output_count: int = 0

        self._env: Optional[str] = None

        self._se_streaming_stats_topic_name: str = "dq-sparkexpectations-stats"
        self._se_streaming_row_dq_res_topic_name: str = (
            "dq-sparkexpectations-row-dq-results"
        )

        self._source_agg_dq_result: Optional[List[Dict[str, str]]] = None
        self._final_agg_dq_result: Optional[List[Dict[str, str]]] = None
        self._source_query_dq_result: Optional[List[Dict[str, str]]] = None
        self._final_query_dq_result: Optional[List[Dict[str, str]]] = None

        self._notification_on_start: bool = False
        self._notification_on_completion: bool = False
        self._notification_on_fail: bool = False
        self._error_drop_threshold: int = 100

        self._cerberus_url: str = "your_cerberus_url"
        self._cerberus_cred_path: str = "your_cerberus_sdb_path"
        self._cerberus_token: Optional[str] = os.environ.get(
            "DQ_SPARK_EXPECTATIONS_CERBERUS_TOKEN"
        )

        self._se_streaming_stats_dict: Dict[str, str]
        self._enable_se_streaming: bool = False
        self._se_streaming_secret_env: str = ""

        self._debugger_mode: bool = False
        self._supported_df_query_dq: DataFrame = self.set_supported_df_query_dq()

        self._source_agg_dq_start_time: Optional[datetime] = None
        self._final_agg_dq_start_time: Optional[datetime] = None
        self._source_query_dq_start_time: Optional[datetime] = None
        self._final_query_dq_start_time: Optional[datetime] = None
        self._row_dq_start_time: Optional[datetime] = None
        self._dq_start_time: Optional[datetime] = None

        self._source_agg_dq_end_time: Optional[datetime] = None
        self._final_agg_dq_end_time: Optional[datetime] = None
        self._source_query_dq_end_time: Optional[datetime] = None
        self._final_query_dq_end_time: Optional[datetime] = None
        self._row_dq_end_time: Optional[datetime] = None
        self._dq_end_time: Optional[datetime] = None

        self._run_id_name = "meta_dq_run_id"
        self._run_date_name = "meta_dq_run_date"
        self._run_date_time_name = "meta_dq_run_datetime"

        self._num_row_dq_rules: int = 0
        self._num_agg_dq_rules: Dict[str, int] = {
            "num_agg_dq_rules": 0,
            "num_source_agg_dq_rules": 0,
            "num_final_agg_dq_rules": 0,
        }
        self._num_query_dq_rules: Dict[str, int] = {
            "num_query_dq_rules": 0,
            "num_source_query_dq_rules": 0,
            "num_final_query_dq_rules": 0,
        }
        self._num_dq_rules: int = 0
        self._summarised_row_dq_res: Optional[List[Dict[str, str]]] = None
        self._rules_error_per: Optional[List[dict]] = None

        self._target_and_error_table_writer_config: dict = {}
        self._stats_table_writer_config: dict = {}

    @property
    def get_run_id(self) -> str:
        """
        Get run_id for the instance of spark-expectations class

        Returns:
            str: returns the run_id
        """
        return self._run_id

    @property
    def get_run_date(self) -> str:
        """
        Get run_date for the instance of the spark-expectations class

        Returns:
            str: returns the run_date

        """
        return self._run_date

    def set_dq_stats_table_name(self, dq_stats_table_name: str) -> None:
        self._dq_stats_table_name = dq_stats_table_name

    @property
    def get_dq_stats_table_name(self) -> str:
        """
        Get dq_stats_table_name to which the final stats of the dq job will be written into

        Returns:
            str: returns the dq_stats_table_name
        """
        if self._dq_stats_table_name:
            return self._dq_stats_table_name
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_dq_stats_table_name' before 
            accessing it"""
        )

    def set_final_table_name(self, final_table_name: str) -> None:
        self._final_table_name = final_table_name

    @property
    def get_final_table_name(self) -> str:
        """
        Get dq_stats_table_name to which the final stats of the dq job will be written into

        Returns:
            str: returns the dq_stats_table_name
        """
        if self._final_table_name:
            return self._final_table_name
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_final_table_name' before 
            accessing it"""
        )

    def set_error_table_name(self, error_table_name: str) -> None:
        self._error_table_name = error_table_name

    @property
    def get_error_table_name(self) -> str:
        """
        Get dq_stats_table_name to which the final stats of the dq job will be written into

        Returns:
            str: returns the dq_stats_table_name
        """
        if self._error_table_name:
            return self._error_table_name
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_error_table_name' before 
            accessing it"""
        )

    @staticmethod
    def set_run_date() -> str:
        """
        This function is used to generate the current datatime in UTC

        Returns:
            str: Returns the current utc datatime in the format - "%Y-%m-%d %H:%M:%S"

        """
        current_datetime: datetime = datetime.now(timezone.utc)
        return current_datetime.replace(tzinfo=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    @property
    def get_row_dq_rule_type_name(self) -> str:
        """
        This function is used to get row data quality rule type name

        Returns:
             str: Returns _row_dq_rule_type_name"

        """
        if self._row_dq_rule_type_name:
            return self._row_dq_rule_type_name
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_row_dq_rule_type_name' before 
            accessing it"""
        )

    @property
    def get_agg_dq_rule_type_name(self) -> str:
        """
        This function is used to get aggregation data quality rule type name

        Returns:
            str: Returns _agg_dq_rule_type_name"

        """
        if self._agg_dq_rule_type_name:
            return self._agg_dq_rule_type_name
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_agg_dq_rule_type_name' before 
            accessing it"""
        )

    @property
    def get_query_dq_rule_type_name(self) -> str:
        """
        This function is used to get query data quality rule type name

        Returns:
            str: Returns _query_dq_rule_type_name"

        """
        if self._query_dq_rule_type_name:
            return self._query_dq_rule_type_name
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_query_dq_rule_type_name' before 
            accessing it"""
        )

    def set_row_dq_status(self, row_dq_status: str = "Skipped") -> None:
        self._row_dq_status = row_dq_status

    @property
    def get_row_dq_status(self) -> str:
        """
        This function is used to get row data quality status

        Returns:
            str: Returns _row_dq_status"

        """
        if self._row_dq_status:
            return self._row_dq_status
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_row_dq_status' before 
            accessing it"""
        )

    def set_source_agg_dq_status(self, source_agg_dq_status: str = "Skipped") -> None:
        self._source_agg_dq_status = source_agg_dq_status

    @property
    def get_source_agg_dq_status(self) -> str:
        """
        This function is used to get source aggregation data quality status

        Returns:
            str: Returns _source_agg_dq_status"

        """
        if self._source_agg_dq_status:
            return self._source_agg_dq_status
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_source_agg_dq_status' before 
            accessing it"""
        )

    def set_final_agg_dq_status(self, final_agg_dq_status: str = "Skipped") -> None:
        self._final_agg_dq_status = final_agg_dq_status

    @property
    def get_final_agg_dq_status(self) -> str:
        """
        This function is used to get final aggregation data quality status

        Returns:
            str: Returns _final_agg_dq_status"

        """
        if self._final_agg_dq_status:
            return self._final_agg_dq_status
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_final_agg_dq_status' before 
            accessing it"""
        )

    def set_source_query_dq_status(
        self, source_query_dq_status: str = "Skipped"
    ) -> None:
        self._source_query_dq_status = source_query_dq_status

    @property
    def get_source_query_dq_status(self) -> str:
        """
        This function is used to get source query data quality status

        Returns:
            str: Returns _source_query_dq_status"

        """
        if self._source_query_dq_status:
            return self._source_query_dq_status
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_source_query_dq_status' before 
            accessing it"""
        )

    def set_final_query_dq_status(self, final_query_dq_status: str = "Skipped") -> None:
        self._final_query_dq_status = final_query_dq_status

    @property
    def get_final_query_dq_status(self) -> str:
        """
        This function is used to get final query dq  data quality status

        Returns:
            str: Returns _final_query_dq_status"

        """
        if self._final_query_dq_status:
            return self._final_query_dq_status
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_final_query_dq_status' before 
            accessing it"""
        )

    def set_dq_run_status(self, dq_run_status: str = "Failed") -> None:
        self._dq_run_status = dq_run_status

    @property
    def get_dq_run_status(self) -> str:
        """
        This function is used to get data quality pipeline status

        Returns:
            str: Returns _dq_status"

        """
        if self._dq_run_status:
            return self._dq_run_status
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_dq_run_status' before 
            accessing it"""
        )

    @property
    def get_config_file_path(self) -> str:
        """
        This function returns config file abs path
        Returns:
            str: Returns _config_file_path(str)

        """
        if self._dq_config_abs_path:
            return self._dq_config_abs_path

        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_dq_config_abs_path' before
            accessing it"""
        )

    def set_mail_smtp_server(self, mail_smtp_server: str) -> None:
        self._mail_smtp_server = mail_smtp_server

    @property
    def get_mail_smtp_server(self) -> str:
        """
        This functions returns smtp server host
        Returns:
            str: returns _mail_smtp_server

        """
        if self._mail_smtp_server:
            return self._mail_smtp_server

        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_mail_smtp_server' before 
            accessing it"""
        )

    def set_mail_smtp_port(self, mail_smtp_port: int) -> None:
        self._mail_smtp_port = mail_smtp_port

    @property
    def get_mail_smtp_port(self) -> int:
        """
        This functions returns smtp port
        Returns:
            int: returns _mail_smtp_server port

        """
        if self._mail_smtp_port:
            return self._mail_smtp_port

        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_mail_smtp_port' before 
            accessing it"""
        )

    def set_enable_mail(self, enable_mail: bool) -> None:
        self._enable_mail = bool(enable_mail)

    @property
    def get_enable_mail(self) -> bool:
        """
        This function return whether mail notification to enable or not
        Returns:
            str: Returns  _enable_mail(bool)

        """
        return self._enable_mail

    def set_to_mail(self, to_mail: str) -> None:
        self._to_mail = to_mail

    @property
    def get_to_mail(self) -> str:
        """
        This function returns list of mail id's
        Returns:
            str: Returns _mail_id(str)

        """
        if self._to_mail:
            return self._to_mail

        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_to_mail' before 
            accessing it"""
        )

    def set_mail_from(self, mail_from: str) -> None:
        self._mail_from = mail_from

    @property
    def get_mail_from(self) -> str:
        """
        This function returns mail id to send email
        Returns:

        """
        if self._mail_from:
            return self._mail_from

        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_mail_from' before 
            accessing it"""
        )

    def set_mail_subject(self, mail_subject: str) -> None:
        self._mail_subject = mail_subject

    @property
    def get_mail_subject(self) -> str:
        """
        This function returns mail subject
        Returns:
            str: Returns _mail_subject(str)

        """
        if self._mail_subject:
            return self._mail_subject

        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_mail_subject' before 
            accessing it"""
        )

    def set_enable_slack(self, enable_slack: bool) -> None:
        """

        Args:
            enable_slack:

        Returns:

        """
        self._enable_slack = enable_slack

    @property
    def get_enable_slack(self) -> bool:
        """
        This function returns whether to enable slack notification or not
        Returns: Returns _enable_slack(bool)

        """
        return self._enable_slack

    def set_slack_webhook_url(self, slack_webhook_url: str) -> None:
        self._slack_webhook_url = slack_webhook_url

    @property
    def get_slack_webhook_url(self) -> str:
        """
        This function returns sack webhook url
        Returns:
            str: Returns _webhook_url(str)

        """

        if self._slack_webhook_url:
            return self._slack_webhook_url
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_slack_webhook_url' before 
            accessing it"""
        )

    def set_enable_teams(self, enable_teams: bool) -> None:
        """

        Args:
            enable_teams:

        Returns:

        """
        self._enable_teams = enable_teams

    @property
    def get_enable_teams(self) -> bool:
        """
        This function returns whether to enable teams notification or not
        Returns: Returns _enable_teams(bool)

        """
        return self._enable_teams

    def set_teams_webhook_url(self, teams_webhook_url: str) -> None:
        self._teams_webhook_url = teams_webhook_url

    @property
    def get_teams_webhook_url(self) -> str:
        """
        This function returns sack webhook url
        Returns:
            str: Returns _webhook_url(str)

        """

        if self._teams_webhook_url:
            return self._teams_webhook_url
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_teams_webhook_url' before 
            accessing it"""
        )

    def set_table_name(self, table_name: str) -> None:
        self._table_name = table_name

    @property
    def get_table_name(self) -> str:
        """
        This function returns table name
        Returns:
            str: Returns _table_name(str)

        """
        if self._table_name:
            return self._table_name
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_table_name' before 
            accessing it"""
        )

    def set_input_count(self, input_count: int = 0) -> None:
        self._input_count = input_count

    @property
    def get_input_count(self) -> int:
        """
        This function return input count
        Returns:
            int: Returns _input_count(int)

        """
        return self._input_count

    def set_error_count(self, error_count: int = 0) -> None:
        self._error_count = error_count

    @property
    def get_error_count(self) -> int:
        """
        This functions return error count
        Returns:
            int: Returns _error_count(int)

        """
        return self._error_count

    def set_output_count(self, output_count: int = 0) -> None:
        self._output_count = output_count

    @property
    def get_output_count(self) -> int:
        """
        This function returns output count
        Returns:
            int: Returns _output(int)

        """
        return self._output_count

    def set_source_agg_dq_result(
        self, source_agg_dq_result: Optional[List[Dict[str, str]]] = None
    ) -> None:
        self._source_agg_dq_result = source_agg_dq_result

    @property
    def get_source_agg_dq_result(self) -> Optional[List[Dict[str, str]]]:
        """
        This function return status of the source_agg_dq_result
        Returns:
            dict: Returns source_agg_dq_result which in list of dict with str(key) and str(value)

        """

        return self._source_agg_dq_result

    def set_final_agg_dq_result(
        self, final_agg_dq_result: Optional[List[Dict[str, str]]] = None
    ) -> None:
        self._final_agg_dq_result = final_agg_dq_result

    @property
    def get_final_agg_dq_result(self) -> Optional[List[Dict[str, str]]]:
        """
        This function return status of the final_agg_dq_result
        Returns:
            dict: Returns final_agg_dq_result which in list of dict with str(key) and str(value)

        """
        return self._final_agg_dq_result

    def set_source_query_dq_result(
        self, source_query_dq_result: Optional[List[Dict[str, str]]] = None
    ) -> None:
        self._source_query_dq_result = source_query_dq_result

    @property
    def get_source_query_dq_result(self) -> Optional[List[Dict[str, str]]]:
        """
        This function return status of the source_query_dq_result
        Returns:
            dict: Returns source_query_dq_result which in list of dict with str(key) and str(value)

        """
        return self._source_query_dq_result

    def set_final_query_dq_result(
        self, final_query_dq_result: Optional[List[Dict[str, str]]] = None
    ) -> None:
        self._final_query_dq_result = final_query_dq_result

    @property
    def get_final_query_dq_result(self) -> Optional[List[Dict[str, str]]]:
        """
        This function return status of the final_query_dq_result
        Returns:
            dict: Returns final_query_dq_result which in list of dict with str(key) and str(value)

        """
        return self._final_query_dq_result

    def set_notification_on_start(self, notification_on_start: bool) -> None:
        self._notification_on_start = notification_on_start

    @property
    def get_notification_on_start(self) -> bool:
        """
        This function returns notification on start
        Returns:
            bool: Returns _notification_on_start

        """
        return self._notification_on_start

    def set_notification_on_completion(self, notification_on_completion: bool) -> None:
        self._notification_on_completion = notification_on_completion

    @property
    def get_notification_on_completion(self) -> bool:
        """
        This function returns notification on completion
        Returns:
            bool: Returns _notification_on_completion

        """
        return self._notification_on_completion

    def set_notification_on_fail(self, notification_on_fail: bool) -> None:
        self._notification_on_fail = notification_on_fail

    @property
    def get_notification_on_fail(self) -> bool:
        """
        This function returns notification on fail
        Returns:
            bool: Returns _notification_on_fail

        """
        return self._notification_on_fail

    def set_env(self, env: Optional[str]) -> None:
        """
        Args:
            env: which accepts env type

        Returns:
            None

        """
        self._env = env

    @property
    def get_env(self) -> Optional[str]:
        """
        functions returns running environment type
        Returns:
               str: Returns _env
        """
        return self._env

    @property
    def get_error_percentage(self) -> float:
        """
        This function returns error percentage
        Returns:
               float: error percentage
        """
        if self._input_count > 0:
            return round((self.get_error_count / self.get_input_count) * 100, 2)
        return 0.0

    @property
    def get_output_percentage(self) -> float:
        """
        This function return output percentage
        Returns:
            float: output percentage

        """
        if self._input_count > 0:
            return round((self.get_output_count / self.get_input_count) * 100, 2)
        return 0.0

    @property
    def get_success_percentage(self) -> float:
        """
        This function returns success percentage
        Returns:
            float: success percentage

        """
        if self._input_count > 0:
            return round(
                ((self.get_input_count - self.get_error_count) / self.get_input_count)
                * 100,
                2,
            )
        return 0.0

    @property
    def get_error_drop_percentage(self) -> float:
        """
        This function returns error drop percentage percentage
        Returns:
            float: error drop percentage

        """
        if self._input_count > 0:
            return round(
                ((self.get_input_count - self.get_output_count) / self.get_input_count)
                * 100,
                2,
            )
        return 0.0

    def set_error_drop_threshold(self, error_drop_threshold: int) -> None:
        self._error_drop_threshold = error_drop_threshold

    @property
    def get_error_drop_threshold(self) -> int:
        """
        This function return error threshold breach
        Returns:
               int: error threshold breach
        """
        if self._error_drop_threshold:
            return self._error_drop_threshold
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_error_drop_threshold'  before 
            accessing it"""
        )

    @property
    def get_cerberus_url(self) -> str:
        """
        This functions implemented to return cerberus url
        Returns:

        """
        if self._cerberus_url:
            return self._cerberus_url
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_cerberus_url'  before 
            accessing it"""
        )

    @property
    def get_cerberus_cred_path(self) -> str:
        """
        This functions implemented to return cerberus credentials path
        Returns:

        """
        if self._cerberus_cred_path:
            return self._cerberus_cred_path
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_cerberus_cred_path'  before 
            accessing it"""
        )

    @property
    def get_cerberus_token(self) -> str:
        """
        This functions implemented to return cerberus token
        Returns:

        """
        if self._cerberus_token:
            return self._cerberus_token
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_cerberus_token'  before 
            accessing it"""
        )

    def set_se_streaming_stats_dict(
        self, se_streaming_stats_dict: Dict[str, str]
    ) -> None:
        """
        This function helps to set secret keys dict"""
        self._se_streaming_stats_dict = se_streaming_stats_dict

    @property
    def get_se_streaming_stats_dict(self) -> Dict[str, str]:
        """
        This function returns secret keys dict
        """
        return self._se_streaming_stats_dict

    @property
    def get_secret_type(self) -> Optional[str]:
        """
        This function helps in getting secret type
        Returns:
             secret type in Optional[str]

        """
        if self._se_streaming_stats_dict.get(user_config.secret_type):
            return self._se_streaming_stats_dict[user_config.secret_type].lower()
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign 
            'UserConfig.secret_type' before 
            accessing it"""
        )

    @property
    def get_server_url_key(self) -> Optional[str]:
        """
        This function helps in getting key / path for kafka server url
         Returns:
              kafka server url key / path in Optional[str]
        """
        _server_url_key: Optional[str] = (
            self._se_streaming_stats_dict.get(user_config.cbs_kafka_server_url)
            if self.get_secret_type == "cerberus"
            else self._se_streaming_stats_dict.get(user_config.dbx_kafka_server_url)
        )
        if _server_url_key:
            return _server_url_key
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign
            'UserConfig.cbs_kafka_server_url' before
            accessing it"""
        )

    @property
    def get_token_endpoint_url(self) -> Optional[str]:
        """
        This function helps in getting key / path for end point url
         Returns:
             end point url key / path in  Optional[str]
        """
        _token_endpoint_url: Optional[str] = (
            self._se_streaming_stats_dict.get(user_config.cbs_secret_token_url)
            if self.get_secret_type == "cerberus"
            else self._se_streaming_stats_dict.get(user_config.dbx_secret_token_url)
        )
        if _token_endpoint_url:
            return _token_endpoint_url
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign
            'UserConfig.cbs_secret_token_url' before
            accessing it"""
        )

    @property
    def get_token(self) -> Optional[str]:
        """
        This function helps in getting key / path for token
        Returns:
             token key / path in Optional[str]
        """
        _token: Optional[str] = (
            self._se_streaming_stats_dict.get(user_config.cbs_secret_token)
            if self.get_secret_type == "cerberus"
            else self._se_streaming_stats_dict.get(user_config.dbx_secret_token)
        )
        if _token:
            return _token
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign
            'UserConfig.cbs_secret_token' before
            accessing it"""
        )

    @property
    def get_client_id(self) -> Optional[str]:
        """
        This function helps in getting key / path for client id
        Returns:
            client id key / path in Optional[str]
        """
        _client_id: Optional[str] = (
            self._se_streaming_stats_dict.get(user_config.cbs_secret_app_name)
            if self.get_secret_type == "cerberus"
            else self._se_streaming_stats_dict.get(user_config.dbx_secret_app_name)
        )
        if _client_id:
            return _client_id
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign
            'UserConfig.cbs_secret_app_name' before
            accessing it"""
        )

    @property
    def get_topic_name(self) -> Optional[str]:
        """
        This function helps in getting key / path for topic name
        Returns:
            topic name key / path in Optional[str]
        """
        _topic_name: Optional[str] = (
            self._se_streaming_stats_dict.get(user_config.cbs_topic_name)
            if self.get_secret_type == "cerberus"
            else self._se_streaming_stats_dict.get(user_config.dbx_topic_name)
        )
        if _topic_name:
            return _topic_name
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign 
            'UserConfig.cbs_topic_name' before 
            accessing it"""
        )

    def set_se_streaming_stats_topic_name(
        self, se_streaming_stats_topic_name: str
    ) -> None:
        self._se_streaming_stats_topic_name = se_streaming_stats_topic_name

    @property
    def get_se_streaming_stats_topic_name(self) -> str:
        """
        This function returns kafka topic name
        Returns:
            str: Returns _se_streaming_stats_topic_name

        """
        if self._se_streaming_stats_topic_name:
            return self._se_streaming_stats_topic_name
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign 
            '_se_streaming_stats_topic_name' before 
            accessing it"""
        )

    def set_debugger_mode(self, debugger_mode: bool) -> None:
        """
        This function sets debugger mode
        Returns:

        """
        self._debugger_mode = debugger_mode

    @property
    def get_debugger_mode(self) -> bool:
        """
        This function returns a debugger
        Returns:
             bool: return debugger

        """
        return self._debugger_mode

    def print_dataframe_with_debugger(self, df: DataFrame) -> None:
        """
        This function has a debugger that can print out the DataFrame
        Returns:

        """
        if self.get_debugger_mode:
            df.show(truncate=False)

    def set_supported_df_query_dq(self) -> DataFrame:
        return self.spark.createDataFrame(
            [
                {
                    "spark_expectations_query_check": "supported_place_holder_dataset_to_run_query_check"
                }
            ]
        )

    @property
    def get_supported_df_query_dq(self) -> DataFrame:
        """
        This function returns the place holder dataframe for query check
        Returns:
            DataFrame: returns dataframe for query dq

        """

        if self._supported_df_query_dq:
            return self._supported_df_query_dq
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_supported_df_query_dq'  before 
            accessing it"""
        )

    def set_source_agg_dq_start_time(self) -> None:
        """
        This function sets start time source agg dq computation
        Returns:
             None

        """
        self._source_agg_dq_start_time = datetime.now()

    def set_source_agg_dq_end_time(self) -> None:
        """
        This function sets end time source agg dq computation
        Returns:
            None

        """
        self._source_agg_dq_end_time = datetime.now()

    def set_final_agg_dq_start_time(self) -> None:
        """
        This function sets start time final agg dq computation
        Returns:
        None

        """
        self._final_agg_dq_start_time = datetime.now()

    def set_final_agg_dq_end_time(self) -> None:
        """
        This function sets end time final agg dq computation
        Returns:
            None
        """
        self._final_agg_dq_end_time = datetime.now()

    def set_source_query_dq_start_time(self) -> None:
        """
        This function sets start time source query dq computation
        Returns:
            None
        """
        self._source_query_dq_start_time = datetime.now()

    def set_source_query_dq_end_time(self) -> None:
        """
        This function sets end time source query dq computation
        Returns:
            None
        """
        self._source_query_dq_end_time = datetime.now()

    def set_final_query_dq_start_time(self) -> None:
        """
        This function sets start time final query dq computation
        Returns:
            None
        """
        self._final_query_dq_start_time = datetime.now()

    def set_final_query_dq_end_time(self) -> None:
        """
        This function sets end time final query dq computation
        Returns:
            None
        """
        self._final_query_dq_end_time = datetime.now()

    def set_row_dq_start_time(self) -> None:
        """
        This function sets start time row dq computation
        Returns:
            None
        """
        self._row_dq_start_time = datetime.now()

    def set_row_dq_end_time(self) -> None:
        """
        This function sets end time row dq computation
        Returns:
            None
        """
        self._row_dq_end_time = datetime.now()

    def set_dq_start_time(self) -> None:
        """
        This function sets start time dq computation
        Returns:
            None
        """
        self._dq_start_time = datetime.now()

    def set_dq_end_time(self) -> None:
        """
        This function sets end time dq computation
        Returns:
            None
        """
        self._dq_end_time = datetime.now()

    def set_end_time_when_dq_job_fails(self) -> None:
        """
        function used to set end time when job fails in any one of the stages by using start time
        Returns:
        """
        if self._source_agg_dq_start_time and self._source_agg_dq_end_time is None:
            self.set_source_agg_dq_end_time()
        elif (
            self._source_query_dq_start_time and self._source_query_dq_end_time is None
        ):
            self.set_source_query_dq_end_time()
        elif self._row_dq_start_time and self._row_dq_end_time is None:
            self.set_row_dq_end_time()
        elif self._final_agg_dq_start_time and self._final_agg_dq_end_time is None:
            self.set_final_agg_dq_end_time()
        elif self._final_query_dq_start_time and self._final_query_dq_end_time is None:
            self.set_final_query_dq_end_time()

    def get_time_diff(
        self, start_time: Optional[datetime], end_time: Optional[datetime]
    ) -> float:
        """
        This function implements time diff
        Args:
            start_time:
            end_time:

        Returns:

        """
        if start_time and end_time:
            time_diff = end_time - start_time

            return round(float(time_diff.total_seconds()), 1)
        else:
            return 0.0

    @property
    def get_source_agg_dq_run_time(self) -> float:
        """
        This function implements time diff for source agg dq run
        Returns:
             float: time in float

        """
        return self.get_time_diff(
            self._source_agg_dq_start_time, self._source_agg_dq_end_time
        )

    @property
    def get_final_agg_dq_run_time(self) -> float:
        """
        This function implements time diff for final agg dq run
        Returns:
             float: time in float

        """
        return self.get_time_diff(
            self._final_agg_dq_start_time, self._final_agg_dq_end_time
        )

    @property
    def get_source_query_dq_run_time(self) -> float:
        """
        This function implements time diff for source query dq run
        Returns:
            float: time in float
        """
        return self.get_time_diff(
            self._source_query_dq_start_time, self._source_query_dq_end_time
        )

    @property
    def get_final_query_dq_run_time(self) -> float:
        """
        This function implements time diff for final query dq run
        Returns:
            float: time in float
        """
        return self.get_time_diff(
            self._final_query_dq_start_time, self._final_query_dq_end_time
        )

    @property
    def get_row_dq_run_time(self) -> float:
        """
        This function implements time diff for row dq run
        Returns:
            float: time in float
        """
        return self.get_time_diff(self._row_dq_start_time, self._row_dq_end_time)

    @property
    def get_dq_run_time(self) -> float:
        """
        This function implements time diff for dq run
        Returns:
            float: time in float
        """
        return self.get_time_diff(self._dq_start_time, self._dq_end_time)

    @property
    def get_run_id_name(self) -> str:
        """
        This function returns name for the run_id column
        Returns:
            str: name of run_id in str

        """
        if self._run_id_name:
            return self._run_id_name
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_run_id_name'  before 
            accessing it"""
        )

    @property
    def get_run_date_name(self) -> str:
        """
        This function returns name for the run_date column
        Returns:
            str: name of run_date in str

        """
        if self._run_date_name:
            return self._run_date_name
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_run_date_name'  before 
            accessing it"""
        )

    @property
    def get_run_date_time_name(self) -> str:
        """
        This function returns name for the run_date_time column
        Returns:
            str: name of run_date_time in str

        """
        if self._run_date_time_name:
            return self._run_date_time_name
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_run_date_time_name'  before 
            accessing it"""
        )

    def reset_num_row_dq_rules(self) -> None:
        """
        This function used to reset the _num_row_dq_rules
        Returns:
            None

        """

        self._num_row_dq_rules = 0  # pragma: no cover

    def reset_num_agg_dq_rules(self) -> None:
        """
        This function used to reset the_num_agg_dq_rules
        Returns:
            None

        """
        self._num_agg_dq_rules = {
            "num_agg_dq_rules": 0,
            "num_source_agg_dq_rules": 0,
            "num_final_agg_dq_rules": 0,
        }

    def reset_num_query_dq_rules(self) -> None:
        """
        This function used to rest the _num_query_dq_rules
        Returns:
            None

        """
        self._num_query_dq_rules = {
            "num_query_dq_rules": 0,
            "num_source_query_dq_rules": 0,
            "num_final_query_dq_rules": 0,
        }

    def reset_num_dq_rules(self) -> None:
        """
        This function used to reset the _num_dq_rules
        Returns:
            None

        """
        self._num_dq_rules = 0

    def set_num_row_dq_rules(self) -> None:
        """
        This function sets number of applied row dq rules for batch run
        Returns:
            None

        """
        self._num_row_dq_rules += 1
        self._num_dq_rules += 1

    def set_num_agg_dq_rules(
        self, source_agg_enabled: bool = False, final_agg_enabled: bool = False
    ) -> None:
        """
        This function sets number of applied agg dq rules for batch run
        source_agg_enabled: Marked True when agg rules set for source, by default False
        final_agg_enabled: Marked True when agg rules set for final, by default False
        Returns:
            None
        """

        self._num_agg_dq_rules["num_agg_dq_rules"] += 1
        self._num_dq_rules += 1

        if source_agg_enabled:
            self._num_agg_dq_rules["num_source_agg_dq_rules"] += 1
        if final_agg_enabled:
            self._num_agg_dq_rules["num_final_agg_dq_rules"] += 1

    def set_num_query_dq_rules(
        self, source_query_enabled: bool = False, final_query_enabled: bool = False
    ) -> None:
        """
        This function sets number of applied query dq rules for batch run
        source_query_enabled: Marked True when query rules set for source, by default False
        final_query_enabled: Marked True when query rules set for final, by default False
        Returns:
            None
        """

        self._num_query_dq_rules["num_query_dq_rules"] += 1
        self._num_dq_rules += 1

        if source_query_enabled:
            self._num_query_dq_rules["num_source_query_dq_rules"] += 1
        if final_query_enabled:
            self._num_query_dq_rules["num_final_query_dq_rules"] += 1

    @property
    def get_num_row_dq_rules(self) -> int:
        """
        This function returns number row dq rules applied for batch run
        Returns:
            int: number of rules in int

        """

        if isinstance(self._num_row_dq_rules, int):
            return self._num_row_dq_rules
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_num_row_dq_rules'  before 
            accessing it"""
        )

    @property
    def get_num_agg_dq_rules(self) -> dict:
        """
        This function returns number agg dq rules applied for batch run
        Returns:
            int: number of rules in int

        """
        if isinstance(self._num_agg_dq_rules, dict):
            return self._num_agg_dq_rules
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_num_agg_dq_rules'  before 
            accessing it"""
        )

    @property
    def get_num_query_dq_rules(self) -> dict:
        """
        This function returns number query dq rules applied for batch run
        Returns:
            int: number of rules in int

        """
        if isinstance(self._num_query_dq_rules, dict):
            return self._num_query_dq_rules
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_num_query_dq_rules'  before 
            accessing it"""
        )

    @property
    def get_num_dq_rules(self) -> int:
        """
        This function returns number dq rules applied for batch run
        Returns:
            int: number of rules in int
        """
        if isinstance(self._num_dq_rules, int):
            return self._num_dq_rules
        raise SparkExpectationsMiscException(
            """The spark expectations context is not set completely, please assign '_num_dq_rules'  before 
            accessing it"""
        )

    def set_summarised_row_dq_res(
        self, summarised_row_dq_res: Optional[List[Dict[str, str]]] = None
    ) -> None:
        """
        This function implements or supports to set row dq summarised res
        Args:
            summarised_row_dq_res: list(dict)
        Returns: None

        """
        self._summarised_row_dq_res = summarised_row_dq_res

    @property
    def get_summarised_row_dq_res(self) -> Optional[List[Dict[str, str]]]:
        """
        This function returns row dq summarised res
        Returns:
            list(dict): Returns summarised_row_dq_res which in list of dict with str(key) and
            str(value) of rule meta data

        """
        return self._summarised_row_dq_res

    def set_rules_exceeds_threshold(self, rules: Optional[List[dict]] = None) -> None:
        """
        This function implements error percentage for each rule type
        """
        self._rules_error_per = rules

    @property
    def get_rules_exceeds_threshold(self) -> Optional[List[dict]]:
        """
        This function returns error percentage for each rule
        """
        return self._rules_error_per

    def set_target_and_error_table_writer_config(self, config: dict) -> None:
        """
        This function sets target and error table writer config
        Args:
            config: dict
        Returns: None

        """
        self._target_and_error_table_writer_config = config

    @property
    def get_target_and_error_table_writer_config(self) -> dict:
        """
        This function returns target and error table writer config
        Returns:
            dict: Returns target_and_error_table_writer_config which in dict

        """
        return self._target_and_error_table_writer_config

    def set_stats_table_writer_config(self, config: dict) -> None:
        """
        This function sets stats table writer config
        Args:
            config: dict
        Returns: None
        """
        self._stats_table_writer_config = config

    @property
    def get_stats_table_writer_config(self) -> dict:
        """
        This function returns stats table writer config
        Returns:
            dict: Returns stats_table_writer_config which in dict
        """
        return self._stats_table_writer_config
