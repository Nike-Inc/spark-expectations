import json
import os
from typing import Optional, Union, Dict, Tuple
from dataclasses import dataclass
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, when, max
from spark_expectations import _log
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.config.user_config import Constants as user_config

from spark_expectations.core.exceptions import (
    SparkExpectationsMiscException,
)


@dataclass
class SparkExpectationsReader:
    """
    This class implements/supports reading data from source system
    """

    _context: SparkExpectationsContext

    def __post_init__(self) -> None:
        self.spark = self._context.spark

    def set_notification_param(
        self,
        notification: Optional[Dict[str, Union[int, str, bool, Dict[str, str]]]] = None,
    ) -> None:
        """
        This function supports to read notifications configurations
        Returns: None

        """
        try:
            _default_notification_dict: Dict[str, Union[str, int, bool, Dict[str, str]]] = json.loads(
                self.spark.conf.get("default_notification_dict")
            )

            _notification_dict: Dict[str, Union[str, int, bool, Dict[str, str]]] = (
                {**_default_notification_dict, **notification} if notification else _default_notification_dict
            )

            if _notification_dict.get(user_config.se_enable_obs_dq_report_result) is True:
                self._context.set_enable_obs_dq_report_result(True)
                if _notification_dict.get(user_config.se_dq_obs_alert_flag) is True:
                    self._context.set_se_dq_obs_alert_flag(True)

                    smtp_port = _notification_dict.get(user_config.se_notifications_email_smtp_port, 25)
                    self._context.set_mail_smtp_port(int(smtp_port) if isinstance(smtp_port, (str, int)) else 25)
                    self._context.set_mail_subject(str(_notification_dict[user_config.se_notifications_email_subject]))
                    self._context.set_mail_smtp_password(
                        str(_notification_dict[user_config.se_notifications_smtp_password])
                    )
                    self._context.set_mail_from(str(_notification_dict[user_config.se_notifications_email_from]))
                    self._context.set_to_mail(
                        str(_notification_dict[user_config.se_notifications_email_to_other_mail_id])
                    )
                    self._context.set_mail_smtp_server(
                        str(_notification_dict[user_config.se_notifications_email_smtp_host])
                    )
                    self._context.set_detailed_default_template(
                        str(_notification_dict[user_config.se_dq_obs_default_email_template])
                    )
                else:
                    self._context.set_se_dq_obs_alert_flag(False)
                    raise SparkExpectationsMiscException(
                        "All params/variables required for email notification is not configured or supplied"
                    )

            if _notification_dict.get(user_config.se_notifications_enable_email) is True:
                if (
                    _notification_dict[user_config.se_notifications_email_smtp_host]
                    and _notification_dict[user_config.se_notifications_email_from]
                    and _notification_dict[user_config.se_notifications_email_to_other_mail_id]
                    and _notification_dict[user_config.se_notifications_email_subject]
                ):
                    self._context.set_enable_mail(True)
                    self._context.set_to_mail(
                        str(_notification_dict[user_config.se_notifications_email_to_other_mail_id])
                    )
                    self._context.set_mail_subject(str(_notification_dict[user_config.se_notifications_email_subject]))
                    self._context.set_mail_smtp_server(
                        str(_notification_dict[user_config.se_notifications_email_smtp_host])
                    )
                    smtp_port = _notification_dict.get(user_config.se_notifications_email_smtp_port, 25)
                    self._context.set_mail_smtp_port(int(smtp_port) if isinstance(smtp_port, (str, int)) else 25)

                    self._context.set_mail_from(str(_notification_dict[user_config.se_notifications_email_from]))
                else:
                    raise SparkExpectationsMiscException(
                        "All params/variables required for email notification is not configured or supplied"
                    )
                if _notification_dict[user_config.se_notifications_enable_smtp_server_auth]:
                    self._context.set_enable_smtp_server_auth(True)
                    if (
                        _notification_dict[user_config.se_notifications_smtp_password]
                        and _notification_dict[user_config.se_notifications_smtp_password] != ""
                    ):
                        self._context.set_mail_smtp_password(
                            str(_notification_dict[user_config.se_notifications_smtp_password])
                        )
                    elif user_config.se_notifications_smtp_creds_dict in _notification_dict:
                        smtp_creds_dict = _notification_dict[user_config.se_notifications_smtp_creds_dict]
                        if isinstance(smtp_creds_dict, dict) and all(
                            isinstance(k, str) and isinstance(v, str) for k, v in smtp_creds_dict.items()
                        ):
                            self._context.set_smtp_creds_dict(smtp_creds_dict)
                        else:
                            raise SparkExpectationsMiscException("SMTP creds dict contains non-string keys or values")
                    else:
                        raise SparkExpectationsMiscException(
                            "SMTP password is not set or secret dict for its retrieval is not provided"
                        )
                if (
                    _notification_dict[user_config.se_notifications_enable_custom_email_body]
                    and _notification_dict[user_config.se_notifications_email_custom_body]
                ):
                    self._context.set_enable_custom_email_body(True)
                    self._context.set_email_custom_body(
                        str(_notification_dict[user_config.se_notifications_email_custom_body])
                    )
                if _notification_dict[user_config.se_notifications_enable_templated_basic_email_body]:
                    self._context.set_enable_templated_basic_email_body(True)
                self._context.set_basic_default_template(
                    str(_notification_dict[user_config.se_notifications_default_basic_email_template])
                )
            if _notification_dict[user_config.se_notifications_enable_slack] is True:
                if _notification_dict[user_config.se_notifications_slack_webhook_url]:
                    self._context.set_enable_slack(True)
                    self._context.set_slack_webhook_url(
                        str(_notification_dict[user_config.se_notifications_slack_webhook_url])
                    )
                else:
                    raise SparkExpectationsMiscException(
                        "All params/variables required for slack notification is not configured or supplied"
                    )

            if _notification_dict[user_config.se_notifications_enable_teams] is True:
                if _notification_dict[user_config.se_notifications_teams_webhook_url]:
                    self._context.set_enable_teams(True)
                    self._context.set_teams_webhook_url(
                        str(_notification_dict[user_config.se_notifications_teams_webhook_url])
                    )
                else:
                    raise SparkExpectationsMiscException(
                        "All params/variables required for slack notification is not configured or supplied"
                    )

                if _notification_dict[user_config.se_notifications_enable_zoom] is True:
                    if _notification_dict[user_config.se_notifications_zoom_webhook_url]:
                        self._context.set_enable_zoom(True)
                        self._context.set_zoom_webhook_url(
                            str(_notification_dict[user_config.se_notifications_zoom_webhook_url])
                        )
                        self._context.set_zoom_token(str(_notification_dict[user_config.se_notifications_zoom_token]))
                    else:
                        raise SparkExpectationsMiscException(
                            "All params/variables required for zoom notification is not configured or supplied"
                        )

        except Exception as e:
            raise SparkExpectationsMiscException(f"error occurred while reading notification configurations {e}")

    def _process_rules_df(self, _dq_queries_dict: dict, column_map: dict, _row: dict, params: dict) -> DataFrame:
        """
        Process the rules DataFrame and generate the query dictionary and column map.

        Args:
            _dq_queries_dict (dict): The dictionary to store the generated queries.
            column_map (dict): The mapping of column names.
            row (dict): The row containing the rule and expectation information.

        Returns:
            tuple: A tuple containing the updated query dictionary and column map.
        """

        if ("query_dq_delimiter" in _row.keys()) and (
            _row["query_dq_delimiter"] is not None and _row["query_dq_delimiter"] != "null"
        ):
            _dq_query_delimiter = _row["query_dq_delimiter"]
            column_map["enable_querydq_custom_output"] = True
        else:
            _dq_query_delimiter = "@"
            column_map["enable_querydq_custom_output"] = False

        if ("enable_querydq_custom_output" in _row.keys()) and (
            _row["enable_querydq_custom_output"] is not None and _row["enable_querydq_custom_output"] != "null"
        ):
            if isinstance(_row["enable_querydq_custom_output"], bool):
                column_map["enable_querydq_custom_output"] = _row["enable_querydq_custom_output"]
            elif isinstance(_row["enable_querydq_custom_output"], str) and _row[
                "enable_querydq_custom_output"
            ].lower() in ["true"]:
                column_map["enable_querydq_custom_output"] = True

            elif isinstance(_row["enable_querydq_custom_output"], str) and _row[
                "enable_querydq_custom_output"
            ].lower() in ["false"]:
                column_map["enable_querydq_custom_output"] = False

            else:
                column_map["enable_querydq_custom_output"] = False

        else:
            column_map["enable_querydq_custom_output"] = False
            _log.info("enable_querydq_custom_output is a boolean column and is not set, defaulting to False")

        _querydq_secondary_queries = _row["expectation"].split(_dq_query_delimiter)

        _querydq_secondary_queries = [_querydq_secondary_queries[0]] + [
            f"{_querydq_secondary_queries[i]}:{_querydq_secondary_queries[i+1]}"
            for i in range(1, len(_querydq_secondary_queries), 2)
        ]

        if len(_querydq_secondary_queries) > 1:
            _dq_queries_dict[column_map["product_id"] + "|" + column_map["table_name"] + "|" + column_map["rule"]] = {}
            for _index, _dq_queries in enumerate(_querydq_secondary_queries):
                if _index == 0:
                    column_map["expectation"] = _dq_queries
                else:
                    _dq_queries_list = _dq_queries.split(":")

                    _dq_queries_dict[
                        column_map["product_id"] + "|" + column_map["table_name"] + "|" + column_map["rule"]
                    ][_dq_queries_list[0]] = _dq_queries_list[1]

                    column_map["expectation" + "_" + str(_dq_queries_list[0])] = _dq_queries_list[1]

            column_map["expectation"] = column_map["expectation"].format(
                **{
                    **_dq_queries_dict[
                        column_map["product_id"] + "|" + column_map["table_name"] + "|" + column_map["rule"]
                    ],
                    **params,
                }
            )
        else:
            column_map["expectation"] = column_map["expectation"].format(**params)

        return _dq_queries_dict, column_map

    def get_rules_from_df(
        self,
        rules_df: DataFrame,
        target_table: str,
        is_dlt: bool = False,
        tag: Optional[str] = None,
        params: Optional[dict] = None,
    ) -> Tuple[Dict, Dict, Dict]:
        """
        This function fetches the data quality rules from the table and return it as a dictionary

        Args:
            rules_df: DataFrame which has your data quality rules
            target_table: Provide the full table name for which the data quality rules are being run
            is_dlt: True if this for fetching the rules for dlt job
            tag: If is_dlt is True, provide the KPI for which you are running the data quality rule
            params: dictionary values for dynamically updating dq rules

        Returns:
            tuple: returns a tuple of two dictionaries with key as 'rule_type' and 'rules_table_row' as value in
                expectations. dict, and key as 'dq_stage_setting' and 'boolean' as value in rules_execution_settings
                    dict
        """
        try:
            self._context.set_final_table_name(target_table)
            self._context.set_error_table_name(f"{target_table}_error")
            self._context.set_table_name(target_table)
            self._context.set_env(os.environ.get("SPARKEXPECTATIONS_ENV"))

            self._context.reset_num_agg_dq_rules()
            self._context.reset_num_dq_rules()
            self._context.reset_num_row_dq_rules()
            self._context.reset_num_query_dq_rules()

            if params is not None:
                rules_df = reduce(
                    lambda df, kv: df.withColumn(
                        "table_name",
                        expr(f"REPLACE(table_name, '{{{kv[0]}}}', '{kv[1]}')"),
                    ),
                    params.items(),
                    rules_df,
                )

            _rules_df = rules_df.filter(
                (rules_df.product_id == self._context.product_id)
                & (rules_df.table_name == target_table)
                & rules_df.is_active
            )

            if not params:
                params = {}

            self._context.print_dataframe_with_debugger(_rules_df)

            _expectations: dict = {}
            _dq_queries_dict: dict = {}
            _rules_execution_settings: dict = {}
            if is_dlt:
                if tag:
                    for row in _rules_df.filter(_rules_df.tag == tag).collect():
                        _expectations[row["rule"]] = row["expectation"].format(**params)
                else:
                    for row in _rules_df.collect():
                        _expectations[row["rule"]] = row["expectation"].format(**params)
            else:
                for row in _rules_df.collect():
                    column_map = {
                        "product_id": row["product_id"].format(**params),
                        "table_name": row["table_name"].format(**params),
                        "rule_type": row["rule_type"],
                        "rule": row["rule"].format(**params),
                        "column_name": row["column_name"],
                        "expectation": row["expectation"],
                        "action_if_failed": row["action_if_failed"],
                        "enable_for_source_dq_validation": row["enable_for_source_dq_validation"],
                        "enable_for_target_dq_validation": row["enable_for_target_dq_validation"],
                        "tag": row["tag"],
                        "description": row["description"],
                        "enable_error_drop_alert": row["enable_error_drop_alert"],
                        "error_drop_threshold": row["error_drop_threshold"],
                    }

                    if row["rule_type"] == self._context.get_query_dq_rule_type_name:
                        _dq_queries_dict, column_map = self._process_rules_df(
                            _dq_queries_dict, column_map, row.asDict(), params
                        )

                    if f"{row['rule_type']}_rules" in _expectations:
                        _expectations[f"{row['rule_type']}_rules"].append(column_map)
                    else:
                        _expectations[f"{row['rule_type']}_rules"] = [column_map]

                    # count the rules enabled for the current run
                    if row["rule_type"] == self._context.get_row_dq_rule_type_name:
                        self._context.set_num_row_dq_rules()
                    elif row["rule_type"] == self._context.get_agg_dq_rule_type_name:
                        self._context.set_num_agg_dq_rules(
                            row["enable_for_source_dq_validation"],
                            row["enable_for_target_dq_validation"],
                        )
                    elif row["rule_type"] == self._context.get_query_dq_rule_type_name:
                        self._context.set_num_query_dq_rules(
                            row["enable_for_source_dq_validation"],
                            row["enable_for_target_dq_validation"],
                        )

                    _expectations["target_table_name"] = target_table
                _rules_execution_settings = self._get_rules_execution_settings(_rules_df)

            return _dq_queries_dict, _expectations, _rules_execution_settings
        except Exception as e:
            raise SparkExpectationsMiscException(f"error occurred while retrieving rules list from the table {e}")

    def _get_rules_execution_settings(self, rules_df: DataFrame) -> dict:
        rules_exe_df = rules_df.select(
            "rule_type",
            "enable_for_source_dq_validation",
            "enable_for_target_dq_validation",
        )
        df = rules_exe_df.select(
            max(when(rules_exe_df["rule_type"] == "row_dq", True).otherwise(False)).alias("row_dq"),
            max(
                when(
                    (rules_exe_df["rule_type"] == "agg_dq") & (rules_exe_df["enable_for_source_dq_validation"]),
                    True,
                ).otherwise(False)
            ).alias("source_agg_dq"),
            max(
                when(
                    (rules_exe_df["rule_type"] == "query_dq") & (rules_exe_df["enable_for_source_dq_validation"]),
                    True,
                ).otherwise(False)
            ).alias("source_query_dq"),
            max(
                when(
                    (rules_exe_df["rule_type"] == "agg_dq") & (rules_exe_df["enable_for_target_dq_validation"]),
                    True,
                ).otherwise(False)
            ).alias("target_agg_dq"),
            max(
                when(
                    (rules_exe_df["rule_type"] == "query_dq") & (rules_exe_df["enable_for_target_dq_validation"]),
                    True,
                ).otherwise(False)
            ).alias("target_query_dq"),
        )

        rule_execution_settings = df.collect()[0].asDict()

        return rule_execution_settings
