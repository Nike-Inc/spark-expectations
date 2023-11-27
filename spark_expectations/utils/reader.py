import os
from typing import Optional, Union, Dict
from dataclasses import dataclass

from pyspark.sql import DataFrame
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
        self, notification: Optional[Dict[str, Union[int, str, bool]]] = None
    ) -> None:
        """
        This function supports to read notifications configurations
        Returns: None

        """
        try:
            _default_spark_conf: Dict[str, Union[str, int, bool]] = {
                user_config.se_notifications_enable_email: False,
                user_config.se_notifications_email_smtp_host: "",
                user_config.se_notifications_email_smtp_port: 25,
                user_config.se_notifications_email_from: "",
                user_config.se_notifications_email_to_other_mail_id: "",
                user_config.se_notifications_email_subject: "spark-expectations-testing",
                user_config.se_notifications_enable_slack: False,
                user_config.se_notifications_slack_webhook_url: "",
                user_config.se_notifications_enable_teams: False,
                user_config.se_notifications_teams_webhook_url: "",
            }

            _notification_dict: Dict[str, Union[str, int, bool]] = (
                {**_default_spark_conf, **notification}
                if notification
                else _default_spark_conf
            )

            if (
                _notification_dict.get(user_config.se_notifications_enable_email)
                is True
            ):
                if (
                    _notification_dict[user_config.se_notifications_email_smtp_host]
                    and _notification_dict[user_config.se_notifications_email_from]
                    and _notification_dict[
                        user_config.se_notifications_email_to_other_mail_id
                    ]
                    and _notification_dict[user_config.se_notifications_email_subject]
                ):
                    self._context.set_enable_mail(True)
                    self._context.set_to_mail(
                        str(
                            _notification_dict[
                                user_config.se_notifications_email_to_other_mail_id
                            ]
                        )
                    )
                    self._context.set_mail_subject(
                        str(
                            _notification_dict[
                                user_config.se_notifications_email_subject
                            ]
                        )
                    )
                    self._context.set_mail_smtp_server(
                        str(
                            _notification_dict[
                                user_config.se_notifications_email_smtp_host
                            ]
                        )
                    )
                    self._context.set_mail_smtp_port(
                        int(
                            _notification_dict[
                                user_config.se_notifications_email_smtp_port
                            ]
                        )
                    )

                    self._context.set_mail_from(
                        str(_notification_dict[user_config.se_notifications_email_from])
                    )
                else:
                    raise SparkExpectationsMiscException(
                        "All params/variables required for email notification is not configured or supplied"
                    )

            if _notification_dict[user_config.se_notifications_enable_slack] is True:
                if _notification_dict[user_config.se_notifications_slack_webhook_url]:
                    self._context.set_enable_slack(True)
                    self._context.set_slack_webhook_url(
                        str(
                            _notification_dict[
                                user_config.se_notifications_slack_webhook_url
                            ]
                        )
                    )
                else:
                    raise SparkExpectationsMiscException(
                        "All params/variables required for slack notification is not configured or supplied"
                    )

            if _notification_dict[user_config.se_notifications_enable_teams] is True:
                if _notification_dict[user_config.se_notifications_teams_webhook_url]:
                    self._context.set_enable_teams(True)
                    self._context.set_teams_webhook_url(
                        str(
                            _notification_dict[
                                user_config.se_notifications_teams_webhook_url
                            ]
                        )
                    )
                else:
                    raise SparkExpectationsMiscException(
                        "All params/variables required for slack notification is not configured or supplied"
                    )

        except Exception as e:
            raise SparkExpectationsMiscException(
                f"error occurred while reading notification configurations {e}"
            )

    def get_rules_from_df(
        self,
        rules_df: DataFrame,
        target_table: str,
        is_dlt: bool = False,
        tag: Optional[str] = None,
    ) -> tuple[dict, dict]:
        """
        This function fetches the data quality rules from the table and return it as a dictionary

        Args:
            rules_df: DataFrame which has your data quality rules
            target_table: Provide the full table name for which the data quality rules are being run
            is_dlt: True if this for fetching the rules for dlt job
            tag: If is_dlt is True, provide the KPI for which you are running the data quality rule

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

            _rules_df = rules_df.filter(
                (rules_df.product_id == self._context.product_id)
                & (rules_df.table_name == target_table)
                & rules_df.is_active
            )

            self._context.print_dataframe_with_debugger(_rules_df)

            _expectations: dict = {}
            _rules_execution_settings: dict = {}
            if is_dlt:
                if tag:
                    for row in _rules_df.filter(_rules_df.tag == tag).collect():
                        _expectations[row["rule"]] = row["expectation"]
                else:
                    for row in _rules_df.collect():
                        _expectations[row["rule"]] = row["expectation"]
            else:
                for row in _rules_df.collect():
                    column_map = {
                        "product_id": row["product_id"],
                        "table_name": row["table_name"],
                        "rule_type": row["rule_type"],
                        "rule": row["rule"],
                        "column_name": row["column_name"],
                        "expectation": row["expectation"],
                        "action_if_failed": row["action_if_failed"],
                        "enable_for_source_dq_validation": row[
                            "enable_for_source_dq_validation"
                        ],
                        "enable_for_target_dq_validation": row[
                            "enable_for_target_dq_validation"
                        ],
                        "tag": row["tag"],
                        "description": row["description"],
                        "enable_error_drop_alert": row["enable_error_drop_alert"],
                        "error_drop_threshold": row["error_drop_threshold"],
                    }

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
                    _rules_execution_settings = self._get_rules_execution_settings(
                        _rules_df
                    )
            return _expectations, _rules_execution_settings
        except Exception as e:
            raise SparkExpectationsMiscException(
                f"error occurred while retrieving rules list from the table {e}"
            )

    def _get_rules_execution_settings(self, rules_df: DataFrame) -> dict:
        rules_df.createOrReplaceTempView("rules_view")
        df = self.spark.sql(
            """SELECT
                MAX(CASE WHEN rule_type = 'row_dq' THEN True ELSE False END) AS row_dq,
                MAX(CASE WHEN rule_type = 'agg_dq' AND enable_for_source_dq_validation = true  
                   THEN True ELSE False END) AS source_agg_dq,
                MAX(CASE WHEN rule_type = 'query_dq' AND enable_for_source_dq_validation = true 
                    THEN True ELSE False END) AS source_query_dq,
                MAX(CASE WHEN rule_type = 'agg_dq' AND enable_for_target_dq_validation = true 
                    THEN True ELSE False END) AS target_agg_dq,
                MAX(CASE WHEN rule_type = 'query_dq' AND enable_for_target_dq_validation = true 
                    THEN True ELSE False END) AS target_query_dq
            FROM rules_view"""
        )
        # convert the df to python dictionary as it has only one row
        rule_execution_settings = df.collect()[0].asDict()
        self.spark.catalog.dropTempView("rules_view")
        return rule_execution_settings
