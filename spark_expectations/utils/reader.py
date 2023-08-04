import os
from typing import Optional, Union, List, Dict
from dataclasses import dataclass

# from cerberus.client import CerberusClient
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
)
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.config.user_config import Constants as user_config
from spark_expectations.core import get_spark_session
from spark_expectations.core.exceptions import (
    SparkExpectationsUserInputOrConfigInvalidException,
    SparkExpectationsMiscException,
)


@dataclass
class SparkExpectationsReader:
    """
    This class implements/supports reading data from source system
    """

    product_id: str
    _context: SparkExpectationsContext

    def __post_init__(self) -> None:
        self.spark = get_spark_session()

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

        except Exception as e:
            raise SparkExpectationsMiscException(
                f"error occurred while reading notification configurations {e}"
            )

    def get_rules_dlt(
        self,
        product_rules_table: str,
        table_name: str,
        action: Union[list, str],
        tag: Optional[str] = None,
    ) -> dict:
        """
        This function supports creating a dict of expectations that is acceptable by DLT
        Args:
             product_rules_table: Provide the full table name, which has your data quality rules
             table_name: Provide the full table name for which the data quality rules are being run
             action: Provide the action which you want to filter from rules table. Value should only from one of these
                           - "fail" or "drop" or "ignore" or provide the needed in a list ["fail", "drop", "ignore"]
             tag: Provide the KPI for which you are running the data quality rule

             Returns:
                   dict: returns a dict with key as 'rule' and 'expectation' as value
        """
        try:
            _actions: List[str] = [].append(action) if isinstance(action, str) else action  # type: ignore
            _expectations: dict = {}
            _rules_df: DataFrame = self.spark.sql(
                f"""
                                       select rule, tag, expectation from {product_rules_table} 
                                       where product_id='{self.product_id}' and table_name='{table_name}' and 
                                       action_if_failed in ('{"', '".join(_actions)}')
                                       """
            )
            if tag:
                for row in _rules_df.filter(col("tag") == tag).collect():
                    _expectations[row["rule"]] = row["expectation"]
            else:
                for row in _rules_df.collect():
                    _expectations[row["rule"]] = row["expectation"]
            return _expectations

        except Exception as e:
            raise SparkExpectationsUserInputOrConfigInvalidException(
                f"error occurred while reading or getting rules from the rules table {e}"
            )

    def get_rules_from_table(
        self,
        product_rules_table: str,
        dq_stats_table_name: str,
        target_table_name: str,
        actions_if_failed: Optional[List[str]] = None,
    ) -> dict:
        """
        This function fetches the data quality rules from the table and return it as a dictionary

        Args:
            product_rules_table: Provide the full table name, which has your data quality rules
            table_name: Provide the full table name for which the data quality rules are being run
            dq_stats_table_name: Provide the table name, to which Data Quality Stats have to be written to
            actions_if_failed: Provide the list of actions in ["fail", "drop", 'ignore'], which need to be applied on a
                particular row if a rule failed

        Returns:
            dict: The dict with table and rules as keys
        """
        try:
            self._context.set_dq_stats_table_name(dq_stats_table_name)

            self._context.set_final_table_name(target_table_name)
            self._context.set_error_table_name(f"{target_table_name}_error")
            self._context.set_table_name(target_table_name)
            self._context.set_env(os.environ.get("SPARKEXPECTATIONS_ENV"))

            self._context.reset_num_agg_dq_rules()
            self._context.reset_num_dq_rules()
            self._context.reset_num_row_dq_rules()
            self._context.reset_num_query_dq_rules()

            _actions_if_failed: List[str] = actions_if_failed or [
                "fail",
                "drop",
                "ignore",
            ]

            _rules_df: DataFrame = self.spark.sql(
                f"""
                        select * from {product_rules_table} where product_id='{self.product_id}' 
                        and table_name='{target_table_name}'
                        and action_if_failed in ('{"', '".join(_actions_if_failed)}') and is_active=true
                        """
            )
            self._context.print_dataframe_with_debugger(_rules_df)

            _expectations: dict = {}
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

                _expectations["target_table_name"] = target_table_name
            return _expectations
        except Exception as e:
            raise SparkExpectationsMiscException(
                f"error occurred while retrieving rules list from the table {e}"
            )
