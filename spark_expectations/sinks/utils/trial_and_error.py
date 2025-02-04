from spark_expectations.config.user_config import Constants as user_config
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
class SparkExpectationsReader1:
    """
    This class implements/supports reading data from source system
    """

    _context: SparkExpectationsContext

    def __post_init__(self) -> None:
        self.spark = self._context.spark

    def set_notification_param1(
        self, notification: Optional[Dict[str, Union[int, str, bool]]] = None
    ) -> None:
        _notification_dict: Dict[str, Union[str, int, bool]] = (
             notification


        )

        print(_notification_dict.get(user_config.se_enable_obs_dq_report_result))






user_conf = {
    user_config.se_notifications_enable_custom_dataframe: False,
    user_config.se_enable_obs_dq_report_result: False,
    user_config.se_dq_obs_alert_flag: True,
    user_config.se_dq_obs_default_email_template: "",
    user_config.se_dq_obs_mode_of_communication: False,
    user_config.se_notifications_enable_email: False,
    user_config.se_notifications_enable_custom_email_body: False,
    user_config.se_notifications_email_smtp_host: "smtp.office365.com",
    user_config.se_notifications_email_smtp_port: 587,
    user_config.se_notifications_service_account_email: "a.dsm.pss.obs@nike.com",
    user_config.se_notifications_service_account_password: "wp=Wq$37#UI?Ijy7_HNU",
    user_config.se_notifications_email_from: "sudeepta.pal@nike.com,aaaalfyofqi7i7nxuvxlboxbym@nike.org.slack.com,aaaali2kvghxahbath2kkud3ga@nike.org.slack.com",
    user_config.se_notifications_email_to_other_mail_id: "sudeepta.pal@nike.com",
    user_config.se_notifications_email_subject: "spark expectations - data quality - notifications",
    user_config.se_notifications_email_custom_body: """Spark Expectations Statistics for this dq run:
    vamsi sudeep malik raghav
    """,
    user_config.se_notifications_enable_slack: False,
    user_config.se_notifications_slack_webhook_url: "",
    user_config.se_notifications_on_start: True,
    user_config.se_notifications_on_completion: True,
    user_config.se_notifications_on_fail: True,
    user_config.se_notifications_on_error_drop_exceeds_threshold_breach: True,
    user_config.se_notifications_on_error_drop_threshold: 15,
    user_config.se_enable_query_dq_detailed_result: True,
    user_config.se_enable_agg_dq_detailed_result: True,
    user_config.se_enable_error_table: True,
    user_config.se_dq_rules_params: {
        "env": "dev",
        "table": "product",
        "data_object_name": "customer_order",
        "data_source": "customer_source",
        "data_layer": "Integrated"
    },
    user_config.se_job_metadata: "abc",}



# Assuming you have an instance of SparkExpectationsContext




