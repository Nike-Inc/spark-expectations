import os
import smtplib
import traceback
from dataclasses import dataclass
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from encodings.punycode import selective_find
from typing import Dict, Tuple

from jinja2 import Environment, FileSystemLoader, BaseLoader
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from spark_expectations.core.context import SparkExpectationsContext


@dataclass
class AlertTrial:
    """
    This class implements the alert trial functionality.
    """
    _context: SparkExpectationsContext

    def __post_init__(self) -> None:
        self.spark = self._context.spark  # Initialize the attribute

    def send_mail(self, body: str, subject: str, receivers_list: str) -> None:
        """
        This function sends the DQ report to the users.

        Args:
            body: Email body.
            subject: Email subject.
            receivers_list: List of email receivers.
        """
        try:
            service_account_email = self._context.get_service_account_email
            service_account_password = self._context.get_service_account_password
            smtp_host = self._context.get_mail_smtp_server
            smtp_port = self._context.get_mail_smtp_port

            msg = MIMEMultipart()
            msg.attach(MIMEText(body, 'html'))
            msg['Subject'] = subject
            msg['From'] = service_account_email
            msg['To'] = receivers_list

            with smtplib.SMTP(smtp_host, port=smtp_port) as smtp_server:
                smtp_server.ehlo()
                smtp_server.starttls()
                smtp_server.login(service_account_email, service_account_password)
                # smtp_server.sendmail(msg['From'], receivers_list.split(','), msg.as_string())
                print("Report sent successfully!")
        except Exception as e:
            print(f"Error in send_mail: {e}")
            traceback.print_exc()

    def get_report_data(self) -> None:
        """
        This function calls the dq_obs_report_data_insert method from SparkExpectationsReport.
        """
        try:
            if self._context.get_dq_obs_rpt_gen_status_flag:
                from spark_expectations.sinks.utils.report import SparkExpectationsReport

                if self._context.get_enable_custom_dataframe:
                    print("entering here")
                    df = self._context.get_custom_dataframe
                else:
                    report = SparkExpectationsReport(self._context)
                    df = self._context.get_df_dq_obs_report_dataframe

                print("Success! Let's redesign the report")
                df.show()

                if not self._context.get_default_template:
                    template_dir = os.path.join(os.path.dirname(__file__), 'templates')
                    env_loader = Environment(loader=FileSystemLoader(template_dir))
                    template = env_loader.get_template('advanced_email_alert_template.jinja')
                else:
                    template_dir = self._context.get_default_template
                    template = Environment(loader=BaseLoader).from_string(template_dir)

                df_data = [row.asDict() for row in df.collect()]
                headers = list(df.columns)
                rows = [row.asDict().values() for row in df.collect()]

                html_output = template.render(
                    title='central_repo_test_table',
                    columns=headers,
                    table_rows=rows,
                    product_id='12345',
                    data_object_name='Sample Data Object',
                    snapshot_date='2023-10-01',
                    region_code='US',
                    dag_name='Sample DAG',
                    run_id='run_12345',
                    overall_status='fail',
                    overall_status_bgcolor='#00FF00',
                    total_rules_executed=10,
                    total_passed_rules=9,
                    total_failed_rules=1,
                    competency_metrics_slack=[],
                    competency_metrics=[],
                    criticality_metrics=[]
                )

                mail_receiver_list = self._context.get_to_mail
                mail_subject = "test"
                self.send_mail(html_output, mail_subject, mail_receiver_list)
        except Exception as e:
            print(f"Error in get_report_data: {e}")
            traceback.print_exc()

    def custom_report_dataframe(self, userdataframe : DataFrame) -> None:
        """
    This function takes a DataFrame from the user, sets it as the custom DataFrame, and generates the report.
        """
        if self._context.set_enable_custom_dataframe:
            self._context.set_custom_dataframe(userdataframe)
            self._context.set_dq_obs_rpt_gen_status_flag(True)
            self.get_report_data()