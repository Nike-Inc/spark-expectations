import os
from dataclasses import dataclass
from typing import Dict, Tuple
from pyspark.sql import SparkSession, DataFrame
import smtplib
import traceback
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from os import getenv
from jinja2 import Environment, FileSystemLoader,BaseLoader
import re

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
        This function is to send the DQ report to the users.

        Args:
            body: Email body.
            subject: Email subject.
            receivers_list: List of email receivers.
        """
        try:
            cerberus_url = 'https://prod.cerberus.nikecloud.com/'
            cerberus_sdb_path = "app/if-common/smtp"
            # SMTP_USER_NAME = list(smtp_details.keys())[0]
            # service_account_email = f"{SMTP_USER_NAME}@nike.com"
            # service_account_password = smtp_details.get(SMTP_USER_NAME)
            service_account_email = self._context.get_service_account_email
            service_account_password = self._context.get_service_account_password
            body = MIMEText(body, 'html')
            msg = MIMEMultipart()
            msg.attach(body)
            msg['Subject'] = subject
            msg['From'] = service_account_email
            msg['To'] = receivers_list

            smtp_host = self._context.get_mail_smtp_server
            smtp_port = self._context.get_mail_smtp_port

            with smtplib.SMTP(smtp_host, port=smtp_port) as smtp_server:
                smtp_server.ehlo()
                smtp_server.starttls()
                smtp_server.login(service_account_email, service_account_password)
                smtp_server.sendmail(msg['From'], receivers_list.split(','), msg.as_string())
                print("Report sent successfully!")
        except Exception as e:
            print(f"Error in send_mail: {e}")
            traceback.print_exc()

    def get_report_data(self) -> None:
        """
        This function calls the dq_obs_report_data_insert method from SparkExpectationsReport.

        Args:
            df_detailed: Detailed DataFrame.
            df_query_output: Query output DataFrame.
        """
        try:
            if self._context.get_dq_obs_rpt_gen_status_flag is True:
                from spark_expectations.sinks.utils.report import SparkExpectationsReport

                report = SparkExpectationsReport(self._context)
                df=self._context.get_df_dq_obs_report_dataframe

                print("success lets redesign the report")
                df.show()
                if len(self._context.get_default_template)==0:
                    template_dir = os.path.join(os.path.dirname(__file__), 'templates')
                    env_loader = Environment(loader=FileSystemLoader(template_dir))
                    template = env_loader.get_template('advanced_email_alert_template.jinja')
                else:
                    template_dir=self._context.get_default_template
                    template = Environment(loader=BaseLoader).from_string(template_dir)

                df_data = [row.asDict() for row in df.collect()]
                headers = list(df.columns)
                rows = [row.asDict().values() for row in df.collect()]
                print("df_data")
                print(df_data)

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
                mail_reciver_list=self._context.get_to_mail
                # mail_subject= self._context.get_mail_subject
                mail_subject="test"
                self.send_mail(html_output, mail_subject, mail_reciver_list)
            elif len(self._context.get_email_custom_body) !=0 and len(self._context.get_mail_subject):
                self.send_mail(self._context.get_email_custom_body, self._context.get_mail_subject, self._context.get_to_mail)



        except Exception as e:
            print(f"Error in get_report_data: {e}")
            traceback.print_exc()

    def with_alert(self,custom_table:str):
        def wrapper(*args, **kwargs):
            self.send_mail("Email body", "Email subject", "receiver@example.com")  # Call the send_mail function

            return

        return wrapper
