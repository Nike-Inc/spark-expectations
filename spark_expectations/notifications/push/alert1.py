import os
import smtplib
import traceback
from dataclasses import dataclass
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, Tuple

from jinja2 import Environment, FileSystemLoader, BaseLoader
from pyspark import Row
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
                smtp_server.sendmail(msg['From'], receivers_list.split(','), msg.as_string())
                print("Report sent successfully!")
        except Exception as e:
            print(f"Error in send_mail: {e}")
            traceback.print_exc()



    def get_report_data(self,report_type : str) -> tuple[list[str], list[Row], int]:
        """
        This function calls the dq_obs_report_data_insert method from SparkExpectationsReport.
        """
        try:
            if self._context.get_dq_obs_rpt_gen_status_flag:
                from spark_expectations.sinks.utils.report import SparkExpectationsReport

                if self._context.get_enable_custom_dataframe:
                    df = self._context.get_custom_dataframe
                else:
                    report = SparkExpectationsReport(self._context)
                    df = self._context.get_df_dq_obs_report_dataframe
                df.createOrReplaceTempView("temp_dq_obs_report")

            queries = {
                "header": "select *,source_dq_status from temp_dq_obs_report",

                "summary": "select *,source_dq_status from temp_dq_obs_report",

                "detailed": "select *,source_dq_status from temp_dq_obs_report",
            }

            format_col_lists = {
                "header": ['source_dq_status'],
                "summary": ['source_dq_status'],
                "detailed": ['source_dq_status']
            }

            query = queries[report_type]
            df = self.spark.sql(query)
            format_col_list = format_col_lists[report_type]

            columns = df.columns
            data = df.collect()
            format_col_idx = columns.index(format_col_list[0])

            return columns, data,format_col_idx
        except Exception as e:
            print(f"Error in get_report_data: {e}")
            traceback.print_exc()
    def prep_report_data(self):

        if not self._context.get_default_template:
            template_dir = os.path.join(os.path.dirname(__file__), 'templates')
            env_loader = Environment(loader=FileSystemLoader(template_dir))
            template = env_loader.get_template('advanced_email_alert_template.jinja')
        else:
            template_dir = self._context.get_default_template
            template = Environment(loader=BaseLoader).from_string(template_dir)

        header_columns, header_data, header_format_col_idx = self.get_report_data( "header")
        summary_columns, summary_data, summary_format_col_idx = self.get_report_data( "summary")
        detailed_columns, detailed_data, detailed_format_col_idx = self.get_report_data("detailed")
        mail_subject="hi"
        mail_receivers_list="sudeepta.pal@nike.com"






        data_dicts = [
            {
                "title": f"Summary by product ID for the run_id ",
                "headers": header_columns,
                "rows": header_data
            },
            {
                "title": "Summary by Scenario :",
                "headers": summary_columns,
                "rows": summary_data
            },
            {
                "title": "Summary by data_rule:",
                "headers": detailed_columns,
                "rows": detailed_data
            }
        ]
        html_data = "<br>".join(
            [template.render(render_table=template.module.render_table, **data_dict) for data_dict in data_dicts])
        html_data = f"<h2>{mail_subject}</h2>" + html_data
        self.send_mail(html_data, mail_subject, mail_receivers_list)

