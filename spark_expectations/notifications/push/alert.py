import os
import smtplib
import traceback
from dataclasses import dataclass
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from jinja2 import Environment, FileSystemLoader, BaseLoader
from pyspark import Row
from spark_expectations import _log
from spark_expectations.notifications import SparkExpectationsEmailPluginImpl
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from spark_expectations.core.context import SparkExpectationsContext


@dataclass
class SparkExpectationsAlert:
    """
    This class implements the alert  functionality.
    """

    _context: SparkExpectationsContext

    def __post_init__(self) -> None:
        self.spark = self._context.spark  # Initialize the attribute

    def get_report_data(self, report_type: str) -> tuple[list[str], list[Row], int]:
        """
        This function calls the dq_obs_report_data_insert method from SparkExpectationsReport.
        """
        try:
            from spark_expectations.sinks.utils.report import SparkExpectationsReport

            report = SparkExpectationsReport(self._context)
            df = self._context.get_df_dq_obs_report_dataframe
            df.createOrReplaceTempView("temp_dq_obs_report")

            queries = {
                "header": f"""SELECT  dq_time AS snapshot_date, product_id,job,
                          CASE WHEN (SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END)) >= 1 THEN 'FAIL' ELSE 'PASS' END AS status
                          FROM temp_dq_obs_report
                          GROUP BY  dq_time, product_id,job""",
                "summary": f"""SELECT product_id, rule, COUNT(rule) AS no_of_rules_executed,
                           'Completed' AS Execution_Status,
                           CASE WHEN (SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END)) >= 1 THEN 'FAIL' ELSE 'PASS' END AS Overall_status,
                           CONCAT('Pass:', SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END), ' / Fail:', SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END)) AS status_summary
                           FROM temp_dq_obs_report
                           GROUP BY product_id,rule""",
                "detailed": f"""SELECT DISTINCT rule, rule AS rule_description,
                            column_name, 'Completed' AS Execution_Status, status AS Validation_Status, total_records,
                            failed_records, valid_records, success_percentage
                            FROM temp_dq_obs_report
                            ORDER BY  rule""",
            }

            format_col_lists = {
                "header": ["status"],
                "summary": ["status_summary"],
                "detailed": ["Validation_Status"],
            }

            query = queries[report_type]
            df = self.spark.sql(query)
            format_col_list = format_col_lists[report_type]

            columns = df.columns
            data = df.collect()
            format_col_idx = columns.index(format_col_list[0])

            return columns, data, format_col_idx
        except Exception as e:
            _log.info(f"Error in get_report_data: {e}")
            traceback.print_exc()

    def prep_report_data(self):
        """
        Prepares the report data and sends it via email.

        This method generates the report data based on the context and sends it
        to the specified email recipients. It uses a Jinja2 template to format
        the report data into HTML.

        The method performs the following steps:
        1. Retrieves the email subject and recipient list from the context.
        2. Loads the email template from the specified directory or context either custom or default
        3. Checks if a custom DataFrame is provided and the report generation status flag is set.
        4. If no custom DataFrame is provided, generates the report data for header, summary, and detailed sections.
        5. Renders the report data into HTML using the Jinja2 template.
        6. Sends the formatted HTML report via email.

        Raises:
            Exception: If an error occurs during the report preparation or email sending process.
        """
        try:
            context = self._context
            mail_subject = self._context.get_mail_subject
            mail_receivers_list = self._context.get_to_mail
            if not self._context.get_default_template:
                template_dir = "../../spark_expectations/config/templates"
                env_loader = Environment(loader=FileSystemLoader(template_dir))
                template = env_loader.get_template(
                    "advanced_email_alert_template.jinja"
                )
            else:
                template_dir = self._context.get_default_template
                template = Environment(loader=BaseLoader).from_string(template_dir)

            header_columns, header_data, header_format_col_idx = self.get_report_data(
                "header"
            )
            (
                summary_columns,
                summary_data,
                summary_format_col_idx,
            ) = self.get_report_data("summary")
            (
                detailed_columns,
                detailed_data,
                detailed_format_col_idx,
            ) = self.get_report_data("detailed")

            data_dicts = [
                {
                    "title": f"Summary by product ID for the run_id ",
                    "headers": header_columns,
                    "rows": header_data,
                },
                {
                    "title": "Summary by Scenario :",
                    "headers": summary_columns,
                    "rows": summary_data,
                },
                {
                    "title": "Summary by data_rule:",
                    "headers": detailed_columns,
                    "rows": detailed_data,
                },
            ]
            html_data = "<br>".join(
                [
                    template.render(
                        render_table=template.module.render_table, **data_dict
                    )
                    for data_dict in data_dicts
                ]
            )
            html_data = f"<h2>{mail_subject}</h2>" + html_data

            config_args = {
                "receiver_mail": context.get_to_mail,
                "subject": context.get_mail_subject,
                "message": str(html_data),
            }
            # calling the email_plugin of Spark expectation
            email_plugin = SparkExpectationsEmailPluginImpl()
            email_plugin.send_notification(context, config_args)

            return html_data, mail_subject, mail_receivers_list
        except Exception as e:
            print(f"Error in prep_report_data: {e}")
            traceback.print_exc()
