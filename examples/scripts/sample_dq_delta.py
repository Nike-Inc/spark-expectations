# Define the product_id

import os
from typing import Dict, Union
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
print(sys.path)
# Define the product_id

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from spark_expectations import _log
from spark_expectations.config.user_config import Constants as user_config
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core.expectations import (
    SparkExpectations,
    WrappedDataFrameWriter,
)
from examples.scripts.base_setup import set_up_delta
from spark_expectations.notifications.push.alert import SparkExpectationsAlert
from spark_expectations.utils.reader import SparkExpectationsReader


writer = WrappedDataFrameWriter().mode("append").format("delta")
spark = set_up_delta()
dic_job_info = {
    "job": "job_name",
    "Region": "NA",
    "env": "dev",
    "Snapshot": "2024-04-15",
    "data_object_name ": "customer_order",
}
job_info = str(dic_job_info)

se: SparkExpectations = SparkExpectations(
    product_id="your_product",
    rules_df=spark.table("dq_spark_dev.dq_rules"),
    stats_table="dq_spark_dev.dq_stats",
    stats_table_writer=writer,
    target_and_error_table_writer=writer,
    debugger=False,
    stats_streaming_options={user_config.se_enable_streaming: False},
)


user_conf: Dict[str, Union[str, int, bool, Dict[str, str]]] = {
    user_config.se_notifications_smtp_password: "w*******",
    user_config.se_notifications_smtp_creds_dict: {
        user_config.secret_type: "cerberus",
        user_config.cbs_url: "https://cerberus.example.com",
        user_config.cbs_sdb_path: "your_sdb_path",
        user_config.cbs_smtp_password: "your_smtp_password",
    },
    user_config.se_notifications_enable_smtp_server_auth: False,
    user_config.se_enable_obs_dq_report_result: False,
    user_config.se_dq_obs_alert_flag: False,
    user_config.se_dq_obs_default_email_template: "",
    user_config.se_notifications_enable_email: False,
    user_config.se_notifications_enable_custom_email_body: False,
    user_config.se_notifications_email_smtp_host: "smtp.office365.com",
    user_config.se_notifications_email_smtp_port: 587,
    user_config.se_notifications_email_from: "a.dsm.*****.com",
    user_config.se_notifications_email_to_other_mail_id: "abc@mail.com",
    user_config.se_notifications_email_subject: "spark expectations - data quality - notifications",
    user_config.se_notifications_email_custom_body: """Spark Expectations Statistics for this dq run:
    'product_id': {},
    'table_name': {},
    'source_agg_dq_results': {}',
    'dq_status': {}""",
    user_config.se_notifications_enable_slack: False,
    user_config.se_notifications_slack_webhook_url: "",
    user_config.se_notifications_on_start: False,
    user_config.se_notifications_on_completion: False,
    user_config.se_notifications_on_fail: False,
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
        "data_layer": "Integrated",
    },
    user_config.se_job_metadata: job_info,
}


@se.with_expectations(
    target_table="dq_spark_dev.customer_order",
    write_to_table=True,
    user_conf=user_conf,
    target_table_view="order",
)
def build_new() -> DataFrame:
    _df_order_source: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "..", "resources", "order.csv"))
    )
    _df_order_source.createOrReplaceTempView("order_source")

    _df_order_target: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "..", "resources", "order.csv"))
    )
    _df_order_target.createOrReplaceTempView("order_target")

    _df_product: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "..", "resources", "product.csv"))
    )
    _df_product.createOrReplaceTempView("product")

    _df_customer_source: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "..", "resources", "customer_source.csv"))
    )

    _df_customer_source.createOrReplaceTempView("customer_source")

    _df_customer_target: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "..", "resources", "customer_source.csv"))
    )
    _df_customer_target.createOrReplaceTempView("customer_target")

    return _df_order_source


if __name__ == "__main__":
    build_new()

    spark.sql("use dq_spark_dev")
    spark.sql("select * from dq_spark_dev.dq_stats").show(truncate=False)
    spark.sql("select * from dq_spark_dev.dq_stats_detailed").show(truncate=False)
    spark.sql("select * from dq_spark_dev.dq_stats_querydq_output").show(truncate=False)
    spark.sql("select * from dq_spark_dev.dq_stats").printSchema()
    spark.sql("select * from dq_spark_dev.dq_stats_detailed").printSchema()
    spark.sql("select * from dq_spark_dev.customer_order").show(truncate=False)
    # spark.sql("select count(*) from dq_spark_local.customer_order_error ").show(
    #    truncate=False
    # )

    _log.info("stats data in the kafka topic")
    # display posted statistics from the kafka topic

    """
    spark.read.format("kafka").option(
        "kafka.bootstrap.servers", "localhost:9092"
    ).option("subscribe", "dq-sparkexpectations-stats").option(
        "startingOffsets", "earliest"
    ).option(
        "endingOffsets", "latest"
    ).load().selectExpr(
        "cast(value as string) as stats_records"
    ).show(
        truncate=False
    )
    """

    # remove docker container
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # os.system(f"sh {current_dir}/../containers/kafka/scripts/docker_kafka_stop_script.sh")
