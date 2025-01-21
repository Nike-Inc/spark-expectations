# mypy: ignore-errors
import os

from pyspark.sql import DataFrame
from spark_expectations import _log
from spark_expectations.examples.base_setup import set_up_delta
from spark_expectations.core.expectations import (
    SparkExpectations,
    WrappedDataFrameWriter,
)
from spark_expectations.config.user_config import Constants as user_config


writer = WrappedDataFrameWriter().mode("append").format("delta")

spark = set_up_delta()
dic_job_info = {
    "job": "na_CORL_DIGITAL_source_to_o9",
    "Region": "NA",
    "env": "dev",
    "Snapshot": "2024-04-15",
}
job_info = str(dic_job_info)

se: SparkExpectations = SparkExpectations(
    product_id="your_product",
    rules_df=spark.table("dq_spark_dev.dq_rules"),
    stats_table="dq_spark_dev.dq_stats",
    stats_table_writer=writer,
    target_and_error_table_writer=writer,
    debugger=False,
    # stats_streaming_options={user_config.se_enable_streaming: False},
)

user_conf = {
    user_config.se_dq_obs_mode_of_communication : False,
    user_config.se_enable_obs_dq_report_result: True,
    user_config.se_dq_obs_alert_flag: False,
    user_config.se_notifications_enable_email: False,
    user_config.se_notifications_enable_custom_email_body: False,
    user_config.se_dq_only_alert: True,
    user_config.se_notifications_email_smtp_host: "smtp.office365.com",
    user_config.se_notifications_email_smtp_port: 587,
    user_config.se_notifications_service_account_email: "a.dsm.pss.obs@nike.com",
    user_config.se_notifications_service_account_password: "wp=Wq$37#UI?Ijy7_HNU",
    user_config.se_notifications_email_from: "sudeepta.pal@nike.com,aaaalfyofqi7i7nxuvxlboxbym@nike.org.slack.com,aaaali2kvghxahbath2kkud3ga@nike.org.slack.com",
    user_config.se_notifications_email_to_other_mail_id: "sudeepta.pal@nike.com",
    user_config.se_notifications_email_subject: "spark expectations - data quality - notifications",
    user_config.se_notifications_email_custom_body: """Spark Expectations Statistics for this dq run:
    'product_id': {},
    'table_name': {},
    'source_agg_dq_results': {}',
    'dq_status': {}""",
    user_config.se_notifications_enable_slack: False,
    user_config.se_notifications_slack_webhook_url: "",
    user_config.se_notifications_on_start: True,
    user_config.se_notifications_on_completion: True,
    user_config.se_notifications_on_fail: True,
    user_config.se_notifications_on_error_drop_exceeds_threshold_breach: True,
    user_config.se_notifications_on_error_drop_threshold: 15,
    user_config.se_enable_query_dq_detailed_result: True,
    user_config.se_enable_agg_dq_detailed_result: True,
    # user_config.querydq_output_custom_table_name: "dq_spark_local.dq_stats_detailed_outputt",
    user_config.se_enable_error_table: True,
    user_config.se_dq_rules_params: {
        "env": "dev",
        "table": "product",
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
        .csv(os.path.join(os.path.dirname(__file__), "resources/order_s.csv"))
    )
    _df_order_source.createOrReplaceTempView("order_source")

    _df_order_target: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "resources/order_t.csv"))
    )
    _df_order_target.createOrReplaceTempView("order_target")

    _df_product: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "resources/product.csv"))
    )
    _df_product.createOrReplaceTempView("product")

    _df_customer_source: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "resources/customer_source.csv"))
    )

    _df_customer_source.createOrReplaceTempView("customer_source")

    _df_customer_target: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "resources/customer_source.csv"))
    )
    _df_customer_target.createOrReplaceTempView("customer_target")

    return _df_order_source


if __name__ == "__main__":
    build_new()

    spark.sql("use dq_spark_dev")
    spark.sql("select * from dq_spark_dev.dq_stats").show(truncate=False)
    spark.sql("select * from dq_spark_dev.dq_stats_detailed").show(truncate=False)
    spark.sql("select * from dq_spark_dev.dq_stats_querydq_output").show(truncate=False)
    _log.info("BELOW IS THE REPORT TABLE")

    # spark.sql("select * from dq_spark_dev.dq_obs_report_data").show(truncate=False)

    # spark.sql("select count(*) from dq_spark_local.customer_order_error ").show(
    #    truncate=False
    # )
    if user_config.se_enable_obs_dq_report_result == "spark.expectations.observability.enabled":
        _log.info("alert_send_successfully")


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
    # os.system(f"sh {current_dir}/docker_scripts/docker_kafka_stop_script.sh")
