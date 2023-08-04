import os
from pyspark.sql import DataFrame
from spark_expectations import _log
from spark_expectations.examples.base_setup import main
from spark_expectations.core import get_spark_session
from spark_expectations.core.expectations import SparkExpectations
from spark_expectations.config.user_config import Constants as user_config

main()

se: SparkExpectations = SparkExpectations(product_id="your_product", debugger=False)
spark = get_spark_session()

global_spark_Conf = {
    user_config.se_notifications_enable_email: False,
    user_config.se_notifications_email_smtp_host: "mailhost.com",
    user_config.se_notifications_email_smtp_port: 25,
    user_config.se_notifications_email_from: "",
    user_config.se_notifications_email_to_other_mail_id: "",
    user_config.se_notifications_email_subject: "spark expectations - data quality - notifications",
    user_config.se_notifications_enable_slack: False,
    user_config.se_notifications_slack_webhook_url: "",
    user_config.se_notifications_on_start: True,
    user_config.se_notifications_on_completion: True,
    user_config.se_notifications_on_fail: True,
    user_config.se_notifications_on_error_drop_exceeds_threshold_breach: True,
    user_config.se_notifications_on_error_drop_threshold: 15,
}


@se.with_expectations(
    se.reader.get_rules_from_table(
        product_rules_table="dq_spark_local.dq_rules",
        target_table_name="dq_spark_local.customer_order",
        dq_stats_table_name="dq_spark_local.dq_stats",
    ),
    write_to_table=True,
    row_dq=True,
    agg_dq={
        user_config.se_agg_dq: True,
        user_config.se_source_agg_dq: True,
        user_config.se_final_agg_dq: True,
    },
    query_dq={
        user_config.se_query_dq: True,
        user_config.se_source_query_dq: True,
        user_config.se_final_query_dq: True,
        user_config.se_target_table_view: "order",
    },
    spark_conf=global_spark_Conf,
)
def build_new() -> DataFrame:
    _df_order: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "resources/order.csv"))
    )
    _df_order.createOrReplaceTempView("order")

    _df_product: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "resources/product.csv"))
    )
    _df_product.createOrReplaceTempView("product")

    _df_customer: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "resources/customer.csv"))
    )

    _df_customer.createOrReplaceTempView("customer")

    return _df_order


if __name__ == "__main__":
    build_new()

    spark.sql("select * from dq_spark_local.dq_stats").show(truncate=False)
    spark.sql("select * from dq_spark_local.dq_stats").printSchema()

    _log.info("stats data in the nsp topic")
    # display posted statistics from the kafka topic
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

    # remove docker container
    current_dir = os.path.dirname(os.path.abspath(__file__))
    os.system(f"sh {current_dir}/docker_scripts/docker_nsp_stop_script.sh")
