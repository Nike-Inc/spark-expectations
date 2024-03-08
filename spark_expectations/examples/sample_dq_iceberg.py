# mypy: ignore-errors
import os

from pyspark.sql import DataFrame
from spark_expectations import _log
from spark_expectations.examples.base_setup import set_up_iceberg
from spark_expectations.core.expectations import (
    SparkExpectations,
    WrappedDataFrameWriter,
)
from spark_expectations.config.user_config import Constants as user_config

writer = WrappedDataFrameWriter().mode("append").format("iceberg")

spark = set_up_iceberg()

print(spark.sparkContext.getConf().getAll())
se: SparkExpectations = SparkExpectations(
    product_id="your_product",
    rules_df=spark.sql("select * from dq_spark_local.dq_rules"),
    stats_table="dq_spark_local.dq_stats",
    stats_table_writer=writer,
    target_and_error_table_writer=writer,
    debugger=False,
    stats_streaming_options={user_config.se_enable_streaming: False},
)

user_conf = {
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
    user_config.se_enable_error_table: True,
    user_config.se_dq_rules_params: {
        "env": "local",
        "table": "product",
    },
}


@se.with_expectations(
    target_table="dq_spark_local.customer_order",
    write_to_table=True,
    user_conf=user_conf,
    target_table_view="order",
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
    spark.sql("select * from dq_spark_local.customer_order").show(truncate=False)
    spark.sql("select count(*) from dq_spark_local.customer_order_error").show(
        truncate=False
    )

    _log.info("stats data in the kafka topic")
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
    os.system(f"sh {current_dir}/docker_scripts/docker_kafka_stop_script.sh")
