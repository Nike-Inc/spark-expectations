# Define the product_id
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from spark_expectations.notifications.push.alert import AlertTrial
from spark_expectations.core.context import SparkExpectationsContext
from pyspark.sql import SparkSession
import os
from spark_expectations.utils.reader import SparkExpectationsReader

from spark_expectations.notifications.push.alert1 import AlertTrial
from spark_expectations.core.context import SparkExpectationsContext


from pyspark.sql import DataFrame
from spark_expectations import _log
from spark_expectations.examples.base_setup import set_up_delta
from spark_expectations.core.expectations import (
    SparkExpectations,
    WrappedDataFrameWriter,
)
from spark_expectations.config.user_config import Constants as user_config


writer = WrappedDataFrameWriter().mode("append").format("delta")
product_id="your_product"
spark = set_up_delta()
dic_job_info = {
    "job": "na_CORL_DIGITAL_source_to_o9",
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
    # stats_streaming_options={user_config.se_enable_streaming: False},
)
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Create a list of sample data
data = [
    (1, "Alice", 30),
    (2, "Bob", 25),
    (3, "Cathy", 28)
]
default_df = spark.createDataFrame(data, schema)



user_conf = {
    user_config.se_notifications_enable_custom_dataframe: False,
    user_config.se_enable_obs_dq_report_result: True,
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
    user_config.se_job_metadata: job_info,}

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

    return _df_order_target
#

if __name__ == "__main__":
    se_dq_obs_alert_flag = user_conf.get(user_config.se_dq_obs_alert_flag, "False")
    se_enable_obs_dq_report_result = user_conf.get(user_config.se_enable_obs_dq_report_result, "False")

    if se_dq_obs_alert_flag is True and se_enable_obs_dq_report_result is False :
            # Create an instance of SparkExpectationsContext with the required arguments
            _context = SparkExpectationsContext(product_id, spark)
            reader = SparkExpectationsReader(_context)
            reader.set_notification_param(user_conf)
            instance = AlertTrial(_context)
            # Create an instance of AlertTrial with the context
            if (user_conf.get(user_config.se_notifications_enable_custom_dataframe)) is True:
                instance.prep_report_data()
            else:
              instance.send_mail(user_conf.get(user_config.se_notifications_email_subject),user_conf.get(user_config.se_notifications_email_custom_body),user_conf.get(user_config.se_notifications_email_to_other_mail_id))
    else:
        build_new()



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
