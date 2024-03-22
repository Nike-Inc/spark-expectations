### Example - Write to Delta

Setup SparkSession for BigQuery to test in your local environment. Configure accordingly for higher environments.
Refer to Examples in [base_setup.py](../spark_expectations/examples/base_setup.py) and
[delta.py](../spark_expectations/examples/sample_dq_bigquery.py)

```python title="spark_session"
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder.config(
        "spark.jars.packages",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.30.0",
    )
)
spark = builder.getOrCreate()

spark._jsc.hadoopConfiguration().set(
    "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
)
spark.conf.set("viewsEnabled", "true")
spark.conf.set("materializationDataset", "<temp_dataset>")
```

Below is the configuration that can be used to run SparkExpectations and write to Delta Lake

```python title="iceberg_write"
import os
from pyspark.sql import DataFrame
from spark_expectations.core.expectations import (
    SparkExpectations,
    WrappedDataFrameWriter,
)
from spark_expectations.config.user_config import Constants as user_config

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = "path_to_your_json_credential_file"  # This is needed for spark write to bigquery
writer = (
    WrappedDataFrameWriter().mode("overwrite")
    .format("bigquery")
    .option("createDisposition", "CREATE_IF_NEEDED")
    .option("writeMethod", "direct")
)

se: SparkExpectations = SparkExpectations(
    product_id="your_product",
    rules_df=spark.read.format("bigquery").load(
        "<project_id>.<dataset_id>.<rules_table>"
    ),
    stats_table="<project_id>.<dataset_id>.<stats_table>",
    stats_table_writer=writer,
    target_and_error_table_writer=writer,
    debugger=False,
    stats_streaming_options={user_config.se_enable_streaming: False}
)


# Commented fields are optional or required when notifications are enabled
user_conf = {
    user_config.se_notifications_enable_email: False,
    # user_config.se_notifications_email_smtp_host: "mailhost.com",
    # user_config.se_notifications_email_smtp_port: 25,
    # user_config.se_notifications_email_from: "",
    # user_config.se_notifications_email_to_other_mail_id: "",
    # user_config.se_notifications_email_subject: "spark expectations - data quality - notifications",
    user_config.se_notifications_enable_slack: False,
    # user_config.se_notifications_slack_webhook_url: "",
    # user_config.se_notifications_on_start: True,
    # user_config.se_notifications_on_completion: True,
    # user_config.se_notifications_on_fail: True,
    # user_config.se_notifications_on_error_drop_exceeds_threshold_breach: True,
    # user_config.se_notifications_on_error_drop_threshold: 15,
    # user_config.se_enable_error_table: True,
    # user_config.se_dq_rules_params: { "env": "local", "table": "product", },
}


@se.with_expectations(
    target_table="<project_id>.<dataset_id>.<target_table_name>",
    write_to_table=True,
    user_conf=user_conf,
    target_table_view="<project_id>.<dataset_id>.<target_table_view_name>",
)
def build_new() -> DataFrame:
    _df_order: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "resources/order.csv"))
    )
    _df_order.createOrReplaceTempView("order")

    return _df_order
```
