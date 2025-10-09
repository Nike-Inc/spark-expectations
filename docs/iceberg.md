### Example - Write to Delta

Setup SparkSession for iceberg to test in your local environment. Configure accordingly for higher environments.
Refer to Examples in [base_setup.py](https://github.com/Nike-Inc/spark-expectations/blob/main/spark_expectations/examples/base_setup.py) and
[iceberg.py](https://github.com/Nike-Inc/spark-expectations/blob/main/spark_expectations/examples/sample_dq_iceberg.py)

```python title="spark_session"
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder.config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1",
    )
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.iceberg.spark.SparkSessionCatalog",
    )
    .config("spark.sql.catalog.spark_catalog.type", "hadoop")
    .config("spark.sql.catalog.spark_catalog.warehouse", "/tmp/hive/warehouse")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "/tmp/hive/warehouse")
)
spark = builder.getOrCreate()
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

writer = WrappedDataFrameWriter().mode("append").format("iceberg")

se: SparkExpectations = SparkExpectations(
    product_id="your_product",
    rules_df=spark.sql("select * from dq_spark_local.dq_rules"),
    stats_table="dq_spark_local.dq_stats",
    stats_table_writer=writer,
    target_and_error_table_writer=writer,
    debugger=False,
    stats_streaming_options={user_config.se_enable_streaming: False},
)

#if smtp server needs to be authenticated, password can be passed directly with user config or set in a secure way like cerberus or databricks secret
smtp_creds_dict = {
    user_config.secret_type: "cerberus",
    user_config.cbs_url: "https://cerberus.example.com",
    user_config.cbs_sdb_path: "",
    user_config.cbs_smtp_password: "",
    # user_config.secret_type: "databricks",
    # user_config.dbx_workspace_url: "https://workspace.cloud.databricks.com",
    # user_config.dbx_secret_scope: "your_secret_scope",
    # user_config.dbx_smtp_password: "your_password",
}

# Commented fields are optional or required when notifications are enabled
user_conf = {
    user_config.se_notifications_enable_email: False,
    # user_config.se_notifications_enable_smtp_server_auth: False,
    # user_config.se_notifications_enable_custom_email_body: True,
    # user_config.se_notifications_email_smtp_host: "mailhost.com",
    # user_config.se_notifications_email_smtp_port: 25,
    # user_config.se_notifications_smtp_password: "your_password",
    # user_config.se_notifications_smtp_creds_dict: smtp_creds_dict,
    # user_config.se_notifications_email_from: "",
    # user_config.se_notifications_email_to_other_mail_id: "",
    # user_config.se_notifications_email_subject: "spark expectations - data quality - notifications",
    # user_config.se_notifications_email_custom_body: "Custom statistics: 'product_id': {}",
    user_config.se_notifications_enable_slack: False,
    # user_config.se_notifications_slack_webhook_url: "",
    # user_config.se_notifications_on_start: True,
    # user_config.se_notifications_on_completion: True,
    # user_config.se_notifications_on_fail: True,
    # user_config.se_notifications_on_error_drop_exceeds_threshold_breach: True,
    # user_config.se_notifications_on_error_drop_threshold: 15,
    # user_config.se_enable_error_table: True,
    # user_config.enable_query_dq_detailed_result: True,
    # user_config.enable_agg_dq_detailed_result: True,
    # user_config.se_dq_rules_params: { "env": "local", "table": "product", },
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

    return _df_order
```
