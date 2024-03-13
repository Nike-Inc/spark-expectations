
### Configurations

In order to establish the global configuration parameter for DQ Spark Expectations, you must define and complete the required fields within a variable. This involves creating a variable and ensuring that all the necessary information is provided in the appropriate fields.

```python
from spark_expectations.config.user_config import Constants as user_config

se_user_conf = {
    user_config.se_notifications_enable_email: False,  # (1)!
    user_config.se_notifications_email_smtp_host: "mailhost.com",  # (2)!
    user_config.se_notifications_email_smtp_port: 25,  # (3)!
    user_config.se_notifications_email_from: "<sender_email_id>",  # (4)!
    user_config.se_notifications_email_to_other_mail_id: "<receiver_email_id's>",  # (5)!
    user_config.se_notifications_email_subject: "spark expectations - data quality - notifications",  # (6)!
    user_config.se_notifications_enable_slack: True,  # (7)!
    user_config.se_notifications_slack_webhook_url: "<slack-webhook-url>",  # (8)!
    user_config.se_notifications_on_start: True,  # (9)!
    user_config.se_notifications_on_completion: True,  # (10)!
    user_config.se_notifications_on_fail: True,  # (11)!
    user_config.se_notifications_on_error_drop_exceeds_threshold_breach: True,  # (12)!
    user_config.se_notifications_on_error_drop_threshold: 15,  # (13)!
    user_config.se_enable_error_table: True,  # (14)!
    user_config.se_dq_rules_params: {
        "env": "local",
        "table": "product",
     }, # (15)!
}
}
```

1. The `user_config.se_notifications_enable_email` parameter, which controls whether notifications are sent via email, is set to false by default
2. The `user_config.se_notifications_email_smtp_host` parameter is set to "mailhost.com" by default and is used to specify the email SMTP domain host
3. The `user_config.se_notifications_email_smtp_port` parameter, which accepts a port number, is set to "25" by default
4. The `user_config.se_notifications_email_from` parameter is used to specify the email ID that will trigger the email notification
5. The `user_config.se_notifications_email_to_other_mail_id` parameter accepts a list of recipient email IDs
6. The `user_config.se_notifications_email_subject` parameter captures the subject line of the email
7. The `user_config.se_notifications_enable_slack` parameter, which controls whether notifications are sent via slack, is set to false by default
8. The `user_config/se_notifications_slack_webhook_url` parameter accepts the webhook URL of a Slack channel for sending notifications
9. When `user_config.se_notifications_on_start` parameter set to `True` enables notification on start of the spark-expectations, variable by default set to `False`
10. When `user_config.se_notifications_on_completion` parameter set to `True` enables notification on completion of spark-expectations framework, variable by default set to `False`
11. When `user_config.se_notifications_on_fail` parameter set to `True` enables notification on failure of spark-expectations data quality framework, variable by default set to `True`
12. When `user_config.se_notifications_on_error_drop_exceeds_threshold_breach` parameter set to `True` enables notification when error threshold reaches above the configured value
13. The `user_config.se_notifications_on_error_drop_threshold` parameter captures error drop threshold value
14. The `user_config.se_enable_error_table` parameter, which controls whether error data to load into error table, is set to true by default
15. The `user_config.se_dq_rules_params` parameter, which are required to dynamically update dq rules

### Spark Expectations Initialization 

For all the below examples the below import and SparkExpectations class instantiation is mandatory

When store for sensitive details is Databricks secret scope,construct config dictionary for authentication of Kafka and 
avoid duplicate construction every time your project is initialized, you can create a dictionary with the following keys and their appropriate values. 
This dictionary can be placed in the __init__.py file of your project or declared as a global variable.
```python
from typing import Dict, Union
from spark_expectations.config.user_config import Constants as user_config

stats_streaming_config_dict: Dict[str, Union[bool, str]] = {
    user_config.se_enable_streaming: True, # (1)!
    user_config.secret_type: "databricks", # (2)!
    user_config.dbx_workspace_url  : "https://workspace.cloud.databricks.com", # (3)!
    user_config.dbx_secret_scope: "sole_common_prod", # (4)!
    user_config.dbx_kafka_server_url: "se_streaming_server_url_secret_key", # (5)!
    user_config.dbx_secret_token_url: "se_streaming_auth_secret_token_url_key", # (6)!
    user_config.dbx_secret_app_name: "se_streaming_auth_secret_appid_key", # (7)!
    user_config.dbx_secret_token: "se_streaming_auth_secret_token_key", # (8)!
    user_config.dbx_topic_name: "se_streaming_topic_name", # (9)!
}
```

1. The `user_config.se_enable_streaming` parameter is used to control the enabling or disabling of Spark Expectations (SE) streaming functionality. When enabled, SE streaming stores the statistics of every batch run into Kafka.
2. The `user_config.secret_type` used to define type of secret store and takes two values (`databricks`, `cerberus`) by default will be `databricks`
3. The `user_config.dbx_workspace_url` used to pass Databricks workspace in the format `https://<workspace_name>.cloud.databricks.com`
4. The `user_config.dbx_secret_scope` captures name of the secret scope
5. The `user_config.dbx_kafka_server_url` captures secret key for the Kafka URL
6. The ` user_config.dbx_secret_token_url` captures secret key for the Kafka authentication app URL
7. The `user_config.dbx_secret_app_name` captures secret key for the Kafka authentication app name
8. The `user_config.dbx_secret_token` captures secret key for the Kafka authentication app secret token
9. The `user_config.dbx_topic_name` captures secret key for the Kafka topic name

Similarly when sensitive store is Cerberus: 

```python
from typing import Dict, Union
from spark_expectations.config.user_config import Constants as user_config

stats_streaming_config_dict: Dict[str, Union[bool, str]] = {
    user_config.se_enable_streaming: True, # (1)!
    user_config.secret_type: "databricks", # (2)!
    user_config.cbs_url  : "https://<url>.cerberus.com", # (3)!
    user_config.cbs_sdb_path: "cerberus_sdb_path", # (4)!
    user_config.cbs_kafka_server_url: "se_streaming_server_url_secret_sdb_path", # (5)!
    user_config.cbs_secret_token_url: "se_streaming_auth_secret_token_url_sdb_path", # (6)!
    user_config.cbs_secret_app_name: "se_streaming_auth_secret_appid_sdb_path", # (7)!
    user_config.cbs_secret_token: "se_streaming_auth_secret_token_sdb_path", # (8)!
    user_config.cbs_topic_name: "se_streaming_topic_name_sdb_path", # (9)!
}
```

1. The `user_config.se_enable_streaming` parameter is used to control the enabling or disabling of Spark Expectations (SE) streaming functionality. When enabled, SE streaming stores the statistics of every batch run into Kafka.
2. The `user_config.secret_type` used to define type of secret store and takes two values (`databricks`, `cerberus`) by default will be `databricks`
3. The `user_config.cbs_url` used to pass Cerberus URL
4. The `user_config.cbs_sdb_path` captures Cerberus secure data store path
5. The `user_config.cbs_kafka_server_url` captures path where Kafka URL stored in the Cerberus sdb
6. The ` user_config.cbs_secret_token_url` captures path where Kafka authentication app stored in the Cerberus sdb
7. The `user_config.cbs_secret_app_name` captures path where Kafka authentication app name stored in the Cerberus sdb
8. The `user_config.cbs_secret_token` captures path where Kafka authentication app name secret token stored in the Cerberus sdb
9. The `user_config.cbs_topic_name`  captures path where Kafka topic name stored in the Cerberus sdb

You can disable the streaming functionality by setting the `user_config.se_enable_streaming` parameter to `False` 

```python
from typing import Dict, Union
from spark_expectations.config.user_config import Constants as user_config

stats_streaming_config_dict: Dict[str, Union[bool, str]] = {
    user_config.se_enable_streaming: False, # (1)!
}
```

1. The `user_config.se_enable_streaming` parameter is used to control the enabling or disabling of Spark Expectations (SE) streaming functionality. When enabled, SE streaming stores the statistics of every batch run into Kafka.

```python
from spark_expectations.core.expectations import SparkExpectations

# product_id should match with the "product_id" in the rules table
se: SparkExpectations = SparkExpectations(
    product_id="your-products-id", 
    stats_streaming_options=stats_streaming_config_dict)  # (1)!
```


1. Instantiate `SparkExpectations` class which has all the required functions for running data quality rules


#### Example 1

```python
from spark_expectations.core.expectations import SparkExpectations, WrappedDataFrameWriter

writer = WrappedDataFrameWriter().mode("append").format("delta")  # (1)!
se = SparkExpectations(  # (10)!
    product_id="your_product",  # (11)!
    rules_df=spark.table("dq_spark_local.dq_rules"),  # (12)!
    stats_table="dq_spark_local.dq_stats",  # (13)!
    stats_table_writer=writer,  # (14)!
    target_and_error_table_writer=writer,  # (15)!
    debugger=False,  # (16)!
    # stats_streaming_options={user_config.se_enable_streaming: False},  # (17)!
)
@se.with_expectations(  # (2)!
    write_to_table=True,  # (3)!
    write_to_temp_table=True,  # (4)!
    user_conf=se_user_conf,  # (5)!
    target_table_view="order",  # (6)!
    target_and_error_table_writer=writer,  # (7)!
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

    _df_customer.createOrReplaceTempView("customer")  # (8)!

    return _df_order  # (9)!
```

1. The `WrappedDataFrameWriter` class is used to wrap the `DataFrameWriter` class and add additional functionality to it
2. The `@se.with_expectations` decorator is used to run the data quality rules
3. The `write_to_table` parameter is used to write the final data into the table. By default, it is False. This is optional, if you just want to run the data quality checks. A good example will be a staging table or temporary view.
4. The `write_to_temp_table` parameter is used to write the input dataframe into the temp table, so that it breaks the spark plan and might speed up the job in cases of complex dataframe lineage
5. The `user_conf` parameter is utilized to gather all the configurations that are associated with notifications. There are four types of notifications: notification_on_start, notification_on_completion, notification_on_fail and notification_on_error_threshold_breach.
   Enable notifications for all four stages by setting the values to `True`. By default, all four stages are set to `False`
6. The `target_table_view` parameter is used to provide the name of a view that represents the target validated dataset for implementation of `query_dq` on the clean dataset from `row_dq`
7. The `target_and_error_table_writer` parameter is used to write the final data into the table. By default, it is False. This is optional, if you just want to run the data quality checks. A good example will be a staging table or temporary view.
8. View registration can be utilized when implementing `query_dq` expectations.
9. Returning a dataframe is mandatory for the `spark_expectations` to work, if we do not return a dataframe - then an exception will be raised
10. Instantiate `SparkExpectations` class which has all the required functions for running data quality rules
11. The `product_id` parameter is used to specify the product ID of the data quality rules. This has to be a unique value
12. The `rules_df` parameter is used to specify the dataframe that contains the data quality rules
13. The `stats_table` parameter is used to specify the table name where the statistics will be written into
14. The `stats_table_writer` takes in the configuration that need to be used to write the stats table using pyspark
15. The `target_and_error_table_writer` takes in the configuration that need to be used to write the target and error table using pyspark
16. The `debugger` parameter is used to enable the debugger mode
17. The `stats_streaming_options` parameter is used to specify the configurations for streaming statistics into Kafka. To not use Kafka, uncomment this.
