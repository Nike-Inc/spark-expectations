
### Configurations

In order to establish the global configuration parameter for DQ Spark Expectations, you must define and complete the required fields within a variable. This involves creating a variable and ensuring that all the necessary information is provided in the appropriate fields.

```python
from spark_expectations.config.user_config import Constants as user_config

se_user_conf = {
    user_config.se_notifications_enable_email: False,  # (1)!
    user_config.se_notifications_enable_smtp_server_auth: False, # (2)!
    user_config.se_notifications_enable_custom_email_body: False, # (3)
    user_config.se_notifications_email_smtp_host: "mailhost.com",  # (4)!
    user_config.se_notifications_email_smtp_port: 25,  # (5)!
    user_config.se_notifications_smtp_password: "your_password",# (6)!
    # user_config.se_notifications_smtp_creds_dict: {
    #     user_config.secret_type: "cerberus",
    #     user_config.cbs_url: "https://cerberus.example.com",
    #     user_config.cbs_sdb_path: "your_sdb_path",
    #     user_config.cbs_smtp_password: "your_smtp_password",
    # }, # (7)!
    user_config.se_notifications_email_from: "<sender_email_id>",  # (8)!
    user_config.se_notifications_email_to_other_mail_id: "<receiver_email_id's>",  # (9)!
    user_config.se_notifications_email_subject: "spark expectations - data quality - notifications",  # (10)!
    user_config.se_notifications_email_custom_body: "custom stats: 'product_id': {}", # (11)!
    user_config.se_notifications_enable_slack: True,  # (12)!
    user_config.se_notifications_slack_webhook_url: "<slack-webhook-url>",  # (13)!
    user_config.se_notifications_on_start: True,  # (14)!
    user_config.se_notifications_on_completion: True,  # (15)!
    user_config.se_notifications_on_fail: True,  # (16)!
    user_config.se_notifications_on_error_drop_exceeds_threshold_breach: True,  # (17)!
    user_config.se_notifications_on_rules_action_if_failed_set_ignore: True,  # (18)! 
   user_config.se_notifications_on_error_drop_threshold: 15,  # (19)!
    user_config.se_enable_error_table: True,  # (20)!
    user_config.enable_query_dq_detailed_result: True, # (21)!
    user_config.enable_agg_dq_detailed_result: True, # (22)!
    user_config.querydq_output_custom_table_name: "<catalog.schema.table-name>", #23
    user_config.se_dq_rules_params: {
        "env": "local",
        "table": "product",
     }, # (24)!
     user_config.se_notifications_enable_templated_basic_email_body: True, # (25)!
     user_config.se_notifications_default_basic_email_template: "", # (26)!
}
```

1. The `user_config.se_notifications_enable_email` parameter, which controls whether notifications are sent via email, is set to false by default
2. The `user_config.se_notifications_enable_smtp_server_auth` optional parameter, which controls whether SMTP server authentication is enabled, is set to false by default
3. The `user_config.se_notifications_enable_custom_email_body` optional parameter, which controls whether custom email body is enabled, is set to false by default
4. The `user_config.se_notifications_email_smtp_host` parameter is set to "mailhost.com" by default and is used to specify the email SMTP domain host
5. The `user_config.se_notifications_email_smtp_port` parameter, which accepts a port number, is set to "25" by default
6. The `user_config.se_notifications_smtp_password` parameter is used to specify the password for the SMTP server (if smtp_server requires authentication either this parameter or `user_config.se_notifications_smtp_creds_dict` should be set)
7. The `user_config.se_notifications_smtp_creds_dict` parameter is used to specify the credentials for the SMTP server (if smtp_server requires authentication either this parameter or `user_config.se_notifications_smtp_password` should be set)
8. The `user_config.se_notifications_email_from` parameter is used to specify the email ID that will trigger the email notification
9. The `user_config.se_notifications_email_to_other_mail_id` parameter accepts a list of recipient email IDs
10. The `user_config.se_notifications_email_subject` parameter captures the subject line of the email
11. The `user_config.se_notifications_email_custom_body` optional parameter, captures the custom email body, need to be compliant with certain syntax
12. The `user_config.se_notifications_enable_slack` parameter, which controls whether notifications are sent via slack, is set to false by default 
13. The `user_config/se_notifications_slack_webhook_url` parameter accepts the webhook URL of a Slack channel for sending notifications 
14. When `user_config.se_notifications_on_start` parameter set to `True` enables notification on start of the spark-expectations, variable by default set to `False`
15. When `user_config.se_notifications_on_completion` parameter set to `True` enables notification on completion of spark-expectations framework, variable by default set to `False`
16. When `user_config.se_notifications_on_fail` parameter set to `True` enables notification on failure of spark-expectations data quality framework, variable by default set to `True`
17. When `user_config.se_notifications_on_error_drop_exceeds_threshold_breach` parameter set to `True` enables notification when error threshold reaches above the configured value 
18. When `user_config.se_notifications_on_rules_action_if_failed_set_ignore` parameter set to `True` enables notification when rules action is set to ignore if failed 
19. The `user_config.se_notifications_on_error_drop_threshold` parameter captures error drop threshold value 
20. The `user_config.se_enable_error_table` parameter, which controls whether error data to load into error table, is set to true by default 
21. When `user_config.enable_query_dq_detailed_result` parameter set to `True`, enables the option to capture the query_dq detailed stats to detailed_stats table. By default set to `False`
22. When `user_config.enable_agg_dq_detailed_result` parameter set to `True`, enables the option to capture the agg_dq detailed stats to detailed_stats table. By default set to `False`
23. The `user_config.querydq_output_custom_table_name` parameter is used to specify the name of the custom query_dq output table which captures the output of the alias queries passed in the query dq expectation. Default is <stats_table>_custom_output 
24. The `user_config.se_dq_rules_params` parameter, which are required to dynamically update dq rules
25. The `user_config.se_notifications_enable_templated_basic_email_body` optional parameter is used to enable using a Jinja template for basic email notifications (notifying on job start, completion, failure, etc.)
26. The `user_config.se_notifications_default_basic_email_template` optional parameter is used to specify the Jinja template used for basic email notifications. If the provided template is blank or this option is missing (while basic email templates are enabled) a default template will be used.

In case of SMTP server authentication, the password can be passed directly with the user config or set in a secure way like Cerberus or Databricks secret.
If it is preferred to use Cerberus for secure password storage, the `user_config.se_notifications_smtp_creds_dict` parameter can be used to specify the credentials for the SMTP server in the following way:

```python
from spark_expectations.config.user_config import Constants as user_config

smtp_creds_dict = {
    user_config.secret_type: "cerberus", # (1)!
    user_config.cbs_url: "https://.example.com", # (2)!
    user_config.cbs_sdb_path: "your_sdb_path", # (3)!
    user_config.cbs_smtp_password: "your_smtp_password", # (4)!
    }
```

1. The `user_config.secret_type` used to define type of secret store and takes two values (`databricks`, `cerberus`)
2. The `user_config.cbs_url` used to pass Cerberus URL 
3. The `user_config.cbs_sdb_path` captures Cerberus secure data store path 
4. The `user_config.cbs_smtp_password` captures key for smtp_password in the Cerberus sdb

Similarly, if it is preferred to use Databricks for secure password storage, the `user_config.se_notifications_smtp_creds_dict` parameter can be used to specify the credentials for the SMTP server in the following way:

```python
from spark_expectations.config.user_config import Constants as user_config

smtp_creds_dict = {
    user_config.secret_type: "databricks", # (1)!
    user_config.dbx_workspace_url: "https://workspace.cloud.databricks.com", # (2)!
    user_config.dbx_secret_scope: "your_secret_scope", # (3)!
    user_config.dbx_smtp_password: "your_password", # (4)!
    }
```

1. The `user_config.secret_type` used to define type of secret store and takes two values (`databricks`, `cerberus`)
2. The `user_config.dbx_workspace_url` used to pass Databricks workspace in the format `https://<workspace_name>.cloud.databricks.com`
3. The `user_config.dbx_secret_scope` captures name of the secret scope
4. The `user_config.dbx_smtp_password` captures secret key for smtp password in the Databricks secret scope

```python
### Spark Expectations Initialization 

For all the below examples the below import and SparkExpectations class instantiation is mandatory

When store for sensitive details is Databricks secret scope, construct config dictionary for authentication of Kafka and 
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
6. The `user_config.cbs_secret_token_url` captures path where Kafka authentication app stored in the Cerberus sdb
7. The `user_config.cbs_secret_app_name` captures path where Kafka authentication app name stored in the Cerberus sdb
8. The `user_config.cbs_secret_token` captures path where Kafka authentication app name secret token stored in the Cerberus sdb
9. The `user_config.cbs_topic_name`  captures path where Kafka topic name stored in the Cerberus sdb

If you are running locally or not using Cerberus or Databricks and want to specify the streaming topic name and Kafka bootstrap server you can enable these custom options by setting `user_config.se_streaming_stats_kafka_custom_config_enable` to True and then providing the below parameters to specify the topic name and server. If `user_config.se_streaming_stats_kafka_custom_config_enable` is set to True but no options are specified, the defaults from `spark_expectations/config/spark-expectations-default-config.yaml` will be used.

Please note that the specified streaming topic and Kafka bootstrap server have to exist when running Spark Expectations (they will not be generated for you).

```python
from typing import Dict, Union
from spark_expectations.config.user_config import Constants as user_config

stats_streaming_config_dict: Dict[str, Union[bool, str]] = {
    user_config.se_enable_streaming: True, # (1)!
    user_config.se_streaming_stats_kafka_custom_config_enable: True, # (2)!
    user_config.se_streaming_stats_topic_name: "dq-sparkexpectations-stats", # (3)!
    user_config.se_streaming_stats_kafka_bootstrap_server: "localhost:9092", # (4)!
}
```

1. The `user_config.se_enable_streaming` parameter is used to control the enabling or disabling of Spark Expectations (SE) streaming functionality. When enabled, SE streaming stores the statistics of every batch run into Kafka.
2. The `user_config.se_streaming_stats_kafka_custom_config_enable` is an optional parameter that, when set to True, enables using `user_config.se_streaming_stats_topic_name` to set the streaming topic name and `user_config.se_streaming_stats_kafka_bootstrap_server` to set the Kafka bootstrap server. If this parameter is set to True but no values are set for the topic name or the bootstrap server, defaults from `spark_expectations/config/spark-expectations-default-config.yaml` will be used.
3. The `user_config.se_streaming_stats_topic_name` parameter is used to set the streaming topic name when enabled with setting `user_config.se_streaming_stats_kafka_custom_config_enable` to True.
4. The `user_config.se_streaming_stats_kafka_bootstrap_server` parameter is used to set the kafka bootstrap server when enabled with setting `user_config.se_streaming_stats_kafka_custom_config_enable` to True.

You can disable the streaming functionality by setting the `user_config.se_enable_streaming` parameter to `False` 

```python
from typing import Dict, Union
from spark_expectations.config.user_config import Constants as user_config

stats_streaming_config_dict: Dict[str, Union[bool, str]] = {
    user_config.se_enable_streaming: False, # (1)!
}
```

1. The `user_config.se_enable_streaming` parameter is used to control the enabling or disabling of Spark Expectations (SE) streaming functionality. When enabled, SE streaming stores the statistics of every batch run into Kafka.

## Enhanced Kafka Error Handling

Spark Expectations now provides comprehensive error handling for Kafka streaming operations with enhanced notifications. When Kafka write operations fail, detailed error information is captured and included in all notifications to help with troubleshooting.

### Kafka Write Status Tracking

The framework automatically tracks the status of Kafka write operations and provides detailed error information:

- **kafka_write_status**: Reports the current state of Kafka operations
  - `Success`: Statistics were successfully written to Kafka
  - `Failed`: Write operation failed (detailed error included)
  - `Disabled`: Kafka streaming is disabled in configuration

- **kafka_write_error**: When status is `Failed`, provides specific error details such as:
  - Connection timeout errors
  - Authentication failures
  - Topic not found errors
  - Broker unavailability

### Enhanced Notification Content

All notification channels (email, Slack, PagerDuty, Teams, Zoom) now include Kafka status information in their messages. This enhancement helps teams quickly distinguish between data quality issues and Kafka infrastructure problems.

**Example notification content:**
```
Spark expectations job has been failed

product_id: your-product-id
table_name: customer_orders
run_id: run_20241117_143022
run_date: 2024-11-17 14:30:22
input_count: 10000
error_percentage: 5.2
kafka_write_status: Failed
kafka_write_error: Connection timeout to Kafka broker at localhost:9092
status: row_dq_status = fail
        run_status = fail
```

### Common Kafka Error Scenarios

| Error Pattern | Likely Cause | Recommended Solution |
|---------------|--------------|---------------------|
| `Connection timeout` | Network issues or broker unavailable | Verify broker URLs and network connectivity |
| `Authentication failed` | Invalid credentials | Check authentication tokens and certificates |
| `Topic not found` | Missing topic | Create the topic or verify topic name |
| `Broker unavailable` | Kafka service issues | Check Kafka cluster health and restart if needed |

### Troubleshooting Best Practices

1. **Monitor notifications** - Check `kafka_write_status` and `kafka_write_error` in job notifications
2. **Validate configuration** - Test Kafka connectivity before deploying to production
3. **Infrastructure monitoring** - Set up alerts based on `kafka_write_status` to catch issues early
4. **Error recovery** - Implement appropriate retry mechanisms for transient Kafka failures
5. **Separate concerns** - Use Kafka status to distinguish between DQ failures and infrastructure issues

This enhanced error handling significantly improves the observability and reliability of your data quality pipelines.

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