
### Configurations

In order to establish the global configuration parameter for DQ Spark Expectations, you must define and complete the required fields within a variable. This involves creating a variable and ensuring that all the necessary information is provided in the appropriate fields.

```python
from spark_expectations.config.user_config import Constants as user_config

se_global_spark_Conf = {
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
}
```

1. The `user_config.se_notifications_enable_email` parameter, which controls whether notifications are sent via email, is set to false by default
2. The `user_config.se_notifications_email_smtp_host` parameter is set to "mailhost.com" by default and is used to specify the email SMTP domain host
3. The `user_config.se_notifications_email_smtp_port` parameter, which accepts a port number, is set to "25" by default
4. The `user_config.se_notifications_email_from` parameter is used to specify the email ID that will trigger the email notification
5. The `user_configse_notifications_email_to_other_mail_id` parameter accepts a list of recipient email IDs
6. The `user_config.se_notifications_email_subject` parameter captures the subject line of the email
7. The `user_config.se_notifications_enable_slack` parameter, which controls whether notifications are sent via slack, is set to false by default
8. The `user_config/se_notifications_slack_webhook_url` parameter accepts the webhook URL of a Slack channel for sending notifications
9. When `user_config.se_notifications_on_start` parameter set to `True` enables notification on start of the spark-expectations, variable by default set to `False`
10. When `user_config.se_notifications_on_completion` parameter set to `True` enables notification on completion of spark-expectations framework, variable by default set to `False`
11. When `user_config.se_notifications_on_fail` parameter set to `True` enables notification on failure of spark-expectations data qulaity framework, variable by default set to `True`
12. When `user_config.se_notifications_on_error_drop_exceeds_threshold_breach` parameter set to `True` enables notification when error threshold reaches above the configured value
13. The `user_config.se_notifications_on_error_drop_threshold` parameter captures error drop threshold value

### Spark Expectations Initialization 

For all the below examples the below import and SparkExpectations class instantiation is mandatory

When store for sensitive details is Databricks secret scope,construct config dictionary for authentication of kafka and 
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
2. The `user_config.secret_type` used to define type of secret store and takes two values (`databricks`, `cererus`) by default will be `databricks`
3. The `user_config.dbx_workspace_url` used to pass databricks workspace in the format `https://<workspace_name>.cloud.databricks.com`
4. The `user_config.dbx_secret_scope` captures name of the secret scope
5. The `user_config.dbx_kafka_server_url` captures secret key for the kafka url
6. The ` user_config.dbx_secret_token_url` captures secret key for the kafka authentication app url
7. The `user_config.dbx_secret_app_name` captures secret key for the kafka authentication app name
8. The `user_config.dbx_secret_token` captures secret key for the kafka authentication app secret token
9. The `user_config.dbx_topic_name` captures secret key for the kafka topic name

Similarly when sensitive store is cerberus: 

```python
from typing import Dict, Union
from spark_expectations.config.user_config import Constants as user_config

stats_streaming_config_dict: Dict[str, Union[bool, str]] = {
                user_config.se_enable_streaming: True, # (1)!
                user_config.secret_type: "databricks", # (2)!
                user_config.cbs_url  : "https://<url>.cerberus.com", # (3)!
                user_config.cbs_sdb_path: "cerberus_sdb_path", # (4)!
                user_config.cbs_kafka_server_url: "se_streaming_server_url_secret_sdb_path", # (5)!
                user_config.cbs_secret_token_url: "se_streaming_auth_secret_token_url_sdb_apth", # (6)!
                user_config.cbs_secret_app_name: "se_streaming_auth_secret_appid_sdb_path", # (7)!
                user_config.cbs_secret_token: "se_streaming_auth_secret_token_sdb_path", # (8)!
                user_config.cbs_topic_name: "se_streaming_topic_name_sdb_path", # (9)!
            }
```

1. The `user_config.se_enable_streaming` parameter is used to control the enabling or disabling of Spark Expectations (SE) streaming functionality. When enabled, SE streaming stores the statistics of every batch run into Kafka.
2. The `user_config.secret_type` used to define type of secret store and takes two values (`databricks`, `cererus`) by default will be `databricks`
3. The `user_config.cbs_url` used to pass cerberus url
4. The `user_config.cbs_sdb_path` captures cerberus secure data store path
5. The `user_config.cbs_kafka_server_url` captures path where kafka url stored in the cerberus sdb
6. The ` user_config.cbs_secret_token_url` captures path where kafka authentication app stored in the cerberus sdb
7. The `user_config.cbs_secret_app_name` captures path where kafka authentication app name stored in the cerberus sdb
8. The `user_config.cbs_secret_token` captures path where kafka authentication app name secret token stored in the cerberus sdb
9. The `user_config.cbs_topic_name`  captures path where kafka topic name stored in the cerberus sdb

```python
from spark_expectations.core.expectations import SparkExpectations

# product_id should match with the "product_id" in the rules table
se: SparkExpectations = SparkExpectations(product_id="your-products-id", stats_streaming_options=stats_streaming_config_dict)  # (1)!
```


1. Instantiate `SparkExpectations` class which has all the required functions for running data quality rules


#### Example 1

```python
from spark_expectations.config.user_config import *  # (7)!


@se.with_expectations(  # (6)!
    se.reader.get_rules_from_df(rules_table="pilot_nonpub.dq.dq_rules", dq_stats_table="pilot_nonpub.dq.dq_stats",
                                target_table=),
    write_to_table=True,  # (4)!
    write_to_temp_table=True,  # (8)!
    row_dq=True,  # (9)!
    agg_dq={  # (10)!
        user_config.se_agg_dq: True,  # (11)!
        user_config.se_source_agg_dq: True,  # (12)!
        user_config.se_final_agg_dq: True,  # (13)!
    },
    query_dq={  # (14)!
        user_config.se_query_dq: True,  # (15)!
        user_config.se_source_query_dq: True,  # (16)!
        user_config.se_final_query_dq: True,  # (17)!
        user_config.se_target_table_view: "order",  # (18)!
    },
    user_conf=se_global_spark_Conf,  # (19)!

)
def build_new() -> DataFrame:
    _df_order: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "resources/order.csv"))
    )
    _df_order.createOrReplaceTempView("order")  # (20)!

    _df_product: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "resources/product.csv"))
    )
    _df_product.createOrReplaceTempView("product")  # (20)!

    _df_customer: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "resources/customer.csv"))
    )

    _df_customer.createOrReplaceTempView("customer")  # (20)!

    return _df_order  # (21)!
```


1. Provide the full table name of the table which contains the rules
2. Provide the table name using which the `_error` table will be created, which contains all the failed records. 
   Note if you are also wanting to write the data using `write_df`, then the table_name provided to both the functions 
   should be same
3. Provide the full table name where the stats will be written into
4. Use this argument to write the final data into the table. By default, it is False.
   This is optional, if you just want to run the data quality checks.
   A good example will be a staging table or temporary view.
5. This functions reads the rules from the table and return them as a dict, which is an input to the `with_expectations` function
6. This is the decorator that helps us run the data quality rules. After running the rules the results will be written into `_stats` table and `error` table
7. import necessary configurable variables from `user_config` package for the specific functionality to configure in spark-expectations
8. Use this argument to write the input dataframe into the temp table, so that it breaks the spark plan and might speed 
   up the job in cases of complex dataframe lineage
9. The argument row_dq is optional and enables the conducting of row-based data quality checks. By default, this 
   argument is set to True, however, if desired, these checks can be skipped by setting the argument to False.
10. The `agg_dq` argument is a dictionary that is used to gather different settings and options for the purpose of configuring the `agg_dq`
11. The argument `se_agg_dq` is utilized to activate the aggregate data quality check, and its default setting is True.
12. The `se_source_agg_dq` argument is optional and enables the conducting of aggregate-based data quality checks on the 
    source data. By default, this argument is set to True, and this option depends on the `agg_dq` value. 
    If desired, these checks can be skipped by setting the source_agg_dq argument to False.
13. This optional argument `se_final_agg_dq` allows to perform agg-based data quality checks on final data, with the 
    default setting being `True`, which depended on `row_agg` and `agg_dq`. skip these checks by setting argument to `False`
14. The `query_dq` argument is a dictionary that is used to gather different settings and options for the purpose of configuring the `query_dq`
15. The argument `se_query_dq` is utilized to activate the aggregate data quality check, and its default setting is True. 
16. The `se_source_query_dq` argument is optional and enables the conducting of query-based data quality checks on the 
    source data. By default, this argument is set to True, and this option depends on the `agg_dq` value. 
    If desired, these checks can be skipped by setting the source_agg_dq argument to False.
17. This optional argument `se_final_query_dq` allows to perform query_based data quality checks on final data, with the 
    default setting being `True`, which depended on `row_agg` and `agg_dq`. skip these checks by setting argument to `False`
18. The parameter `se_target_table_view` can be provided with the name of a view that represents the target validated dataset for implementation of `query_dq` on the clean dataset from `row_dq`
19. The `spark_conf` parameter is utilized to gather all the configurations that are associated with notifications
20. View registration can be utilized when implementing `query_dq` expectations.
21. Returning a dataframe is mandatory for the `spark_expectations` to work, if we do not return a dataframe - then an exceptionm will be raised


#### Example 2

```python
@se.with_expectations(  # (1)!
    se.reader.get_rules_from_df(rules_table="pilot_nonpub.dq.dq_rules", dq_stats_table="pilot_nonpub.dq.dq_stats",
                                target_table="pilot_nonpub.customer_order")
)

,
row_dq = True  # (2)!
)

def build_new() -> DataFrame:
    _df: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "resources/employee.csv"))
    )
    return df 
```

1. Conduct only row-based data quality checks while skipping the aggregate data quality checks
2. Disabled the aggregate data quality checks

#### Example 3

```python
@se.with_expectations(  # (1)!
    se.reader.get_rules_from_df(rules_table="pilot_nonpub.dq.dq_rules", dq_stats_table="pilot_nonpub.dq.dq_stats",
                                target_table="pilot_nonpub.customer_order"),
    row_dq=False,  # (2)!
    agg_dq={
        user_config.se_agg_dq: True,
        user_config.se_source_agg_dq: True,
        user_config.se_final_agg_dq: False,
    }

)
def build_new() -> DataFrame:
    _df: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "resources/employee.csv"))
    )
    return df 
```

1. Perform only aggregate-based data quality checks while avoiding both row-based data quality checks and aggregate data
   quality checks on the validated dataset, since row validation has not taken place
2. Disabled the row data quality checks

#### Example 4

```python
@se.with_expectations(  # (1)!
    se.reader.get_rules_from_df(rules_table="pilot_nonpub.dq.dq_rules", dq_stats_table="pilot_nonpub.dq.dq_stats",
                                target_table="pilot_nonpub.customer_order"),
    row_dq=True,
    query_dq={  # (2)!
        user_config.se_query_dq: True,
        user_config.se_source_query_dq: True,
        user_config.se_final_query_dq: True,
        user_config.se_target_table_view: "order",
    },

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
```

1. Conduct row-based and query-based data quality checks only on the source and target dataset, while skipping the aggregate 
   data quality checks on the validated dataset
2. Enabled the query data quality checks

#### Example 5

```python
@se.with_expectations(  # (1)!
    se.reader.get_rules_from_df(rules_table="pilot_nonpub.dq.dq_rules", dq_stats_table="pilot_nonpub.dq.dq_stats",
                                target_table="pilot_nonpub.customer_order"),
    row_dq=True,
    agg_dq={  # (10)!
        user_config.user_configse_agg_dq: True,
        user_config.se_source_agg_dq: True,
        user_config.se_final_agg_dq: False,  # (2)!
    },

)
def build_new() -> DataFrame:
    _df_order: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "resources/order.csv"))
    )
    return _df_order 
```

1. Conduct row-based and aggregate-based data quality checks only on the source dataset, while skipping the aggregate 
   data quality checks on the validated dataset
2. Disabled the final aggregate data quality quality checks


#### Example 6

```python
import os


@se.with_expectations(
    se.reader.get_rules_from_df(rules_table="pilot_nonpub.dq.dq_rules", dq_stats_table="pilot_nonpub.dq.dq_stats",
                                target_table="pilot_nonpub.customer_order"),
    user_conf=se_global_spark_Conf,  # (2)!

)
def build_new() -> DataFrame:
    _df_order: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "resources/order.csv"))
    )
    return _df_order 
```

1. There are four types of notifications: notification_on_start, notification_on_completion, notification_on_fail and notification_on_error_threshold_breach. 
   Enable notifications for all four stages by setting the values to `True`
2. To provide the absolute file path for a configuration variable that holds information regarding notifications, use the 
  decalared global variable, `se_global_spark_Conf`


#### Example 7

```python
@se.with_expectations(  # (1)!
    se.reader.get_rules_from_df(rules_table="pilot_nonpub.dq.dq_rules", dq_stats_table="pilot_nonpub.dq.dq_stats",
                                target_table="pilot_nonpub.customer_order"),
    row_dq=False,
    agg_dq={
        user_config.se_agg_dq: False,
        user_config.se_source_agg_dq: False,
        user_config.se_final_agg_dq: True,
    },
    query_dq={
        user_config.se_query_dq: False,
        user_config.se_source_query_dq: True,
        user_config.se_final_query_dq: True,
        user_config.se_target_table_view: "order",
    },
)
def build_new() -> DataFrame:
    _df_order: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "resources/order.csv"))
    )
    return _df_order 
```

1. Below combination of `row_dq, agg_dq, source_agg_dq, final_agg_dq, query_dq, source_query_dq and final_query_dq` skips the data quality checks because 
   source_agg_dq depends on agg_dq and final_agg_dq depends on row_dq and agg_dq


#### Example 8

```python
@se.with_expectations(  # (1)!
    se.reader.get_rules_from_df(rules_table="pilot_nonpub.dq.dq_rules", dq_stats_table="pilot_nonpub.dq.dq_stats",
                                target_table="pilot_nonpub.customer_order", actions_if_failed=["drop", "ignore"])
)
def build_new() -> DataFrame:
    _df_order: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "resources/order.csv"))
    )
    return _df_order 
```

1. By default `action_if_failed` contains ["fail", "drop", "ignore"], but if we want to run only rules which has a 
   particular action then we can pass them as list shown in the example
   

#### Example 9

```python
@se.with_expectations(  # (1)!
    se.reader.get_rules_from_df(rules_table="pilot_nonpub.dq.dq_rules", dq_stats_table="pilot_nonpub.dq.dq_stats",
                                target_table="pilot_nonpub.customer_order", actions_if_failed=["drop", "ignore"]),
    row_dq=True,  # (2)!
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
    }

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

```

1. The default options for the action_if_failed field are ["fail", "drop", or "ignore"], but you can specify which of 
   these actions to run by providing a list of the desired actions in the example when selecting which data quality rules 
   set to apply
2. Data quality rules will only be applied if they have ["drop" or "ignore"] specified in the action_if_failed field
   

#### Example 10

```python
@se.with_expectations(
    se.reader.get_rules_from_df(rules_table="pilot_nonpub.dq.dq_rules", dq_stats_table="pilot_nonpub.dq.dq_stats",
                                target_table="pilot_nonpub.customer_order"),
    user_conf={"spark.files.maxPartitionBytes": "134217728"},  # (1)!
    options={"mode": "overwrite", "partitionBy": "order_month",
             "overwriteSchema": "true"},  # (2)!
    options_error_table={"partition_by": "id"}  # (3)!
)
def build_new() -> DataFrame:
    _df_order: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "resources/order.csv"))
    )
    return _df_order
```

1. Provide the optional `spark_conf` if needed, this is used while writing the data into the `final` and `error` table along with notification related configurations
2. Provide the optional `options` if needed, this is used while writing the data into the `final` table
3. Provide the optional `options_error_table` if needed, this is used while writing the data into the `error` table