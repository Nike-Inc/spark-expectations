# Databricks notebook source
# MAGIC %md
# MAGIC ### Spark - Expectations - User - Guide - Documentation
# MAGIC * Please read through the [Spark Expectation Documentation](https://engineering.nike.com/spark-expectations) before proceeding with this demo

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mandatory Kafka(NSP) - Jar - Installation - In - The - Compute - Cluster
# MAGIC * Please install the kafka jar using the path `dbfs:/kafka-jars/databricks-shaded-strimzi-kafka-oauth-client-1.1.jar`
# MAGIC * If the jar is not available in the dbfs location, please raise a ticket with GAP Support team to add the jar to your workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create - Widgets
# MAGIC * widgets to capture database, product_id, stats_table_name and table_name

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC # below code to create widgets of database, stats_table_name and table_name
# MAGIC dbutils.widgets.text("database", "default")
# MAGIC dbutils.widgets.text("product_id", "apla_nd")
# MAGIC dbutils.widgets.text("rule_table_name", "dq_rules")
# MAGIC dbutils.widgets.text("stats_table_name", "dq_stats")
# MAGIC dbutils.widgets.text("table_name", "employee_new")
# MAGIC # dbutils.widgets.dropdown("database", "default", [database[0] for database in spark.catalog.listDatabases()]) -- limit 1024
# MAGIC
# MAGIC # get the values from widgets
# MAGIC database = dbutils.widgets.get("database")
# MAGIC product_id = dbutils.widgets.get("product_id")
# MAGIC rule_table_name = dbutils.widgets.get("rule_table_name")
# MAGIC stats_table_name = dbutils.widgets.get("stats_table_name")
# MAGIC table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install - Required - Libraries
# MAGIC * Need to install two libraries in order to use the Spark Expectations engine: Spark Expectations and Pluggy
# MAGIC
# MAGIC `Note: specific command can only be used in a DBS environment for a version below 11.0. It suggests install spark-expectations libaray on running cluster`

# COMMAND ----------

# MAGIC %sh
# MAGIC # pip install -U spark-expectations --extra-index-url https://artifactory.nike.com/artifactory/api/pypi/python-local/simple
# MAGIC # pip install "git+https://github.com/Nike-Inc/spark-expectations.git"
# MAGIC pip install spark-expectations
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configurations
# MAGIC * The configuration involves variables related to email and Slack notifications
# MAGIC * Modify or insert appropriate values as required 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perquisite(Rules & Stats - Tables)
# MAGIC * For the Spark Expectations project, it is necessary to have a rules table and a statistics table as prerequisites

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- drop error and final table if exists
# MAGIC drop table if exists ${database}.${table_name};
# MAGIC drop table if exists ${database}.${table_name}_error;
# MAGIC
# MAGIC -- drop if rules table exists
# MAGIC drop table if exists ${database}.${rule_table_name};
# MAGIC
# MAGIC -- create rules table 
# MAGIC create table if not exists ${database}.${rule_table_name} (
# MAGIC     product_id STRING,
# MAGIC     table_name STRING,
# MAGIC     rule_type STRING,
# MAGIC     rule STRING,
# MAGIC     column_name STRING,
# MAGIC     expectation STRING,
# MAGIC     action_if_failed STRING,
# MAGIC     tag STRING,
# MAGIC     description STRING,
# MAGIC     enable_for_source_dq_validation BOOLEAN, 
# MAGIC     enable_for_target_dq_validation BOOLEAN,
# MAGIC     is_active BOOLEAN,
# MAGIC     enable_error_drop_alert BOOLEAN,
# MAGIC     error_drop_threshold INT
# MAGIC )
# MAGIC     USING delta; 
# MAGIC     
# MAGIC --set constraints on rules table 
# MAGIC ALTER TABLE ${database}.${rule_table_name} ADD CONSTRAINT rule_type_action CHECK (rule_type in ('row_dq', 'agg_dq', 'query_dq'));
# MAGIC ALTER TABLE ${database}.${rule_table_name} ADD CONSTRAINT action CHECK ((rule_type = 'row_dq' and action_if_failed IN ('ignore', 'drop', 'fail')) or 
# MAGIC (rule_type = 'agg_dq' and action_if_failed in ('ignore', 'fail')) or (rule_type = 'query_dq' and action_if_failed in ('ignore', 'fail')));
# MAGIC
# MAGIC -- drop if statistics table exists
# MAGIC drop table if exists ${database}.${stats_table_name};

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Dataset - Used 
# MAGIC * The dataset used in the practice session was either obtained or artificially generated for this purpose

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC from spark_expectations.examples.read_example_dataset import spark_expectations_read_order_dataset
# MAGIC
# MAGIC spark_expectations_read_order_dataset().createOrReplaceTempView("order")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from order;

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC from spark_expectations.examples.read_example_dataset import spark_expectations_read_order_dataset
# MAGIC
# MAGIC spark_expectations_read_order_dataset().createOrReplaceTempView("product")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from product;

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC from spark_expectations.examples.read_example_dataset import spark_expectations_read_customer_dataset
# MAGIC
# MAGIC spark_expectations_read_order_dataset().createOrReplaceTempView("customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set - Data - Quality -Expectations - Rules
# MAGIC * Established rules for the employee dataset
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into table ${database}.${rule_table_name} values
# MAGIC     ('${product_id}', '${database}.${table_name}',  'row_dq', 'customer_id_is_not_null', 'customer_id', 'customer_id is not null','drop', 'completeness', 'customer_id ishould not be null', true, true, true, false, 0)
# MAGIC     ,('${product_id}', '${database}.${table_name}', 'row_dq', 'sales_greater_than_zero', 'sales', 'sales > 0', 'drop', 'accuracy', 'sales value should be greater than zero', true, true, true, false, 0)
# MAGIC     ,('${product_id}', '${database}.${table_name}', 'row_dq', 'discount_threshold', 'discount', 'discount*100 < 60','drop', 'accuracy', 'discount should be less than 40', true, true, true, false, 0)
# MAGIC     ,('${product_id}', '${database}.${table_name}', 'row_dq', 'ship_mode_in_set', 'ship_mode', 'lower(trim(ship_mode)) in(\'second class\', \'standard class\')', 'drop', 'accuracy', 'ship_mode mode belongs in the sets', true, true, true, false, 0)
# MAGIC     ,('${product_id}', '${database}.${table_name}', 'row_dq', 'profit_threshold', 'profit', 'profit>0', 'ignore', 'accuracy', 'profit threshold should be greater tahn 0', true, true, true, false, 0)
# MAGIC     
# MAGIC     ,('${product_id}', '${database}.${table_name}', 'agg_dq', 'sum_of_sales', 'sales', 'sum(sales)>10000', 'ignore', 'accuracy', 'sum of sales should be greater than 10000',  true, true, true, false, 0)
# MAGIC     ,('${product_id}', '${database}.${table_name}', 'agg_dq', 'sum_of_quantity', 'quantity', 'sum(quantity)>100', 'ignore', 'accuracy', 'sum of quantity should be greater than 100', true, true, true, false, 0)
# MAGIC     ,('${product_id}', '${database}.${table_name}', 'agg_dq', 'distinct_of_ship_mode', 'ship_mode', 'count(distinct ship_mode)<=3', 'ignore', 'accuracy', 'distinct ship_mode should be less than or equal to 3', true, true, true, false, 0)
# MAGIC     ,('${product_id}', '${database}.${table_name}', 'agg_dq', 'row_count', '*', 'count(*)>=10000', 'ignore', 'validity', 'row count of the dataset', true, true, true, false, 0)
# MAGIC
# MAGIC     ,('${product_id}', '${database}.${table_name}', 'query_dq', 'product_missing_count_threshold', '*', '((select count(distinct product_id) from product) - (select count(distinct product_id) from order))>(select count(distinct product_id) from product)*0.2', 'ignore', 'validity', 'product id count in order table should always be greater than 80% ', true, true, true, false, 0)
# MAGIC     ,('${product_id}', '${database}.${table_name}', 'query_dq', 'product_category', '*', '(select count(distinct category) from product) < 5', 'ignore', 'accuracy', 'distinct product category', true, true, true, false, 0)
# MAGIC     ,('${product_id}', '${database}.${table_name}', 'query_dq', 'row_count_in_order', '*', '(select count(*) from order)<10000', 'ignore', 'accuracy', 'count of the row in order dataset', true, true, true, false, 0)
# MAGIC     ,('${product_id}', '${database}.${table_name}', 'query_dq', 'profit_and_quanity_comp_in_order', '*', '(with clean_data as(
# MAGIC select case when profit>0 and quantity>0 then 0 else 1 end as error_record
# MAGIC from order
# MAGIC )
# MAGIC select count(*)
# MAGIC from clean_data
# MAGIC where error_record>0)<0', 'ignore', 'accuracy', 'ship_date should always be greater than or equal to order_date', true, true, true, false, 0)
# MAGIC ,('${product_id}', '${database}.${table_name}', 'query_dq', 'referential_check_between_order_and_product', '*', '(with clean_data as(
# MAGIC select count(order.product_id) as unknow_product_id
# MAGIC from order left join product
# MAGIC on order.product_id = product.product_id
# MAGIC where product.product_id is null
# MAGIC )
# MAGIC select max(unknow_product_id)
# MAGIC from clean_data)<0', 'ignore', 'validity', 'referential intergrity betweeen order and product', true, true, true, false, 0)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${database}.${rule_table_name} order by rule_type;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality dimensions or tag
# MAGIC * Uniqueness: 
# MAGIC  Uniqueness ensures every record represent a unique identity and high uniqueness score represents lesser duplicates and overlaps. It is measured against all records within a dataset. 
# MAGIC  
# MAGIC * Completeness:
# MAGIC  Completeness helps us to understand if all the required data is available in the consumption layer.
# MAGIC
# MAGIC   Column level completeness: If any critical columns are null, it should be tagged under completeness failure and completeness percentage should also reflect the failure
# MAGIC
# MAGIC  Table level completeness:  Did all the records in the source made into target level
# MAGIC
# MAGIC * Accuracy:
# MAGIC
# MAGIC Accuracy defines how well the data depicts correctness. Checks whether the data contains any values that are outside the predefined dataset
# MAGIC
# MAGIC * Validity:
# MAGIC
# MAGIC Validity defines whether the data comply with the business requirements or follows the specific format or range
# MAGIC
# MAGIC * Consistency :
# MAGIC
# MAGIC Consistency refers to data values, logic or calculations being aligned to their standards and definitions regardless of source. Consistency checks for whether data deviate too much from current baseline

# COMMAND ----------

# MAGIC %md
# MAGIC #### action_if_failed
# MAGIC * If the `action_if_failed` parameter is set to ignore for an expectation, any failed records for that expectation will be stored in the `_error_table` and `_final_table` categories
# MAGIC * If the `action_if_failed` parameter is set to drop for an expectation, any failed records for that expectation will be stored only in the `_erro_tabler` category
# MAGIC * If the `action_if_failed` parameter is set to fail for an expectation, and any record fails for that expectation, the Spark Expectations engine will also fail

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initialise & Run - Spark-Expectations - FrameWork
# MAGIC * Modify and configure the Spark Expectations decorator as needed

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install cerberus-python-client

# COMMAND ----------

# DBTITLE 0,Initialise & Run Spark-Expectations  
# MAGIC %python
# MAGIC from pyspark.sql import DataFrame
# MAGIC # from spark_expectations.core import spark
# MAGIC from spark_expectations.core.expectations import SparkExpectations
# MAGIC from spark_expectations.config.user_config import Constants as user_config
# MAGIC from spark_expectations.examples.read_example_dataset import (spark_expectations_read_order_dataset, spark_expectations_read_product_dataset, spark_expectations_read_customer_dataset)
# MAGIC
# MAGIC df_build_new: SparkExpectations = SparkExpectations(product_id="apla_nd", 
# MAGIC                                                     debugger=False,
# MAGIC                                                     )
# MAGIC
# MAGIC notification_conf = {
# MAGIC     user_config.se_notifications_enable_email: False,
# MAGIC     user_config.se_notifications_email_smtp_host: "mailhost.nike.com",
# MAGIC     user_config.se_notifications_email_smtp_port: 25,
# MAGIC     user_config.se_notifications_email_from: "",
# MAGIC     user_config.se_notifications_email_to_other_mail_id: "",
# MAGIC     user_config.se_notifications_email_subject: "spark expectations - data quality - notifications",
# MAGIC     user_config.se_notifications_enable_slack: False,
# MAGIC     user_config.se_notifications_slack_webhook_url: "",
# MAGIC     user_config.se_notifications_on_start: True,
# MAGIC     user_config.se_notifications_on_completion: True,
# MAGIC     user_config.se_notifications_on_fail: True,
# MAGIC     user_config.se_notifications_on_error_drop_exceeds_threshold_breach: True,
# MAGIC     user_config.se_notifications_on_error_drop_threshold: 15,
# MAGIC
# MAGIC }
# MAGIC
# MAGIC
# MAGIC @df_build_new.with_expectations(
# MAGIC     df_build_new.reader.get_rules_from_table(
# MAGIC         product_rules_table=f"{dbutils.widgets.get('database')}.{dbutils.widgets.get('rule_table_name')}",
# MAGIC         target_table_name=f"{dbutils.widgets.get('database')}.{dbutils.widgets.get('table_name')}",
# MAGIC         dq_stats_table_name=f"{dbutils.widgets.get('database')}.{dbutils.widgets.get('stats_table_name')}",
# MAGIC     ),
# MAGIC     write_to_table=True,
# MAGIC     row_dq=True,
# MAGIC     agg_dq={
# MAGIC         user_config.se_agg_dq: True,
# MAGIC         user_config.se_source_agg_dq: True,
# MAGIC         user_config.se_final_agg_dq: True,
# MAGIC     },
# MAGIC     query_dq={
# MAGIC         user_config.se_query_dq: True,
# MAGIC         user_config.se_source_query_dq: True,
# MAGIC         user_config.se_final_query_dq: True,
# MAGIC         user_config.se_target_table_view: "order",
# MAGIC     },
# MAGIC     spark_conf=notification_conf,
# MAGIC )
# MAGIC def build_new() -> DataFrame:
# MAGIC     _df_order: DataFrame = spark_expectations_read_order_dataset()
# MAGIC     _df_order.createOrReplaceTempView("order")
# MAGIC
# MAGIC     _df_product: DataFrame = spark_expectations_read_product_dataset()
# MAGIC     _df_product.createOrReplaceTempView("product")
# MAGIC
# MAGIC     _df_customer: DataFrame = spark_expectations_read_customer_dataset()
# MAGIC     _df_customer.createOrReplaceTempView("customer")
# MAGIC
# MAGIC     return _df_order
# MAGIC
# MAGIC build_new()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Statistics - Table
# MAGIC * The statistics for each run will be either recorded or saved in the statistics table 

# COMMAND ----------

# DBTITLE 0,Statistics Table
# MAGIC %sql
# MAGIC select * from ${database}.${stats_table_name}

# COMMAND ----------

# MAGIC %md
# MAGIC %md 
# MAGIC #### status
# MAGIC    * `source_agg`: status of agg data quality checks on source data
# MAGIC    * `row_dq`: status of the row data quality check on source data
# MAGIC    * `final_agg`: status of the agg data quality checks on clean dataset 
# MAGIC    * `run_status`: spark expectations engine or pipeline status 
# MAGIC    
# MAGIC #### % 
# MAGIC * `success_percentage`: the total number of input data records that have no issues
# MAGIC * `output_percentage`:  the total number of input data records that were included in the `_final_table`
# MAGIC * `error_percentage`:  The total number of input data records that were included in the `_error_table`
# MAGIC
# MAGIC #### Agg dq results
# MAGIC
# MAGIC * `source_agg_dq_results`: the metadata of the `agg_dq` expectations that were not met by the `source` data
# MAGIC * `final_agg_dq_results`: the metadata of the `agg_dq` expectations that were not met by the `final` data

# COMMAND ----------

# MAGIC %md
# MAGIC ### NSP - Statistics  
# MAGIC * The statistics for each run will be transmitted to a global NSP `test-us-west-2-general-v2-nike-dq-sparkexpectations-stats` topic using Spark Expectations, for the purpose of global dashboarding

# COMMAND ----------

# MAGIC %python
# MAGIC bootstrapServers = dbutils.secrets.get(scope="sole_common_prod", key="se_streaming_server_url_secret_key")
# MAGIC token = dbutils.secrets.get(scope="sole_common_prod", key="se_streaming_auth_secret_token_key")
# MAGIC oauthClientId=dbutils.secrets.get(scope="sole_common_prod", key="se_streaming_auth_secret_appid_key")
# MAGIC
# MAGIC OKTA_CLIENT_ID = dbutils.secrets.get(scope="sole_common_prod", key="se_streaming_auth_secret_appid_key")
# MAGIC client_secret = dbutils.secrets.get(scope="sole_common_prod", key="se_streaming_auth_secret_token_key")
# MAGIC oauth_endpoint_uri = dbutils.secrets.get(scope="sole_common_prod", key="se_streaming_auth_secret_token_url_key")
# MAGIC BROKER_ENDPOINT = dbutils.secrets.get(scope="sole_common_prod", key="se_streaming_server_url_secret_key")
# MAGIC TOPIC_BRONZE =  dbutils.secrets.get(scope="sole_common_prod", key="se_streaming_topic_name") 
# MAGIC # "test-us-west-2-general-v2-nike-dq-sparkexpectations-stats"
# MAGIC df = spark.read \
# MAGIC     .format("kafka") \
# MAGIC     .option("subscribe", TOPIC_BRONZE) \
# MAGIC     .option("kafka.bootstrap.servers", BROKER_ENDPOINT) \
# MAGIC     .option("kafka.security.protocol", "SASL_SSL") \
# MAGIC     .option("kafka.sasl.mechanism", "OAUTHBEARER") \
# MAGIC     .option("kafka.sasl.jaas.config", f"kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.client.id='{OKTA_CLIENT_ID}' oauth.client.secret='{client_secret}' oauth.token.endpoint.uri='{oauth_endpoint_uri}';") \
# MAGIC     .option("kafka.sasl.login.callback.handler.class", "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler") \
# MAGIC     .option('fetchOffset.numRetries', 3) \
# MAGIC     .option("maxOffsetsPerTrigger", 2000) \
# MAGIC     .option("kafka.group.id", f"{OKTA_CLIENT_ID}:3")\
# MAGIC     .option("startingOffsets", "earliest") \
# MAGIC     .option("includeHeaders", "true") \
# MAGIC     .option("failOnDataLoss", "false") \
# MAGIC     .load().selectExpr("CAST(value AS STRING) as stats_record")
# MAGIC
# MAGIC df.createOrReplaceTempView("dq_sparkexpectations_stats")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dq_sparkexpectations_stats;

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Quarantined - Error - Records
# MAGIC * Any records that are rejected by the Spark Expectations rules validation engine will be separated and stored in an error table named `${database}.${table_name}_error`
# MAGIC * The `${database}.${table_name}_error` table has an attribute called row_dq_results, which stores the metadata for one or more expectations that were not met by the rejected records
# MAGIC * In addition to the rejected records and row_dq_results metadata, the ${database}.${table_name}_error table includes additional information related to the run, such as run_id and run_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${database}.${table_name}_error;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean - DataSet
# MAGIC * Records that satisfy all expectations, as well as records that failed for expectations where action_if_failed is set to ignore, will be recorded in the final table named `${database}.${table_name}`
# MAGIC * The dataset is augmented with additional metadata generated by Spark Expectations, including run_id and run_date.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${database}.${table_name};

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup - Activity
# MAGIC * Clear all the tables that were created during the demo or practice session for Spark Expectations 

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop rulestable
# MAGIC drop table if exists ${database}.${rule_table_name};
# MAGIC
# MAGIC --drop error table
# MAGIC drop table if exists  ${database}.${table_name}_error;
# MAGIC --drop clean table
# MAGIC drop table if exists ${database}.${table_name};
# MAGIC
# MAGIC -- drop if temp table exists
# MAGIC drop table if exists ${database}.${table_name}_temp;
# MAGIC
# MAGIC --drop created temp tables 
# MAGIC drop table if exists dq_sparkexpectations_stats;
# MAGIC drop table if exists spark_expectations_employee_dataset;
