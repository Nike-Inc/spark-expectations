# Spark-Expectations

[![CodeQL](https://github.com/Nike-Inc/spark-expectations/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/Nike-Inc/spark-expectations/actions/workflows/codeql-analysis.yml)
[![build](https://github.com/Nike-Inc/spark-expectations/actions/workflows/onpush.yml/badge.svg)](https://github.com/Nike-Inc/spark-expectations/actions/workflows/onpush.yml)
[![codecov](https://codecov.io/gh/Nike-Inc/spark-expectations/branch/main/graph/badge.svg)](https://codecov.io/gh/Nike-Inc/spark-expectations)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
![PYPI version](https://img.shields.io/pypi/v/spark-expectations.svg)
![PYPI - Downloads](https://static.pepy.tech/badge/spark-expectations)
![PYPI - Python Version](https://img.shields.io/pypi/pyversions/spark-expectations.svg)

<p align="center">
Spark Expectations is a specialized tool designed with the primary goal of maintaining data integrity within your processing pipeline.
By identifying and preventing malformed or incorrect data from reaching the target destination, it ensues that only quality data is
passed through. Any erroneous records are not simply ignored but are filtered into a separate error table, allowing for 
detailed analysis and reporting. Additionally, Spark Expectations provides valuable statistical data on the filtered content, 
empowering you with insights into your data quality.
</p>

<p align="center">
<img src=https://github.com/Nike-Inc/spark-expectations/blob/main/docs/se_diagrams/logo.png?raw=true width="400" height="400"></p>

---

The documentation for spark-expectations can be found [here](https://engineering.nike.com/spark-expectations/latest/)

### Contributors

Thanks to all the [contributors](https://github.com/Nike-Inc/spark-expectations/blob/main/CONTRIBUTORS.md) who have helped ideate, develop and bring it to its current state 

### Contributing

We're delighted that you're interested in contributing to our project! To get started, 
please carefully read and follow the guidelines provided in our [contributing](https://github.com/Nike-Inc/spark-expectations/blob/main/CONTRIBUTING.md) document

### Change Log
Most recent updates can be found in the [GitHub Releases](https://github.com/Nike-Inc/spark-expectations/releases)

# What is Spark Expectations?
#### Spark Expectations is a Data quality framework built in PySpark as a solution for the following problem statements:

1. The existing data quality tools validates the data in a table at rest and provides the success and error metrics. Users need to manually check the metrics to identify the error records
2. The error data is not quarantined to an error table or there are no corrective actions taken to send only the valid data to downstream
3. Users further downstream must consume the same data incorrectly, or they must perform additional calculations to eliminate records that don't comply with the data quality rules.
4. Another process is required as a corrective action to rectify the errors in the data and lot of planning is usually required for this activity

#### Spark Expectations solves these issues using the following principles:

1. All the records which fail one or more data quality rules, are by default quarantined in an _error table along with the metadata on rules that failed, job information etc. This makes it easier for analysts or product teams to view the incorrect data and collaborate with the teams responsible for correcting and reprocessing it.
2. Aggregated metrics are provided for the raw data and the cleansed data for each run along with the required metadata to prevent recalculation or computation.
3. The data that doesn't meet the data quality contract or the standards is not moved to the next level or iterations unless or otherwise specified. 

---
# Features Of Spark Expectations

Please find the spark-expectations flow and feature diagrams below

<p align="center">
<img src=https://github.com/Nike-Inc/spark-expectations/blob/main/docs/se_diagrams/flow.png?raw=true width=1000></p>

<p align="center">
<img src=https://github.com/Nike-Inc/spark-expectations/blob/main/docs/se_diagrams/features.png?raw=true width=1000></p>

## Data Quality Rule Types

Spark-Expectations supports three distinct types of data quality rules:

- **Row-Level Data Quality (`row_dq`)**: Checks conditions on individual rows, such as `col1 > 10` or `col2 is not null`.
- **Aggregate Data Quality (`agg_dq`)**: Checks conditions on aggregated values, such as `sum(col3) > 20` or `avg(col1) < 25`.
- **Query-Based Data Quality (`query_dq`)**: Checks conditions using full SQL queries, such as `(select sum(col1) from test_table) > 10`.

Each rule type has its own expectation format and validation logic.

👉 **For detailed documentation and examples, see the [Data Quality Rule Types section](https://engineering.nike.com/spark-expectations/latest/user_guide/data_quality_rules/).**

## Spark Expectation Observability Feature

This feature enhances data observability by leveraging the *stats detailed table* and *custom query table* to generate a *report table* with key metrics.  

### Workflow:
1. *Automatic Trigger: The observability feature is initiated upon the completion of **Spark Expectation*, based on configurable user-defined settings.  
2. Extracts relevant data from the *stats detailed table* and *custom query table*, processes it, and creates a *report table* with key insights.  
3. Compiles and summarizes essential observability metrics.  
4. Delivers an *alert notification* via email using a *Jinja template*.  

### Template Customization:
- Users can specify a *custom Jinja template* for formatting the report.  
- If no custom template is provided, a *default Jinja template* is used to generate and send the alert.  

This feature enables proactive monitoring, providing timely insights to enhance data quality and system reliability.
For more details see the [observability documentation](https://engineering.nike.com/spark-expectations/latest/Observability_examples/), the [email alerts documentation](https://engineering.nike.com/spark-expectations/latest/user_guide/notifications/email_notifications/), and the [Slack notifications documentation](https://engineering.nike.com/spark-expectations/latest/user_guide/notifications/slack_notifications/).

## Sample Alert Notification  
Below is an example of the alert email generated by the observability feature:  

![Alert Email Screenshot](https://github.com/Nike-Inc/spark-expectations/blob/main/docs/se_diagrams/alert_sample.png?raw=true)

# Spark - Expectations Setup

### Prerequisites

You can find the developer setup instructions in the [Setup](https://engineering.nike.com/spark-expectations/latest/developer_guide/setup/) section of the documentation. This guide will help you set up your development environment and get you started with Spark Expectations.

### Configurations

In order to establish the global configuration parameter for DQ Spark Expectations, you must define and complete the 
required fields within a variable. This involves creating a variable and ensuring that all the necessary information 
is provided in the appropriate fields.

```python
from spark_expectations.config.user_config import Constants as user_config

se_user_conf = {
    user_config.se_notifications_enable_email: False,
    user_config.se_notifications_email_smtp_host: "mailhost.nike.com",
    user_config.se_notifications_email_smtp_port: 25,
    user_config.se_enable_obs_dq_report_result: True,
    user_config.se_dq_obs_alert_flag: True,
    user_config.se_dq_obs_default_email_template: "",
    user_config.se_notifications_email_from: "<sender_email_id>",
    user_config.se_notifications_email_to_other_nike_mail_id: "<receiver_email_id's>",
    user_config.se_notifications_email_subject: "spark expectations - data quality - notifications", 
    user_config.se_notifications_enable_slack: True,
    user_config.se_notifications_slack_webhook_url: "<slack-webhook-url>", 
    user_config.se_notifications_on_start: True, 
    user_config.se_notifications_on_completion: True,
    user_config.se_notifications_on_fail: True,
    user_config.se_notifications_on_error_drop_exceeds_threshold_breach: True, 
    user_config.se_notifications_on_rules_action_if_failed_set_ignore: True,
    user_config.se_notifications_on_error_drop_threshold: 15,
    #Optional
    #Below two params are optional and need to be enabled to capture the detailed stats in the <stats_table_name>_detailed.
    #user_config.enable_query_dq_detailed_result: True,
    #user_config.enable_agg_dq_detailed_result: True,
    #Below two params are optional and need to be enabled to pass the custom email body
    #user_config.se_notifications_enable_custom_email_body: True,
    #user_config.se_notifications_email_custom_body: "'product_id': {}",
    #Below two parameters are optional and are for enabling html templates for the custom email body
    #user_config.se_notifications_enable_templated_custom_email: True,
    #user_config.se_notifications_email_custom_template: "",
    #Below parameter is optional and needs to be enabled in case authorization is required to access smtp server.
    #user_config.se_notifications_email_smtp_auth: True,
    #Below parameter is optional and used to specify environment value.
    #user_config.se_dq_rules_params: {"env": "prod/dev/local" }
    #Below two parameters are optional and used to enable and specify the default template for basic email notifications.
    #user_config.se_notifications_enable_templated_basic_email_body: True
    #user_config.se_notifications_default_basic_email_template: ""
}
```

### Spark Expectations Initialization 

For all the below examples the below import and SparkExpectations class instantiation is mandatory

1. Instantiate `SparkExpectations` class which has all the required functions for running data quality rules

```python
from spark_expectations.core.expectations import SparkExpectations, WrappedDataFrameWriter
from pyspark.sql import SparkSession

spark: SparkSession = SparkSession.builder.getOrCreate()
writer = WrappedDataFrameWriter().mode("append").format("delta")
# writer = WrappedDataFrameWriter().mode("append").format("iceberg")
# product_id should match with the "product_id" in the rules table
se: SparkExpectations = SparkExpectations(
    product_id="your_product",
    rules_df=spark.table("dq_spark_local.dq_rules"),
    stats_table="dq_spark_local.dq_stats",
    stats_table_writer=writer,
    target_and_error_table_writer=writer,
    debugger=False,
    # stats_streaming_options={user_config.se_enable_streaming: False},
)
```

2. Decorate the function with `@se.with_expectations` decorator

```python
from spark_expectations.config.user_config import *
from pyspark.sql import DataFrame
import os


@se.with_expectations(
    target_table="dq_spark_local.customer_order",
    write_to_table=True,
    user_conf=se_user_conf,
    target_table_view="order",
)
def build_new() -> DataFrame:
    # Return the dataframe on which Spark-Expectations needs to be run
    _df_order: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "resources/order.csv"))
    )
    _df_order.createOrReplaceTempView("order")

    return _df_order 
```

3. Spark_expectation observability enablement


 ```python
    user_config.se_enable_obs_dq_report_result: True,
    user_config.se_dq_obs_alert_flag: True,
    user_config.se_dq_obs_default_email_template: "",
    #for alert make sure to provide all the details related to SMTP for sending mail, else it will throw an error.
    user_config.se_notifications_email_smtp_host: "mailhost.nike.com",
    user_config.se_notifications_email_smtp_port: 25,
    user_config.se_notifications_smtp_password: "************"
    user_config.se_notifications_email_smtp_host: "smtp.se.com"
    

   
```
   
### Adding Certificates

To enable trusted SSL/TLS communication during Spark-Expectations testing, you may need to provide custom Certificate Authority (CA) certificates. Place any required `.crt` files in the `containers/certs` directory. During test container startup, all certificates in this folder will be automatically imported into the container's trusted certificate store, ensuring that your Spark jobs and dependencies can establish secure connections as needed.