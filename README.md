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

The documentation for spark-expectations can be found [here](https://engineering.nike.com/spark-expectations/)

### Contributors

Thanks to all the [contributors](https://github.com/Nike-Inc/spark-expectations/blob/main/CONTRIBUTORS.md) who have helped ideate, develop and bring it to its current state 

### Contributing

We're delighted that you're interested in contributing to our project! To get started, 
please carefully read and follow the guidelines provided in our [contributing](https://github.com/Nike-Inc/spark-expectations/blob/main/CONTRIBUTING.md) document

# What is Spark Expectations?
#### Spark Expectations is a Data quality framework built in Pyspark as a solution for the following problem statements:

1. The existing data quality tools validates the data in a table at rest and provides the success and error metrics. Users need to manually check the metrics to identify the error records
2. The error data is not quarantined to an error table or there are no corrective actions taken to send only the valid data to downstream
3. Users further downstream must consume the same data incorrectly, or they must perform additional calculations to eliminate records that don't comply with the data quality rules.
4. Another process is required as a corrective action to rectify the errors in the data and lot of planning is usually required for this acitivity

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


# Spark - Expectations Setup

### Configurations

In order to establish the global configuration parameter for DQ Spark Expectations, you must define and complete the 
required fields within a variable. This involves creating a variable and ensuring that all the necessary information 
is provided in the appropriate fields.

```python
from spark_expectations.config.user_config import *

se_global_spark_Conf = {
    se_notifications_enable_email: False,
    se_notifications_email_smtp_host: "mailhost.nike.com",
    se_notifications_email_smtp_port: 25,
    se_notifications_email_from: "<sender_email_id>",
    se_notifications_email_to_other_nike_mail_id: "<receiver_email_id's>",
    se_notifications_email_subject: "spark expectations - data quality - notifications", 
    se_notifications_enable_slack: True,
    se_notifications_slack_webhook_url: "<slack-webhook-url>", 
    se_notifications_on_start: True, 
    se_notifications_on_completion: True,
    se_notifications_on_fail: True,
    se_notifications_on_error_drop_exceeds_threshold_breach: True, 
    se_notifications_on_error_drop_threshold: 15,
}
```

### Spark Expectations Initialization 

For all the below examples the below import and SparkExpectations class instantiation is mandatory

```python
from spark_expectations.core.expectations import SparkExpectations


# product_id should match with the "product_id" in the rules table
se: SparkExpectations = SparkExpectations(product_id="your-products-id")
```

1. Instantiate `SparkExpectations` class which has all the required functions for running data quality rules

```python
from spark_expectations.config.user_config import *


@se.with_expectations(
    se.reader.get_rules_from_df(rules_table="pilot_nonpub.dq.dq_rules", dq_stats_table="pilot_nonpub.dq.dq_stats",
                                target_table=),
    write_to_table=True,
    write_to_temp_table=True,
    row_dq=True,
    agg_dq={
        se_agg_dq: True,
        se_source_agg_dq: True,
        se_final_agg_dq: True,
    },
    query_dq={
        se_query_dq: True,
        se_source_query_dq: True,
        se_final_query_dq: True,
        se_target_table_view: "order",
    },
    user_conf=se_global_spark_Conf,

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
