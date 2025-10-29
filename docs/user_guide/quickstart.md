# Quick Start

To successfully run spark-expectations user needs to create `Rules` table as a first step. 


## Required Tables

Spark expectation expects that Rules table is created for spark-expectations to run seamlessly and integrate with a spark job.


### Rules Table

The below SQL statements used three namespaces which works with Databricks Unity Catalog, but if you are using hive
please update the namespaces accordingly and also provide necessary table metadata.

We need to create a rules tables which contains all the data quality rules. Please use the below template to create
your rules table for your project.

```sql
create table if not exists `catalog`.`schema`.`{product}_rules` (
    product_id STRING,  -- (1)!
    table_name STRING,  -- (2)!
    rule_type STRING,  -- (3)!
    rule STRING,  -- (4)!
    column_name STRING,  -- (5)!
    expectation STRING,  -- (6)!
    action_if_failed STRING,  -- (7)!
    tag STRING,  -- (8)!
    description STRING,  -- (9)!
    enable_for_source_dq_validation BOOLEAN,  -- (10)! 
    enable_for_target_dq_validation BOOLEAN,  -- (11)!
    is_active BOOLEAN,  -- (12)!
    enable_error_drop_alert BOOLEAN,  -- (13)!
    error_drop_threshold INT,  -- (14)!
    query_dq_delimiter STRING,  -- (15)!
    enable_querydq_custom_output BOOLEAN,  -- (16)!
    priority STRING DEFAULT "medium", -- (17)!
);
```

1. `product_id` A unique name at the level of dq rules execution
2. `table_name` The table for which the rule is being defined for
3. `rule_type` 3 different type of rules. They are 'row_dq', 'agg_dq' and 'query_dq'
4. `rule` Short description of the rule 
5. `column_name` The column name for which the rule is defined for. This only applies for row_dq. For agg_dq and query_dq, use blank/empty value. 
6. `expectation` Provide the DQ rule condition 
7. `action_if_failed` There are 3 different types of actions. These are 'ignore', 'drop', and 'fail'. 
    Ignore: The rule is run and the output is logged. No action is performed regardless of whether the rule has succeeded or failed. Applies for all 3 rule types. 
    Drop: The rows that fail the rule get dropped from the dataset. Applies for only row_dq rule type.
    Fail: job fails if the rule fails. Applies for all 3 rule types.
8. `tag` provide some tag name to dq rule example:  completeness, validity, uniqueness etc. 
9. `description`  Long description for the rule
10. `enable_for_source_dq_validation` flag to run the agg rule
11. `enable_for_target_dq_validation` flag to run the query rule
12. `is_active` true or false to indicate if the rule is active or not. 
13. `enable_error_drop_alert` true or false. This determines if an alert notification should be sent out if row(s) is(are) dropped from the data set
14. `error_drop_threshold` Threshold for the alert notification that gets triggered when row(s) is(are) dropped from the data set
15. `query_dq_delimiter` segregate custom queries delimiter ex: $, @ etc. By default it is @. Users can override it with any other delimiter based on the need. The same delimiter mentioned here has to be used in the custom query.
16. `enable_querydq_custom_output` required custom query output in separate table
17. `priority` Priority level for the rule. Supported values are: 'low', 'medium' and 'high'.


The Spark Expectation process consists of three phases:
1. When enable_for_source_dq_validation is true, execute agg_dq and query_dq on the source Dataframe
2. If the first step is successful, proceed to run row_dq
3. When enable_for_target_dq_validation is true, execute agg_dq and query_dq on the Dataframe resulting from row_dq

### Rule Type

The rules column has a column called "rule_type". It is important that this column should only accept one of 
these three values - `[row_dq, agg_dq, query_dq]`. If other values are provided, the library may cause unforeseen errors.
Please run the below command to add constraints to the above created rules table

```sql
ALTER TABLE `catalog`.`schema`.`{product}_rules` 
ADD CONSTRAINT rule_type_action CHECK (rule_type in ('row_dq', 'agg_dq', 'query_dq'));
```

For further information about creating individual rules please refer to the [Rules Guide](../data_quality_rules/)


## Initiating Spark Expectation


#### Sample input data 

```python

data = [
    {"id": 1, "age": 25,   "email": "alice@example.com"},
    {"id": 2, "age": 17,   "email": "bob@example.com"},
    {"id": 3, "age": None, "email": "charlie@example.com"},
    {"id": 4, "age": 40,   "email": "bob@example.com"},
    {"id": 5, "age": None, "email": "ron@example.com"},
    {"id": 6, "age": 41,   "email": None},
]

input_df = spark.createDataFrame(pd.DataFrame(data))
input_df.show(truncate=False)

```
#### Insert expectations into Rules table

```python

# Name of the rules table previously created 
rules_table = f"{catalog}.{schema}.{product}_rules"
product_identifier = "test_product"

rules_data = [
    {
        "product_id": product_identifier,
        "table_name": f"{catalog}.{schema}.{target_table_name}",
        "rule_type": "row_dq",
        "rule": "age_not_null",
        "column_name": "age",
        "expectation": "age IS NOT NULL",
        "action_if_failed": "drop",
        "tag": "completeness",
        "description": "Age must not be null",
        "enable_for_source_dq_validation": True,
        "enable_for_target_dq_validation": True,
        "is_active": True,
        "enable_error_drop_alert": False,
        "error_drop_threshold": 0,
        "priority": "medium",
    }
]

import pandas as pd

rules_df = spark.createDataFrame(pd.DataFrame(rules_data))
rules_df.show(truncate=True)
rules_df.write.mode("overwrite").saveAsTable(rules_table)

```


#### Streaming and User config

Following example let's spark-expectation use default configuration. 

Only configuration we are passing is to disable streaming option


```python

from spark_expectations.config.user_config import Constants as user_config

stats_streaming_config_dict = {
    user_config.se_enable_streaming: False
}

user_config = {}

```

#### Run SparkExpectations job

Please reference [Spark Expectation notebooks](https://github.com/Nike-Inc/spark-expectations/tree/main/examples/notebooks) for fully functioning examples. 

!!! note
    Spark-Expectation repository itself provides [docker compose yaml file](https://github.com/Nike-Inc/spark-expectations/blob/main/containers/compose.yaml) for running those notebooks.
     
    ```bash
        # Generate self signed certs for mailpit server
        make generate-mailserver-certs 

        # running following makefile target will spin up spark, jupyter, mailpit and kafka service
        make local-se-server-start ARGS="--build"` 
    ```


```python

from pyspark.sql import DataFrame
from spark_expectations.core.expectations import (
    SparkExpectations, WrappedDataFrameWriter
)

from spark_expectations.core import load_configurations
# Initialize Default Config: in a future release this will not be required 
load_configurations(spark) 

writer = WrappedDataFrameWriter().mode("append").format("delta")

se = SparkExpectations(
    product_id=f"{product_identifier}",                   # (1)!
    rules_df=rules_dataframe,                             # (2)!
    stats_table=f"{catalog}.{schema}.{stats_table_name}", # (3)!
    stats_table_writer=writer,                            # (4)!
    target_and_error_table_writer=writer,                 # (5)!
    stats_streaming_options=stats_streaming_config_dict   # (6)!
)

"""
This decorator helps to wrap a function which returns dataframe and apply dataframe rules on it

Args:
    target_table: Name of the table where the final dataframe need to be written
    write_to_table: Mark it as "True" if the dataframe need to be written as table
    write_to_temp_table: Mark it as "True" if the input dataframe need to be written to the temp table to break
                        the spark plan
    user_conf: Provide options to override the defaults, while writing into the stats streaming table
    target_table_view: This view is created after the _row_dq process to run the target agg_dq and query_dq.
        If value is not provided, defaulted to {target_table}_view
    target_and_error_table_writer: Provide the writer to write the target and error table,
        this will take precedence over the class level writer

Returns:
    Any: Returns a function which applied the expectations on dataset
"""


@se.with_expectations(
    target_table=f"{catalog}.{schema}.{target_table_name}",
    write_to_table=True,
    write_to_temp_table=True,
    user_conf=user_config,
)
def get_dataset():
    _df_source: DataFrame = input_df
    _df_source.createOrReplaceTempView("in_memory_data_source")
    return _df_source


# This will run the DQ checks and raise if any "fail" rules are violated
get_dataset()


```
<!-- Annotations for tooltips -->
1. The unique product identifier for DQ execution.
2. DataFrame containing the rules to apply.
3. Name of the stats table for logging results.If it doesn't exist it will be generated
4. Writer object for the stats table.
5. Writer object for target and error tables.
6. Dictionary of streaming options for stats.
