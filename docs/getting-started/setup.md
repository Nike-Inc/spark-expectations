## Installation
The library is available in the Python Package Index (PyPi) and can be installed in your environment using the below command or 
 add the library "spark-expectations" into the requirements.txt or poetry dependencies.

```shell
pip install -U spark-expectations
```

## Required Tables

There are two tables that need to be created for spark-expectations to run seamlessly and integrate with a spark job.
The below SQL statements used three namespaces which works with Databricks Unity Catalog, but if you are using hive
please update the namespaces accordingly and also provide necessary table metadata.


### Rules Table

We need to create a rules tables which contains all the data quality rules. Please use the below template to create
your rules table for your project.

```sql
create table if not exists `catalog`.`schema`.`{product}_rules` (
    product_id STRING,
    table_name STRING,
    rule_type STRING,
    rule STRING,
    column_name STRING,
    expectation STRING,
    action_if_failed STRING,
    tag STRING,
    description STRING,
    enable_for_source_dq_validation BOOLEAN, 
    enable_for_target_dq_validation BOOLEAN,
    is_active BOOLEAN,
    enable_error_drop_alert BOOLEAN,
    error_drop_threshold INT,
    query_dq_delimiter STRING,
    enable_querydq_custom_output BOOLEAN
);
```

### Rule Type For Rules

The rules column has a column called "rule_type". It is important that this column should only accept one of 
these three values - `[row_dq, agg_dq, query_dq]`. If other values are provided, the library may cause unforeseen errors.
Please run the below command to add constraints to the above created rules table

```sql
ALTER TABLE `catalog`.`schema`.`{product}_rules` 
ADD CONSTRAINT rule_type_action CHECK (rule_type in ('row_dq', 'agg_dq', 'query_dq'));
```

### Action If Failed For Row, Aggregation and Query Data Quality Rules

The rules column has a column called "action_if_failed". It is important that this column should only accept one of 
these values - `[fail, drop or ignore]` for `'rule_type'='row_dq'` and `[fail, ignore]` for `'rule_type'='agg_dq' and 'rule_type'='query_dq'`. 
If other values are provided, the library may cause unforeseen errors.
Please run the below command to add constraints to the above created rules table

```sql
ALTER TABLE apla_nd_dq_rules ADD CONSTRAINT action CHECK 
((rule_type = 'row_dq' and action_if_failed IN ('ignore', 'drop', 'fail')) or 
(rule_type = 'agg_dq' and action_if_failed in ('ignore', 'fail')) or 
(rule_type = 'query_dq' and action_if_failed in ('ignore', 'fail')));
```

### DQ Stats Table

In order to collect the stats/metrics for each data quality job run, the spark-expectations job will
automatically create the stats table if it does not exist. The below SQL statement can be used to create the table
if you want to create it manually, but it is not recommended.

```sql
create table if not exists `catalog`.`schema`.`dq_stats` (
    product_id STRING,
    table_name STRING,
    input_count LONG,
    error_count LONG,
    output_count LONG,
    output_percentage FLOAT,
    success_percentage FLOAT,
    error_percentage FLOAT,
    source_agg_dq_results array<map<string, string>>,
    final_agg_dq_results array<map<string, string>>,
    source_query_dq_results array<map<string, string>>,
    final_query_dq_results array<map<string, string>>,
    row_dq_res_summary array<map<string, string>>,
    row_dq_error_threshold array<map<string, string>>,
    dq_status map<string, string>,
    dq_run_time map<string, float>,
    dq_rules map<string, map<string,int>>,
    meta_dq_run_id STRING,
    meta_dq_run_date DATE,
    meta_dq_run_datetime TIMESTAMP
);
```

### DQ Detailed Stats Table

This table provides detailed stats of all the expectations along with the status provided in the stats table in a relational format.
This table need not be created. It gets auto created with "_detailed " to the dq stats table name.
Below is the schema


```
run_id string,    
product_id string ,  
table_name string ,  
rule_type string ,  
rule string ,
source_expectations string,
tag string ,
description string,
source_dq_status string ,
source_dq_actual_outcome string ,
source_dq_expected_outcome string ,
source_dq_actual_row_count string ,
source_dq_error_row_count string ,
source_dq_row_count string ,
target_expectations string ,
target_dq_status string ,
target_dq_actual_outcome string ,
target_dq_expected_outcome string ,
target_dq_actual_row_count string ,
target_dq_error_row_count string ,
target_dq_row_count string ,
dq_date date ,
dq_time string
```