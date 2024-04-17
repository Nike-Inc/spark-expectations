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
    Fail: DAG fails if the rule fails. Applies for all 3 rule types.
8. `tag` provide some tag name to dq rule example:  completeness, validity, uniqueness etc. 
9. `description`  Long description for the rule
10. `enable_for_source_dq_validation` flag to run the agg rule
11. `enable_for_target_dq_validation` flag to run the query rule
12. `is_active` true or false to indicate if the rule is active or not. 
13. `enable_error_drop_alert` true or false. This determines if an alert notification should be sent out if row(s) is(are) dropped from the data set
14. `error_drop_threshold` Threshold for the alert notification that gets triggered when row(s) is(are) dropped from the data set
15. `query_dq_delimiter` segregate custom queries delimiter ex: $, @ etc. By default it is @. Users can override it with any other delimiter based on the need. The same delimiter mentioned here has to be used in the custom query.
16. `enable_querydq_custom_output` required custom query output in separate table

rule_type, enable_for_source_dq_validation and enable_for_target_dq_validation columns define source_agg_dq, target_agg_dq,source_query_dq and target_query_dq. please see the below definitions:
If rule_type is row_dq then row_dq is TRUE
If rule_type is agg_dq and enable_for_source_dq_validation is TRUE then source_agg_dq is TRUE
If rule_type is agg_dq and enable_for_target_dq_validation is TRUE then target_agg_dq is TRUE
If rule_type is query_dq and enable_for_source_dq_validation is TRUE then source_query_dq is TRUE
If rule_type is query_dq and enable_for_target_dq_validation is TRUE then target_query_dq is TRUE

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
    product_id STRING,  -- (1)!
    table_name STRING,  -- (2)!
    input_count LONG,  -- (3)!
    error_count LONG,  -- (4)!
    output_count LONG,  -- (5)!
    output_percentage FLOAT,  -- (6)!
    success_percentage FLOAT,  -- (7)!
    error_percentage FLOAT,  -- (8)!
    source_agg_dq_results array<map<string, string>>,  -- (9)!
    final_agg_dq_results array<map<string, string>>,  -- (10)!
    source_query_dq_results array<map<string, string>>,  -- (11)!
    final_query_dq_results array<map<string, string>>,  -- (12)!
    row_dq_res_summary array<map<string, string>>,  -- (13)!
    row_dq_error_threshold array<map<string, string>>,  -- (14)!
    dq_status map<string, string>,  -- (15)!
    dq_run_time map<string, float>,  -- (16)!
    dq_rules map<string, map<string,int>>,  -- (17)!
    meta_dq_run_id STRING,  -- (18)!
    meta_dq_run_date DATE,  -- (19)!
    meta_dq_run_datetime TIMESTAMP,  -- (20)!
);
```

1. `product_id` A unique name at the level of dq rules execution
2. `table_name` The table for which the rule is being defined for
3. `input_count` total input row count of given dataframe
4. `error_count` total error count for all row_dq rules
5. `output_count` total count of records that passed the row_dq rules or configured to be ignored when they fail
6. `output_percentage` percentage of total count of records that passed the row_dq rules or configured to be ignored when they fail
7. `success_percentage` percentage of total count of records that passed the row_dq rules
8. `error_percentage` percentage of total count of records that failed the row_dq rules
9. `source_agg_dq_results` results for agg dq rules are stored
10. `final_agg_dq_results` results for agg dq rules are stored after row_dq rules executed
11. `source_query_dq_results` results for query dq rules are stored
12. `final_query_dq_results` results for query dq rules are stored after row_dq rules executed
13. `row_dq_res_summary` summary of row dq results are stored
14. `row_dq_error_threshold` threshold for rules defined in the rules table for row_dq rules
15. `dq_status`  stores the status of the rule execution.
16. `dq_run_time` time taken by the rules
17. `dq_rules` how many dq rules are executed in this run
18. `meta_dq_run_id` unique id generated for this run
19. `meta_dq_run_date` date on which rule is executed
20. `meta_dq_run_datetime` date and time on which rule is executed

### DQ Detailed Stats Table

This table provides detailed stats of all the expectations along with the status provided in the stats table in a relational format.
This table need not be created. It gets auto created with "_detailed " to the dq stats table name. This is optional and only get's created if the config is set to have the detailed stats table.
Below is the schema


```sql
create table if not exists `catalog`.`schema`.`dq_stats_detailed` (
run_id string,  -- (1)!    
product_id string,  -- (2)!  
table_name string,  -- (3)!  
rule_type string,  -- (4)!  
rule string,  -- (5)!
source_expectations string,  -- (6)!
tag string,  -- (7)!
description string,  -- (8)!
source_dq_status string,  -- (9)!
source_dq_actual_outcome string,  -- (10)!
source_dq_expected_outcome string,  -- (11)!
source_dq_actual_row_count string,  -- (12)!
source_dq_error_row_count string,  -- (13)!
source_dq_row_count string,  -- (14)!
target_expectations string,  -- (15)!
target_dq_status string,  -- (16)!
target_dq_actual_outcome string,  -- (17)!
target_dq_expected_outcome string,  -- (18)!
target_dq_actual_row_count string,  -- (19)!
target_dq_error_row_count string,  -- (20)!
target_dq_row_count string,  -- (21)!
dq_date date,  -- (22)!
dq_time string,  -- (23)!
);
```

1. `run_id` Run Id for a specific run 
2. `product_id` Unique product identifier 
3. `table_name` The target table where the final data gets inserted
4. `rule_type` Either row/query/agg dq
5. `rule`  Rule name
6. `source_expectations` Actual Rule to be executed on the source dq
7. `tag` completeness,uniqueness,validity,accuracy,consistency,
8. `description` Description of the Rule
9. `source_dq_status` Status of the rule execution in the Source dq
10. `source_dq_actual_outcome` Actual outcome of the Source dq check
11. `source_dq_expected_outcome` Expected outcome of the Source dq check
12. `source_dq_actual_row_count` Number of rows of the source dq
13. `source_dq_error_row_count` Number of rows failed in the source dq
14. `source_dq_row_count` Number of rows of the source dq
15. `target_expectations` Actual Rule to be executed on the target dq
16. `target_dq_status` Status of the rule execution in the Target dq
17. `target_dq_actual_outcome` Actual outcome of the Target dq check
18. `target_dq_expected_outcome` Expected outcome of the Target dq check
19. `target_dq_actual_row_count` Number of rows of the target dq
20. `target_dq_error_row_count` Number of rows failed in the target dq
21. `target_dq_row_count` Number of rows of the target dq
22. `dq_date` Dq executed date
23. `dq_time` Dq executed timestamp
