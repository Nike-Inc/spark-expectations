

### DQ Stats Table

In order to collect the stats/metrics for each data quality job run, the spark-expectations job will
automatically create the stats table if it does not exist. 

!!! warning
    The below SQL statement can be used to create the table
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
    dq_env STRING,  -- (21)!
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
9. `source_agg_dq_results` results for agg dq rules are stored after row_dq rules executed in an array with each entry as a map\<string, string\> containing the following keys: `description`, `tag`, `column_name`, `rule`, `rule_type`, `status`, `action_if_failed`
10. `final_agg_dq_results` results for agg dq rules are stored after row_dq rules executed in an array with each entry as a map\<string, string\> containing the following keys: `description`, `tag`, `column_name`, `rule`, `rule_type`, `status`, `action_if_failed`
11. `source_query_dq_results` results for query dq rules are stored after row_dq rules executed in an array with each entry as a map\<string, string\> containing the following keys: `description`, `tag`, `column_name`, `rule`, `rule_type`, `status`, `action_if_failed`
12. `final_query_dq_results` results for query dq rules are stored after row_dq rules executed in an array with each entry as a map\<string, string\> containing the following keys: `description`, `tag`, `column_name`, `rule`, `rule_type`, `status`, `action_if_failed`
13. `row_dq_res_summary` summary of row dq results are stored in an array with each entry as a map\<string, string\> containing the following keys: `description`, `tag`, `column_name`, `rule`, `rule_type`, `failed_row_count`, `action_if_failed`
14. `row_dq_error_threshold` error threshold results for rules defined in the rules table for row_dq rules are stored in an array with each entry as a map\<string, string\> containing the following keys: `description`, `column_name` ,`error_drop_threshold`, `rule_type`, `action_if_failed`, `rule_name`, `error_drop_percentage`
15. `dq_status`  stores the status of the rule execution.
16. `dq_run_time` time taken by the rules
17. `dq_rules` how many dq rules are executed in this run
18. `meta_dq_run_id` unique id generated for this run
19. `meta_dq_run_date` date on which rule is executed
20. `meta_dq_run_datetime` date and time on which rule is executed
21. `dq_env` environment value passed from the user_config.se_dq_rules_params

### DQ Detailed Stats Table

Library is responsible for auto generating two stats tables that provide per expectation/rule executaion status view. 

Tables in question are
- `<stats_table_name>_detailed`
- `<stats_table_name>_querydq_output`

This table provides detailed stats of all the expectations along with the status provided in the stats table in a relational format.
This table need not be created. It gets auto created with "_detailed " to the dq stats table name. 


!!! warning
    Detailed Stats Tables are optional. It is auto created and named as stats table with suffix `_detailed`.

    Default Behaviour: Detailed Stats table is disabled. To enable it pass 
    ```
    user_config.se_enable_agg_dq_detailed_result: True,
    ```
    


#### Schema


```sql
create table if not exists `catalog`.`schema`.`<stats_table_name>_detailed` (
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
source_dq_start_time string,  -- (15)!
source_dq_end_time string,  -- (16)!
target_expectations string,  -- (17)!
target_dq_status string,  -- (18)!
target_dq_actual_outcome string,  -- (19)!
target_dq_expected_outcome string,  -- (20)!
target_dq_actual_row_count string,  -- (21)!
target_dq_error_row_count string,  -- (22)!
target_dq_row_count string,  -- (23)!
target_dq_start_time string,  -- (24)!
target_dq_end_time string,  -- (25)!
dq_date date,  -- (26)!
dq_time string,  -- (27)!
dq_job_metadata_info string,  -- (28)!
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
15. `source_dq_start_time` source dq start timestamp
16. `source_dq_end_time` source dq end timestamp
17. `target_expectations` Actual Rule to be executed on the target dq
18. `target_dq_status` Status of the rule execution in the Target dq
19. `target_dq_actual_outcome` Actual outcome of the Target dq check
20. `target_dq_expected_outcome` Expected outcome of the Target dq check
21. `target_dq_actual_row_count` Number of rows of the target dq
22. `target_dq_error_row_count` Number of rows failed in the target dq
23. `target_dq_row_count` Number of rows of the target dq
24. `target_dq_start_time` target dq start timestamp
25. `target_dq_end_time` target dq end timestamp
26. `dq_date` Dq executed date
27. `dq_time` Dq executed timestamp
28. `dq_job_metadata_info` dq job metadata



### DQ Query Output Table 

!!! warning
    DQ Query Output Table is optional. It is auto created and named as stats table with suffix `_querydq_output`.

    Name can be overriden by passing `querydq_output_custom_table_name`

    Default Behaviour: Detailed Stats table is disabled.

    ```
    user_config.querydq_output_custom_table_name: <string> 
    user_config.se_enable_query_dq_detailed_result: True <-- Toggle switch if table should be enabled
    
    ```

#### Schema 

```sql
create table if not exists `<catalog>`.`<schema>`.`<stats_table_name>_querydq_output` (
    run_id string,  -- (1)!    
    product_id string,  -- (2)!  
    table_name string,  -- (3)!  
    rule string,  -- (4)! 
    column_name string,  -- (5)!  
    alias string,  -- (6)!  
    dq_type string,  -- (7)!  
    source_output string,  -- (8)!  
    target_output string,  -- (9)!  
    dq_time string,  -- (10)!  
);
```

1. `run_id` Run Id for a specific run 
2. `product_id` Unique product identifier 
3. `table_name` --
4. `rule`  Rule name
5. `column_name` column name
6. `alias` --
7. `dq_type` --
8. `source_output` --
9. `target_output` --
10. `dq_time` Dq executed timestamp
