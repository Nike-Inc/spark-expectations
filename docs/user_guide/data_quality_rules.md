When evaluating characteristic of data and it's quality we usually think of data quality dimensions. Spark-expectations is there to verify data integrity once data has already been ingested or in-flight before data lands in a datastore. Some of the data quality dimensions are 

| Dimension        | What It Means                                                                             | Business Quality            | Technical Quality     |
|------------------|-------------------------------------------------------------------------------------------|---------------------------- |-----------------------|
| `Completeness`   | Proportion of the data against the potential of 100% data                                 | Is this field required for business use and does it need to be populated fully to answer business questions?                  | Is the data we received equal to the data we loaded in the target?               |
| `Validity`       | Measures of the existence, structure, content, and other basic characteristics of data    | Data is valid when it meets the formal and structural requirements of the business rule that the organization defines         | Validity is a measure of the correspondence between the formal aspects of the column data and format that the organization requires          |
| `Uniqueness`     | Extent to which all distinct values of a data element appear only once                    | Is an individual data point recorded more than once?                                   | Is an individual data point recorded more than once?                                       |
| `Consistency`    | Absence of difference when comparing two or more representations against its definition   | Are the data within the data attribute the same as it moves across the ecosystem based on expectations?                      | Are the record counts and/or summation of the measure of data attributes the same over time based on the defined threshold?                |
| `Timeliness`     | Degree to which data is available when it is required                                     | Is data relevant for the business use at the point in time?                             | Did my data arrive on time? Are tables refreshing on time?                 |
| `Accuracy`       | Degree to which data correctly describes the real-world object or event being described   | Does data describe the real-world environment it's trying to represent?                  | Does data describe the real-world environment it's trying to represent?                |


!!! Hint "[Rule Examples](../../configurations/rules/)" 

## Rules Table 
For user to be able to run spark-expectation or define rules `Rules table` needs to exist and. 
Format of a table needs to match. 

The below SQL statements used three namespaces which works with Databricks Unity Catalog, but if you are using hive
please update the namespaces accordingly and also provide necessary table metadata.


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
10. `enable_for_source_dq_validation` when true, agg_dq and query_dq will run on the dataset before row_dq rules get excuted (and filter down the dataset).
11. `enable_for_target_dq_validation` when true, agg_dq and query_dq will run on the dataset after row_dq rules ran.
12. `is_active` true or false to indicate if the rule is active or not. 
13. `enable_error_drop_alert` true or false. This determines if an alert notification should be sent out if row(s) is(are) dropped from the data set
14. `error_drop_threshold` Threshold for the alert notification that gets triggered when row(s) is(are) dropped from the data set
15. `query_dq_delimiter` segregate custom queries delimiter ex: $, @ etc. By default it is @. Users can override it with any other delimiter based on the need. The same delimiter mentioned here has to be used in the custom query.
16. `enable_querydq_custom_output` required custom query output in separate table
17. `priority` Priority level for the rule. Supported values are: 'low', 'medium' and 'high'.


The Spark Expectation process consists of three phases:

1. When enable_for_source_dq_validation is true, execute agg_dq and query_dq on the source Dataframe
2. If the first step is successful, proceed to run row_dq
3. When enable_for_target_dq_validation is true, execute agg_dq and query_dq on the Dataframe resulting from row_dq. If the Dataframe does not cosists any records after phase 2, this step will not run.

### Action If Failed Configuration For Data Quality Rules

The rules column has a column called `action_if_failed`. It is important that this column should only accept one of 
these values:

 - `[fail, drop or ignore]` for `'rule_type'='row_dq'` and 
 - `[fail, ignore]` for `'rule_type'='agg_dq' and 'rule_type'='query_dq'`. 
 
If other values are provided, the library may cause unforeseen errors.
Please run the below command to add constraints to the above created rules table

```sql
ALTER TABLE apla_nd_dq_rules
ADD CONSTRAINT priority_constraint CHECK (
    priority IN ('low', 'medium', 'high')
);

ALTER TABLE apla_nd_dq_rules
ADD CONSTRAINT action CHECK (
    (rule_type = 'row_dq' and action_if_failed IN ('ignore', 'drop', 'fail')) or 
    (rule_type = 'agg_dq' and action_if_failed in ('ignore', 'fail')) or 
    (rule_type = 'query_dq' and action_if_failed in ('ignore', 'fail'))
);
```

### Rule Types

| Rule Type | Valid Expectation Format | Aggregate Functions Allowed | SQL Queries Allowed |
|-----------|-------------------------|----------------------------|-----------------------|
| `row_dq`  | Simple row expressions  | ❌ No                      | ❌ No                 |
| `agg_dq`  | Aggregate expressions   | ✅ Yes                     | ❌ No                 |
| `query_dq`| SQL queries             | ✅ Yes (via SQL)           | ✅ Yes                |

**Tip:**  
- Match your expectation format to the rule type for correct validation.
- Use `row_dq` for per-row checks, `agg_dq` for summary statistics, and `query_dq` for advanced SQL-based checks.


Below are the details and examples for each rule type:

---

#### 1. Row-Level Data Quality (`row_dq`)

**Purpose:**  
Checks conditions on individual rows, without using aggregate functions or SQL queries.

**Valid Expectation Examples:**
- `col1 > 10`
- `col2 < 25`
- `col1 is null`
- `col1 is not null`
- `(col3 % 2) = 0`

**Characteristics:**
- Operates on each row independently.
- **Aggregate functions are NOT allowed** (e.g., `sum`, `avg`, `min`, `max`).
- **SQL queries are NOT allowed**.

---

#### 2. Aggregate Data Quality (`agg_dq`)

**Purpose:**  
Checks conditions on aggregated values computed from the DataFrame.

**Valid Expectation Examples:**
- `sum(col3) > 20`
- `avg(col3) > 25`
- `min(col1) > 10`
- `stddev(col3) > 10`
- `count(distinct col2) > 4`
- `avg(col1) > 4`
- `avg(col3) > 18 and avg(col3) < 25`
- `avg(col3) between 18 and 25`
- `count(*) > 5` *(Note: support may depend on your Spark version)*

**Characteristics:**
- **Aggregate functions are required** (e.g., `sum`, `avg`, `min`, `max`, `count`, `stddev`).
- Operates on the entire DataFrame or groups of rows.
- **SQL queries are NOT allowed**.

---

#### 3. Query-Based Data Quality (`query_dq`)

**Purpose:**  
Checks conditions using full SQL queries, typically for more complex or cross-table checks.

**Valid Expectation Examples:**
- `(select sum(col1) from test_table) > 10`
- `(select stddev(col3) from test_table) > 0`
- `(select max(col1) from test_final_table_view) > 10`
- `(select min(col3) from test_final_table_view) > 0`
- `(select count(col1) from test_final_table_view) > 3`
- `(select count(case when col3>0 then 1 else 0 end) from test_final_table_view) > 10`
- `(select sum(col1) from {table}) > 10`
- `(select count(*) from test_table) > 10`

**Characteristics:**
- Must be a valid SQL query, typically starting with `select ... from ...`.
- Can reference other tables or views.
- Supports complex logic, including subqueries and case statements.


### Examples of Supported Rule Types

- **Row DQ**: 
```sql
INSERT INTO `catalog`.`schema`.`{product}_rules` VALUES ('your_product', 'your_table', 'row_dq', 'check_nulls', 'column_name', 'is not null', 'drop', 'completeness', 'Check for null values in column_name', true, true, true, false, 0, "medium");
```

- **Aggregation DQ**: 
```sql
INSERT INTO `catalog`.`schema`.`{product}_rules` VALUES ('your_product', 'your_table', 'agg_dq', 'check_row_count', '', 'COUNT(*) > 0', 'fail', 'completeness', 'Ensure the table has at least one row', true, true, true, false, 0, "medium");
```

- **Query DQ**:
```sql
INSERT INTO `catalog`.`schema`.`{product}_rules` VALUES ('your_product', 'your_table', 'query_dq', 'check_custom_query', '', 'SELECT COUNT(*) FROM your_table WHERE column_name IS NULL', 'ignore', 'validity', 'Custom query to check for null values in column_name', false, true, true, false, 0, "medium");
```



