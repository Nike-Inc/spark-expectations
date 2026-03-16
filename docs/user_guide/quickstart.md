# Quick Start

This guide walks you through a minimal working example of Spark Expectations.

## Required Tables

Spark Expectations requires a **rules table** to define your data quality expectations.

### Rules Table

The SQL below uses three-part names compatible with Databricks Unity Catalog. Adjust for Hive or other catalogs as needed.

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
3. `rule_type` One of `'row_dq'`, `'agg_dq'`, or `'query_dq'`
4. `rule` Short name for the rule
5. `column_name` The column the rule applies to. For `agg_dq` and `query_dq`, use an empty string.
6. `expectation` The DQ rule condition (SQL expression)
7. `action_if_failed` One of `'ignore'`, `'drop'` (row_dq only), or `'fail'`
8. `tag` Category tag (e.g., completeness, validity, uniqueness)
9. `description` Long description for the rule
10. `enable_for_source_dq_validation` When true, run agg_dq/query_dq on the source DataFrame **before** row_dq
11. `enable_for_target_dq_validation` When true, run agg_dq/query_dq on the DataFrame **after** row_dq
12. `is_active` Whether the rule is active
13. `enable_error_drop_alert` Send alert when rows are dropped by this rule
14. `error_drop_threshold` Threshold percentage for triggering the error drop alert
15. `query_dq_delimiter` Delimiter for composite query_dq expectations (default: `@`)
16. `enable_querydq_custom_output` Capture custom query output in a separate table
17. `priority` Rule priority: `'low'`, `'medium'`, or `'high'`

The DQ process runs in three phases:

1. **Source validation** -- When `enable_for_source_dq_validation` is true, execute `agg_dq` and `query_dq` on the input DataFrame
2. **Row validation** -- Run `row_dq` rules on every row
3. **Target validation** -- When `enable_for_target_dq_validation` is true, execute `agg_dq` and `query_dq` on the DataFrame after row_dq filtering

### Rule Type Constraint

Add this constraint to prevent invalid `rule_type` values:

```sql
ALTER TABLE `catalog`.`schema`.`{product}_rules` 
ADD CONSTRAINT rule_type_action CHECK (rule_type in ('row_dq', 'agg_dq', 'query_dq'));
```

For details on writing rules, see the [Rules Guide](data_quality_rules.md).


## Complete Working Example

The following is a self-contained example. Replace `catalog`, `schema`, and table names with your own.

### 1. Sample input data 

```python
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

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

### 2. Insert expectations into Rules table

```python
product_identifier = "test_product"
target_table_name = "my_target_table"
rules_table = "catalog.schema.test_product_rules"

rules_data = [
    {
        "product_id": product_identifier,
        "table_name": f"catalog.schema.{target_table_name}",
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

rules_df = spark.createDataFrame(pd.DataFrame(rules_data))
rules_df.write.mode("overwrite").saveAsTable(rules_table)
```

### 3. Configure and run SparkExpectations

```python
from pyspark.sql import DataFrame
from spark_expectations.config.user_config import Constants as user_config
from spark_expectations.core.expectations import (
    SparkExpectations, WrappedDataFrameWriter
)
from spark_expectations.core import load_configurations

load_configurations(spark)

writer = WrappedDataFrameWriter().mode("append").format("delta")

se = SparkExpectations(
    product_id=product_identifier,                       # (1)!
    rules_df=spark.table(rules_table),                   # (2)!
    stats_table="catalog.schema.dq_stats",               # (3)!
    stats_table_writer=writer,                            # (4)!
    target_and_error_table_writer=writer,                 # (5)!
    stats_streaming_options={                             # (6)!
        user_config.se_enable_streaming: False,
    },
)

user_conf = {
    user_config.se_notifications_on_start: False,
    user_config.se_notifications_on_completion: False,
    user_config.se_notifications_on_fail: False,
    user_config.se_enable_error_table: True,
}

@se.with_expectations(
    target_table=f"catalog.schema.{target_table_name}",
    write_to_table=True,
    write_to_temp_table=True,
    user_conf=user_conf,
)
def get_dataset():
    _df_source: DataFrame = input_df
    _df_source.createOrReplaceTempView("in_memory_data_source")
    return _df_source


get_dataset()
```
<!-- Annotations for tooltips -->
1. Must match the `product_id` in your rules table.
2. Read the rules table as a DataFrame. Can also use `load_rules_from_yaml()` for file-based rules.
3. Stats table for logging DQ metrics. Auto-created if it doesn't exist.
4. Writer config for the stats table.
5. Writer config for target and error tables.
6. Pass `se_enable_streaming: False` to disable Kafka stats streaming.

!!! tip "Try it locally"
    The repository provides a [Docker Compose setup](https://github.com/Nike-Inc/spark-expectations/blob/main/containers/compose.yaml) with Jupyter Lab, Kafka, and Mailpit (SMTP test server):

    ```bash
    make generate-mailserver-certs
    make local-se-server-start ARGS="--build"
    ```

    Then open [http://localhost:8888](http://localhost:8888) and run any of the [example notebooks](https://github.com/Nike-Inc/spark-expectations/tree/main/examples/notebooks).

---

## `with_expectations` Decorator Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `target_table` | `str` | *required* | Fully qualified name of the target table |
| `write_to_table` | `bool` | `False` | Write the result DataFrame as a table. Set `False` if you only want DQ checks without writing. |
| `write_to_temp_table` | `bool` | `False` | Write the input DataFrame to a temp table first, then read it back. This breaks the Spark execution plan and can speed up jobs with complex DataFrame lineage. |
| `user_conf` | `Dict` | `None` | Configuration overrides for notifications, streaming, error tables, etc. See [Configuration Reference](configuration_reference.md). |
| `target_table_view` | `str` | `{target_table}_view` | Name of the temporary view created after row DQ. Target `agg_dq` and `query_dq` rules run against this view. If you write `query_dq` rules, they must reference this view name. |
| `target_and_error_table_writer` | `Writer` | `None` | Per-call writer override. Takes precedence over the class-level writer. |

!!! tip "When to use `write_to_temp_table`"
    Set this to `True` when your input DataFrame has complex lineage (e.g., multiple joins, UDFs, or external data sources). Writing to a temp table materializes the DataFrame, breaking the Spark plan into two stages and often improving performance.

!!! tip "Understanding `target_table_view`"
    After row DQ runs, the cleaned DataFrame is registered as a temporary view. Target `agg_dq` and `query_dq` rules execute SQL against this view. If your `query_dq` expectation references a table name, make sure it matches `target_table_view` (or the default `{table_name}_view`).

---

## `load_configurations()`

The `load_configurations(spark)` function reads default settings from `spark-expectations-default-config.yaml` and returns the streaming and notification config dictionaries. The full configuration resolution order is:

1. **Built-in defaults** from `spark-expectations-default-config.yaml`
2. **Spark session config** (`spark.conf.get(...)`) can override defaults
3. **`user_conf` dict** passed to `with_expectations` takes highest precedence

!!! note
    In serverless environments (Databricks Serverless), Spark session config access is limited. Use the `user_conf` dict to set all configuration explicitly.

---

## Common Exceptions

When working with Spark Expectations, you may encounter these exceptions:

| Exception | When It's Raised |
|---|---|
| `SparkExpectOrFailException` | A rule with `action_if_failed = "fail"` has failed |
| `SparkExpectationsDataframeNotReturnedException` | The decorated function did not return a DataFrame |
| `SparkExpectationsUserInputOrConfigInvalidException` | Invalid configuration, rule definition, or input |
| `SparkExpectationsErrorThresholdExceedsException` | Error drop percentage exceeds the configured threshold |
| `SparkExpectationsMiscException` | General internal error |

Notification-specific exceptions (`SparkExpectationsEmailException`, `SparkExpectationsSlackNotificationException`, etc.) are raised when a notification channel fails but do not affect the DQ processing itself.
