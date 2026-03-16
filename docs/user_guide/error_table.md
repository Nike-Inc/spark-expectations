# Error Table

When row-level data quality rules run, rows that fail any rule with `action_if_failed` set to `drop` or `ignore` are written to an **error table**. This table serves as a quarantine zone for investigating and correcting data quality issues.

## How It Works

During the row DQ phase, each row is evaluated against all active `row_dq` rules. The outcome depends on the `action_if_failed` setting:

- **`drop`**: The row is removed from the target table and written to the error table.
- **`ignore`**: The row is kept in the target table **and** written to the error table.
- **`fail`**: The entire job fails immediately.

The error table captures every failing row along with metadata about which rules failed.

## Default Naming

The error table is automatically named by appending `_error` to the target table name:

```
target_table = "catalog.schema.customer_order"
error_table  = "catalog.schema.customer_order_error"  (auto-generated)
```

## Custom Error Table Name

Override the default name by calling `set_error_table_name()` on the SparkExpectations context **before** invoking the decorated function:

```python
se = SparkExpectations(
    product_id="your_product",
    rules_df=rules_df,
    stats_table="catalog.schema.dq_stats",
    stats_table_writer=writer,
    target_and_error_table_writer=writer,
)

se._context.set_error_table_name("catalog.schema.custom_error_table")

@se.with_expectations(
    target_table="catalog.schema.customer_order",
    write_to_table=True,
)
def get_data():
    return input_df

get_data()
```

## Enabling / Disabling

The error table is **enabled by default**. To disable it, set `se_enable_error_table` to `False` in the user config:

```python
from spark_expectations.config.user_config import Constants as user_config

user_conf = {
    user_config.se_enable_error_table: False,
}
```

!!! warning
    Disabling the error table means failed rows are silently dropped with no record of which rows or rules were involved. This is not recommended for production workloads.

## Schema

The error table has the same columns as the input DataFrame, plus additional metadata columns appended by Spark Expectations:

| Column | Type | Description |
|---|---|---|
| *(all input columns)* | *(original types)* | The original row data |
| `meta_dq_run_id` | STRING | Unique identifier for this DQ run |
| `meta_dq_run_date` | DATE | Date the DQ run was executed |
| `meta_dq_run_datetime` | TIMESTAMP | Timestamp of the DQ run |
| `meta_dq_rule_fail_records` | ARRAY&lt;MAP&lt;STRING, STRING&gt;&gt; | Array of maps describing each failed rule: `rule`, `rule_type`, `action_if_failed`, `description`, `tag` |

## Error Drop Alerts

You can configure notifications when the percentage of dropped rows exceeds a threshold:

```python
user_conf = {
    user_config.se_notifications_on_error_drop_exceeds_threshold_breach: True,
    user_config.se_notifications_on_error_drop_threshold: 15,
}
```

When more than 15% of input rows are dropped, a notification is sent via any enabled channel (email, Slack, Teams, Zoom, PagerDuty).

Individual rules can also opt into error drop alerts using the `enable_error_drop_alert` and `error_drop_threshold` columns in the rules table.
