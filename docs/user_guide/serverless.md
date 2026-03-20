# Databricks Serverless Compute Support

Spark Expectations now provides full support for **Databricks Serverless Compute**, enabling data quality validation in serverless environments with automatic adaptation to platform constraints.

## Overview

Databricks Serverless Compute offers a managed, auto-scaling environment that simplifies cluster management. However, it comes with specific limitations that require framework adaptations:

- **Configuration Restrictions**: Limited access to Spark configuration properties
- **DataFrame Persistence Limitations**: `PERSIST TABLE` operations are not supported  
- **Managed Environment**: Reduced control over Spark session configuration

Spark Expectations automatically detects and adapts to these constraints when running in serverless mode.

## Limitations

!!! warning "Email Notifications in Serverless"
    **Email notifications may not work in Databricks Serverless environments** due to network restrictions. Ensure your serverless compute has the necessary permissions and network access to send emails via SMTP.
    
    **Workaround:** Use Slack, Teams, or other webhook-based notifications instead - these work reliably in serverless environments.

## Quick Start

### Enable Serverless Mode

To use Spark Expectations on Databricks Serverless Compute, simply set the serverless flag in your user configuration:

```python
from spark_expectations.core.expectations import SparkExpectations, WrappedDataFrameWriter

# Configure for serverless environment
user_conf = {
    user_config.is_serverless: True,  # Enable serverless mode
    user_config.se_notifications_enable_email: False,  # Email may not work in serverless
    user_config.se_notifications_enable_slack: True,   # Use Slack instead (recommended)
    user_config.se_enable_error_table: True,
    user_config.se_enable_query_dq_detailed_result: True,    
    user_config.se_dq_rules_params: {
        "env": "local",
        "table": "orders",
    },
    user_config.se_enable_streaming: False
}

writer = WrappedDataFrameWriter().mode("append").format("delta")
# Create SparkExpectations instance
se = SparkExpectations(
    product_id="your_product_id",
    rules_df=your_rules_dataframe,
    stats_table="your_stats_table",
    target_and_error_table_writer=writer,
    stats_table_writer=writer,
    user_conf=user_conf  # Enable serverless mode
)

# Use the decorator as normal
@se.with_expectations(
    target_table="your_target_table",
    user_conf=user_conf  # Also pass to decorator
)
def process_data():
    # Your data processing logic
    return processed_dataframe

# Execute your data pipeline
result_df = process_data()
```

## ANSI Mode Compatibility

Databricks Serverless Compute runs with `spark.sql.ansi.enabled=true` by default. Under ANSI mode, `CAST()` raises `CAST_INVALID_INPUT` when it encounters empty strings, `NULL`s, or non-numeric values -- a strict behavior that differs from the permissive default in standard compute.

Spark Expectations automatically detects ANSI mode and applies the following safeguards:

1. **Automatic detection**: `SparkExpectationsContext` checks `spark.sql.ansi.enabled` at initialization and exposes a `get_ansi_enabled` property.
2. **ANSI-safe type conversions**: All internal `.cast()` calls use `try_cast` expressions, which return `NULL` instead of raising errors on invalid input.
3. **Temporary ANSI disable**: During DQ rule processing, the framework temporarily sets `spark.sql.ansi.enabled=false` and restores the original value afterward. This prevents strict-mode failures from user-defined SQL expressions that may contain implicit casts.
4. **Null-safe aggregation**: Aggregation and query DQ results handle `None` values gracefully instead of raising `TypeError`.

**No user action is required** -- the framework handles this transparently.

### Writing ANSI-safe Custom Expectations

If you are writing custom `query_dq` or `agg_dq` expectations that include `CAST()` operations, use `try_cast()` instead to ensure compatibility across both ANSI and non-ANSI environments:

| Instead of | Use |
|-----------|-----|
| `CAST(column AS BIGINT)` | `try_cast(column AS BIGINT)` |
| `CAST(column AS DOUBLE)` | `try_cast(column AS DOUBLE)` |
| `CAST(column AS DATE)` | `try_cast(column AS DATE)` |

**Example** -- an aggregation rule with `try_cast`:

```sql
sum(try_cast(amount AS DOUBLE)) > 1000
```

### What Changes Were Made

| Component | Change |
|-----------|--------|
| `core/context.py` | Added `_ansi_enabled` detection at init, `get_ansi_enabled` property |
| `core/expectations.py` | ANSI safety net: temporarily disables ANSI during processing |
| `sinks/utils/report.py` | Replaced all `.cast()` with `try_cast` for row-count and percentage columns |
| `utils/actions.py` | Null-safe handling of aggregation and query DQ results |

!!! note "Spark Version Requirement"
    `try_cast` requires Spark 3.4+ (Databricks Runtime 13.0+). All Databricks Serverless environments meet this requirement.

### Testing ANSI Compatibility

See the [ANSI Mode Testing Guide](ansi_testing_guide.md) for instructions on running and validating ANSI mode changes locally and on Databricks.

