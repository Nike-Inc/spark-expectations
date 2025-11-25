# Databricks Serverless Compute Support

Spark Expectations now provides full support for **Databricks Serverless Compute**, enabling data quality validation in serverless environments with automatic adaptation to platform constraints.

## Overview

Databricks Serverless Compute offers a managed, auto-scaling environment that simplifies cluster management. However, it comes with specific limitations that require framework adaptations:

- **Configuration Restrictions**: Limited access to Spark configuration properties
- **DataFrame Persistence Limitations**: `PERSIST TABLE` operations are not supported  
- **Managed Environment**: Reduced control over Spark session configuration

Spark Expectations automatically detects and adapts to these constraints when running in serverless mode.

## Quick Start

### Enable Serverless Mode

To use Spark Expectations on Databricks Serverless Compute, simply set the serverless flag in your user configuration:

```python
from spark_expectations.core.expectations import SparkExpectations, WrappedDataFrameWriter

# Configure for serverless environment
user_conf = {
    user_config.se_notifications_enable_email: False,
    user_config.se_notifications_enable_slack: False,
    user_config.se_enable_error_table: True,
    user_config.se_enable_query_dq_detailed_result: True,
    user_config.is_serverless: True,  # This flag is for Serverless conf
    user_config.se_dq_rules_params: {
        "env": "local",
        "table": "orders",
    },
    user_config.se_enable_streaming: False
}

# Create SparkExpectations instance
se = SparkExpectations(
    product_id="your_product_id",
    rules_df=your_rules_dataframe,
    stats_table="your_stats_table",
    target_and_error_table_writer=WrappedDataFrameWriter().mode("append").format("delta"),
    stats_table_writer=WrappedDataFrameWriter().mode("append").format("delta"),
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

