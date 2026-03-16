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
from spark_expectations.config.user_config import Constants as user_config

# Configure for serverless environment
user_conf = {
    user_config.is_serverless: True,
    user_config.se_notifications_enable_email: False,
    user_config.se_notifications_enable_slack: True,
    user_config.se_enable_error_table: True,
    user_config.se_enable_query_dq_detailed_result: True,
    user_config.se_dq_rules_params: {
        "env": "local",
        "table": "orders",
    },
}

writer = WrappedDataFrameWriter().mode("append").format("delta")

se = SparkExpectations(
    product_id="your_product_id",
    rules_df=your_rules_dataframe,
    stats_table="your_stats_table",
    target_and_error_table_writer=writer,
    stats_table_writer=writer,
    stats_streaming_options={user_config.se_enable_streaming: False},
)

@se.with_expectations(
    target_table="your_target_table",
    write_to_table=True,
    user_conf=user_conf,
)
def process_data():
    return processed_dataframe

result_df = process_data()
```

