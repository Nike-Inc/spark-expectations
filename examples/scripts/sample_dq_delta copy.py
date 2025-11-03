# Define the product_id

import os
from typing import Dict, Union

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from spark_expectations import _log
from spark_expectations.config.user_config import Constants as user_config
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core.expectations import (
    SparkExpectations,
    WrappedDataFrameWriter,
    WrappedDataFrameStreamWriter
)
from examples.scripts.base_setup import set_up_delta
from spark_expectations.notifications.push.alert import SparkExpectationsAlert
from spark_expectations.utils.reader import SparkExpectationsReader


writer = WrappedDataFrameWriter().mode("append").format("delta")
stream_writer = WrappedDataFrameStreamWriter().outputMode("append").format("delta").option("checkpointLocation", "/tmp/checkpoint")

spark = set_up_delta()
dic_job_info = {
    "job": "job_name",
    "Region": "NA",
    "env": "dev",
    "Snapshot": "2024-04-15",
    "data_object_name ": "customer_order",
}
job_info = str(dic_job_info)

CONFIG ={}
CONFIG["product_id"] = "testProductid"
CONFIG["target_table"] = "testTargetTable"



import pandas as pd

rules_data = [
    {
        "product_id": CONFIG["product_id"],
        "table_name": CONFIG["target_table"],
        "rule_type": "row_dq",
        "rule": "age_not_null",
        "column_name": "age",
        "expectation": "age IS NOT NULL",
        "action_if_failed": "drop",  # For streaming, use 'ignore' to keep processing
        "tag": "completeness",
        "description": "Age must not be null",
        "enable_for_source_dq_validation": True,
        "enable_for_target_dq_validation": False,
        "is_active": True,
        "enable_error_drop_alert": False,
        "error_drop_threshold": 0,
        "query_dq_delimiter": "",
        "enable_querydq_custom_output": False,
        "priority": "medium",
    },
    {
        "product_id": CONFIG["product_id"],
        "table_name": CONFIG["target_table"],
        "rule_type": "row_dq",
        "rule": "age_range_valid",
        "column_name": "age",
        "expectation": "age BETWEEN 10 AND 100",
        "action_if_failed": "ignore",
        "tag": "validity",
        "description": "Age must be between 10 and 100",
        "enable_for_source_dq_validation": True,
        "enable_for_target_dq_validation": False,
        "is_active": True,
        "enable_error_drop_alert": False,
        "error_drop_threshold": 0,
        "query_dq_delimiter": "",
        "enable_querydq_custom_output": False,
        "priority": "high",
    },
    {
        "product_id": CONFIG["product_id"],
        "table_name": CONFIG["target_table"],
        "rule_type": "row_dq",
        "rule": "email_not_null",
        "column_name": "email",
        "expectation": "email IS NOT NULL",
        "action_if_failed": "ignore",
        "tag": "completeness",
        "description": "Email must not be null",
        "enable_for_source_dq_validation": True,
        "enable_for_target_dq_validation": False,
        "is_active": True,
        "enable_error_drop_alert": False,
        "error_drop_threshold": 0,
        "query_dq_delimiter": "",
        "enable_querydq_custom_output": False,
        "priority": "medium",
    },
    {
        "product_id": CONFIG["product_id"],
        "table_name": CONFIG["target_table"],
        "rule_type": "row_dq",
        "rule": "email_format_valid",
        "column_name": "email",
        "expectation": "email LIKE '%@%.%'",
        "action_if_failed": "ignore",
        "tag": "validity",
        "description": "Email must be in valid format",
        "enable_for_source_dq_validation": True,
        "enable_for_target_dq_validation": False,
        "is_active": True,
        "enable_error_drop_alert": False,
        "error_drop_threshold": 0,
        "query_dq_delimiter": "",
        "enable_querydq_custom_output": False,
        "priority": "low",
    },
]

rules_df = spark.createDataFrame(pd.DataFrame(rules_data))

notification_conf = {"se_enable_error_table": True}



se: SparkExpectations = SparkExpectations(
    product_id="your_product",
    rules_df=rules_df,
    stats_table="dq_spark_dev.dq_stats",
    stats_table_writer=writer,
    target_and_error_table_writer=stream_writer,
    debugger=False,
    stats_streaming_options={user_config.se_enable_streaming: False},
)

@se.with_expectations(
    target_table="dq_spark_dev.customer_order",
    write_to_table=True,
    user_conf=notification_conf,
    target_table_view="order",
)
def build_new() -> DataFrame:
    from pyspark.sql.functions import col, expr, when, lit

    # Create a streaming DataFrame using rate source
    # This generates rows with columns: timestamp, value
    streaming_source = (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", "5")  # Generate 5 rows per second
        .option("numPartitions", "2")
        .load()
    )

    # Transform the streaming data to add meaningful columns for DQ checks
    streaming_df = (
        streaming_source
        .withColumn("id", col("value"))
        .withColumn("age", (col("value") % 50) + 10)  # Age between 10-59
        .withColumn(
            "email",
            when(col("value") % 10 == 0, lit(None))  # Every 10th record has null email
            .otherwise(expr("concat('user', value, '@example.com')"))
        )
        .withColumn("name", expr("concat('User_', value)"))
        .select("id", "age", "email", "name", "timestamp")
    )

    print("✓ Streaming source created")
    print(f"  Is streaming: {streaming_df.isStreaming}")
    print("  Schema:")
    streaming_df.printSchema()
    return streaming_df


if __name__ == "__main__":
    import time
    
    _log.info("Starting streaming DQ example with Delta Lake...")
    
    try:
        # Call the decorated function to start streaming
        query = build_new()
        
        if hasattr(query, 'isActive') and query.isActive:
            _log.info(f"✅ Streaming query started: {query.name}")
            _log.info(f"   Query ID: {query.id}")
            _log.info("   Running for 30 seconds...")
            
            # Monitor for 30 seconds
            for i in range(2):
                if query.isActive:
                    progress = query.lastProgress
                    if progress:
                        batch_id = progress.get('batchId', 'N/A')
                        _log.info(f"   📊 Processing batch: {batch_id}")
                    #time.sleep(5)
                else:
                    break
            
            query.stop()
            _log.info("🏁 Streaming query stopped successfully")
        else:
            _log.info("✅ Processed as batch DataFrame")
            
    except Exception as e:
        _log.error(f"❌ Example failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Cleanup
        try:
            for active_query in spark.streams.active:
                if active_query.isActive:
                    active_query.stop()
        except:
            pass
        
        spark.stop()
        _log.info("🎉 Example completed!")
