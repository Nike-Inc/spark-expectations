# Spark Expectations Streaming Writer Guide

This comprehensive guide covers the updated `SparkExpectationsWriter` class that now supports both batch and streaming DataFrames with automatic detection and built-in best practice warnings.

## Key Features

1. **Automatic Detection**: The `save_df_as_table` method automatically detects if a DataFrame is streaming using `df.isStreaming`
2. **Unified API**: Single method handles both batch and streaming DataFrames
3. **Streaming Configuration**: Supports all streaming-specific configurations like triggers, checkpoints, and output modes
4. **Query Management**: Helper methods for monitoring and stopping streaming queries
5. **Built-in Warnings**: Production-ready warning system for checkpoint location best practices

## Usage Examples

### Basic Usage (Batch DataFrame)
```python
from spark_expectations.sinks.utils.writer import SparkExpectationsWriter

# For batch DataFrames, it automatically uses df.write
config = {
    "mode": "overwrite",
    "format": "delta",
    "partitionBy": ["date"]
}

# Returns None for batch DataFrames
result = writer.save_df_as_table(batch_df, "my_table", config)
```

### Streaming DataFrame Usage
For a streaming dataframe, you have to define a stream writer for the target_and_error_table_writer and a batch writer for the stats_table_writer

```python
# For streaming DataFrames, it automatically uses df.writeStream
target_writer = (WrappedDataFrameStreamWriter()
                 .outputMode("append")
                 .format("delta")
                 .queryName("se_streaming_target_query")
                 .trigger(processingTime='5 seconds')
                 .option("checkpointLocation", "checkpoint_path/target")
                 .option("maxFilesPerTrigger", "100")
                 )
streaming_config = target_writer.build()
stats_writer = WrappedDataFrameWriter().mode("append").format("delta")

# Returns StreamingQuery for streaming DataFrames
streaming_query = writer.save_df_as_table(streaming_df, "my_streaming_table", streaming_config)
```

### Managing Streaming Queries
```python
# Check streaming query status
status = writer.get_streaming_query_status(streaming_query)
print(f"Query status: {status}")

# Stop streaming query gracefully
success = writer.stop_streaming_query(streaming_query, timeout=30)
```

## 🚨 Production Best Practices

- Disable all notifications for start, completion, failure, error drop threshold
- Set se_enable_streaming to False to disable streaming stats to Kafka (for streaming jobs stats are not calculated)

### Checkpoint Location (CRITICAL)

When using Spark Expectations (SE) with streaming DataFrames, configuring a proper checkpoint location is **critical** for production workloads. The updated `SparkExpectationsWriter` now includes built-in warnings to help ensure proper configuration.

#### Why Checkpoint Location Matters

**1. Fault Tolerance**
- Enables recovery from driver failures
- Maintains processing state across restarts
- Prevents data loss during unexpected shutdowns

**2. Exactly-Once Processing**
- Ensures each record is processed exactly once
- Prevents duplicate data in target tables
- Maintains data consistency and integrity

**3. Progress Tracking**
- Tracks which batches have been processed
- Enables resuming from the last processed offset
- Prevents reprocessing of already handled data

#### Warning System

The `SparkExpectationsWriter` provides two levels of warnings:

**Early Detection Warning** (triggered when streaming DataFrame detected without checkpoint):
```
🚨 PRODUCTION BEST PRACTICE WARNING: Streaming DataFrame detected without checkpointLocation!
For production workloads with Spark Expectations (SE), always configure a dedicated
checkpoint location to ensure fault tolerance, exactly-once processing, and proper
recovery after failures. Add 'checkpointLocation' to your streaming config options.
```

**Configuration Processing Warning** (triggered during options processing):
```
⚠️  PRODUCTION WARNING: No checkpointLocation specified for streaming DataFrame.
For production workloads, it is strongly recommended to set a dedicated checkpoint
location in the 'options' config when using Spark Expectations (SE) to write in
streaming fashion to target tables.
```

#### ✅ Correct Configuration

```python
target_writer = (WrappedDataFrameStreamWriter()
                 .outputMode("append")
                 .format("delta")
                 .queryName("se_streaming_target_query")
                 .trigger(processingTime='5 seconds')
                 .option("checkpointLocation", "checkpoint_path/target")
                 .option("maxFilesPerTrigger", "100")
                 )
streaming_config = target_writer.build()

# This will log: "Using checkpoint location: checkpoint_path/target"
streaming_query = writer.save_df_as_table(streaming_df, "my_table", streaming_config)
```

#### ❌ Avoid creating a writer class without checkpoint location
This could lead to errors
```python
# Missing checkpoint location entirely
target_writer = (WrappedDataFrameStreamWriter()
                 .outputMode("append")
                 .format("delta")
                 )
    # No options with checkpointLocation!

```

## Configuration Options

### Streaming-Specific Options
- `outputMode`: "append", "complete", or "update"
- `queryName`: Name for the streaming query
- `trigger`: Processing trigger configuration
  - `{"processingTime": "10 seconds"}` 
  - `{"once": True}`
  - `{"continuous": "1 second"}`
- `checkpointLocation`: Automatically appends table name to path

### Common Options (Both Batch and Streaming)
- `format`: "delta", "parquet", "json", etc.
- `partitionBy`: List of partition columns
- `options`: Additional writer options

## Checkpoint Location Guidelines

### 1. Dedicated Paths
Use unique checkpoint paths for each streaming job:
```
/checkpoints/spark_expectations/
├── customer_data_quality/
├── orders_data_quality/
└── inventory_data_quality/
```

### 2. Persistent Storage
- Use reliable, persistent storage (HDFS, S3, Azure Data Lake)
- Avoid local filesystems in distributed environments
- Ensure checkpoint location survives cluster restarts

### 3. Naming Conventions
Include meaningful identifiers:
```
/checkpoints/spark_expectations/{environment}/{dataset}_dq_stream/
```

Example:
```
/checkpoints/spark_expectations/prod/customer_orders_dq_stream/
```

### 4. Access Permissions
- Ensure Spark has read/write access to checkpoint location
- Use appropriate IAM roles/permissions for cloud storage
- Test checkpoint accessibility before production deployment

## Monitoring and Maintenance

### 1. Monitor Checkpoint Size
- Checkpoint directories grow over time
- Implement retention policies for old checkpoint data
- Monitor storage usage

### 2. Backup Critical Checkpoints
- For mission-critical streams, consider checkpoint backups
- Document recovery procedures
- Test recovery scenarios

### 3. Version Compatibility
- Checkpoint format can change between Spark versions
- Plan for checkpoint migration during upgrades
- Test compatibility before version updates

## Troubleshooting

### Common Issues

1. **Permission Errors**
   ```
   Solution: Verify Spark has write access to checkpoint location
   ```

2. **Checkpoint Corruption**
   ```
   Solution: Delete corrupted checkpoint (will restart from beginning)
   ```

3. **Path Conflicts**
   ```
   Solution: Use unique checkpoint paths for each streaming job
   ```

## Complete Production Example

```python
from spark_expectations.sinks.utils.writer import SparkExpectationsWriter

# Production-ready streaming configuration

target_writer = (WrappedDataFrameStreamWriter()
                 .outputMode("append")
                 .format("delta")
                 .queryName("se_streaming_target_query")
                 .trigger(processingTime='1 minute')
                 .option("checkpointLocation", f"{CONFIG['checkpoint_path']}/target")
                 .option("maxFilesPerTrigger", "500")
                 .option("maxBytesPerTrigger", "1g")
                 )
production_config = target_writer.build()

# This configuration will NOT trigger warnings
streaming_query = writer.save_df_as_table(
    df=streaming_df,
    table_name=CONFIG["target_table"], 
    config=production_config
)

# Monitor the stream
status = writer.get_streaming_query_status(streaming_query)
print(f"Stream status: {status}")

# Graceful shutdown when needed
success = writer.stop_streaming_query(streaming_query, timeout=60)
if success:
    print("Stream stopped successfully")
```
## Checkout more on example notebook for usage

[spark_expectation_streaming_dbx.ipynb](https://github.com/Nike-Inc/spark-expectations/blob/main/examples/notebooks/spark_expectation_streaming_dbx.ipynb)

## Implementation Details

The updated `save_df_as_table` method:

1. **Detects DataFrame Type**: Checks `df.isStreaming` to determine if DataFrame is streaming
2. **Issues Warnings**: Logs warnings if checkpoint location is missing for streaming DataFrames
3. **Uses Appropriate Writer**: 
   - `df.writeStream` for streaming DataFrames
   - `df.write` for batch DataFrames
4. **Applies Configuration**: Handles streaming-specific options like triggers and output modes
5. **Returns Appropriate Type**: 
   - `StreamingQuery` for streaming DataFrames
   - `None` for batch DataFrames
6. **Manages Table Properties**: Sets product_id and other metadata for both streaming and batch tables
7. **Handles Errors Gracefully**: Comprehensive exception handling and logging

## Key Takeaways

- ✅ **Always configure checkpoint locations** for production streaming workloads
- ✅ **Use dedicated checkpoint paths** for each streaming job
- ✅ **Monitor checkpoint health** and implement retention policies
- ✅ **Test recovery scenarios** before production deployment
- ✅ **Pay attention to warnings** - they help prevent production issues

**Remember: Checkpoint locations are not optional for production streaming workloads with Spark Expectations!**