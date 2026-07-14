# DataFrame Writer Configuration

`WrappedDataFrameWriter` and `WrappedDataFrameStreamWriter` are builder classes that wrap PySpark's `DataFrameWriter` and `DataStreamWriter` APIs into configuration dictionaries. Instead of chaining methods on a live DataFrame (e.g., `df.write.mode("overwrite").format("delta")`), you build a config dict that Spark Expectations applies when writing target tables, error tables, and stats tables.

!!! tip "Why use these builders?"
    Spark Expectations needs to know *how* to write data before your function returns a DataFrame. The wrapped writers let you declare write configuration once and reuse it across multiple tables (target, error, stats) without tying the config to a specific DataFrame instance.

## Import

```python
from spark_expectations.core.expectations import (
    SparkExpectations,
    WrappedDataFrameWriter,
    WrappedDataFrameStreamWriter,
)
```

---

## WrappedDataFrameWriter (Batch)

Builder for **batch** DataFrame write configuration. All methods return `self` for chaining.

| Method | Description |
|--------|-------------|
| `mode(saveMode: str)` | Set write mode: `"overwrite"`, `"append"`, `"ignore"`, `"errorifexists"` |
| `format(source: str)` | Set format: `"delta"`, `"parquet"`, `"bigquery"`, `"iceberg"`, etc. |
| `partitionBy(*columns: str)` | Set partition columns |
| `option(key: str, value: str)` | Set a single write option |
| `options(**options: str)` | Set multiple write options |
| `bucketBy(num_buckets: int, *columns: str)` | Set bucketing (not supported for Delta) |
| `sortBy(*columns: str)` | Set sort columns within buckets |
| `build() -> Dict` | Returns the configuration dictionary |

!!! warning "Bucketing and Delta"
    `bucketBy` is **not supported** for Delta tables. Calling `build()` with `format("delta")` and `bucketBy(...)` will raise `SparkExpectationsMiscException`.

### Example

```python title="Batch writer chaining"
writer = (
    WrappedDataFrameWriter()
    .mode("overwrite")
    .format("delta")
    .partitionBy("date", "region")
    .option("compression", "snappy")
    .options(mergeSchema="true")
    .build()
)
```

---

## WrappedDataFrameStreamWriter (Streaming)

Builder for **streaming** DataFrame write configuration. All methods return `self` for chaining.

| Method | Description |
|--------|-------------|
| `outputMode(output_mode: str)` | Set output mode: `"append"`, `"complete"`, `"update"` |
| `format(source: str)` | Set format: `"delta"`, `"parquet"`, etc. |
| `queryName(query_name: str)` | Set streaming query name |
| `trigger(**trigger_options)` | Set trigger: `processingTime="10 seconds"`, `once=True`, `continuous="1 second"` |
| `partitionBy(*columns)` | Set partition columns (accepts `str` or `List[str]`) |
| `option(key: str, value: str)` | Set a single option (e.g. `checkpointLocation`) |
| `options(**options: str)` | Set multiple options |
| `build() -> Dict` | Returns the configuration dictionary |

### Trigger options

| Trigger | Example | Use case |
|---------|---------|----------|
| `processingTime` | `trigger(processingTime="10 seconds")` | Process every N seconds |
| `once` | `trigger(once=True)` | Process available data once and stop |
| `continuous` | `trigger(continuous="1 second")` | Continuous processing with checkpoints |

### Example

```python title="Streaming writer chaining"
stream_writer = (
    WrappedDataFrameStreamWriter()
    .outputMode("append")
    .format("delta")
    .queryName("se_streaming_target_query")
    .trigger(processingTime="10 seconds")
    .option("checkpointLocation", "/path/to/checkpoint")
    .partitionBy("date")
    .build()
)
```

---

## Passing Writers to SparkExpectations

Pass writers to the `SparkExpectations` constructor via:

- **`target_and_error_table_writer`** — Used for the target table and error table (passing records, error records).
- **`stats_table_writer`** — Used for the stats table and detailed stats tables.

Pass the **builder instance** (e.g., `WrappedDataFrameWriter()` or `WrappedDataFrameStreamWriter()`). Spark Expectations calls `.build()` internally to obtain the config.

```python title="SparkExpectations with writers"
writer = WrappedDataFrameWriter().mode("append").format("delta")

se = SparkExpectations(
    product_id="your_product",
    rules_df=spark.table("dq_rules"),
    stats_table="dq_stats",
    target_and_error_table_writer=writer,
    stats_table_writer=writer,
    debugger=False,
    stats_streaming_options={user_config.se_enable_streaming: False},
)
```

!!! note "Per-decorator override"
    You can override `target_and_error_table_writer` per `@se.with_expectations(...)` call using the `target_and_error_table_writer` argument on the decorator.

---

## Usage Examples

### Delta (Batch)

```python title="Delta batch write"
from spark_expectations.config.user_config import Constants as user_config
from spark_expectations.core.expectations import (
    SparkExpectations,
    WrappedDataFrameWriter,
)

writer = WrappedDataFrameWriter().mode("append").format("delta")

se = SparkExpectations(
    product_id="your_product",
    rules_df=spark.table("dq_spark_local.dq_rules"),
    stats_table="dq_spark_local.dq_stats",
    stats_table_writer=writer,
    target_and_error_table_writer=writer,
    debugger=False,
    stats_streaming_options={user_config.se_enable_streaming: False},
)

@se.with_expectations(target_table="dq_spark_local.customer_order", write_to_table=True)
def build_new():
    return spark.read.csv("order.csv", header=True, inferSchema=True)
```

### BigQuery (Batch)

```python title="BigQuery batch write"
from spark_expectations.config.user_config import Constants as user_config

writer = (
    WrappedDataFrameWriter()
    .mode("overwrite")
    .format("bigquery")
    .option("createDisposition", "CREATE_IF_NEEDED")
    .option("writeMethod", "direct")
)

se = SparkExpectations(
    product_id="your_product",
    rules_df=spark.read.format("bigquery").load("<project>.<dataset>.<rules_table>"),
    stats_table="<project>.<dataset>.<stats_table>",
    stats_table_writer=writer,
    target_and_error_table_writer=writer,
    debugger=False,
    stats_streaming_options={user_config.se_enable_streaming: False},
)

@se.with_expectations(
    target_table="<project>.<dataset>.<target_table>",
    write_to_table=True,
)
def build_new():
    return spark.read.csv("order.csv", header=True, inferSchema=True)
```

### Iceberg (Batch)

```python title="Iceberg batch write"
from spark_expectations.config.user_config import Constants as user_config

writer = WrappedDataFrameWriter().mode("append").format("iceberg")

se = SparkExpectations(
    product_id="your_product",
    rules_df=spark.sql("select * from dq_spark_local.dq_rules"),
    stats_table="dq_spark_local.dq_stats",
    stats_table_writer=writer,
    target_and_error_table_writer=writer,
    debugger=False,
    stats_streaming_options={user_config.se_enable_streaming: False},
)

@se.with_expectations(target_table="dq_spark_local.customer_order", write_to_table=True)
def build_new():
    return spark.read.csv("order.csv", header=True, inferSchema=True)
```

### Streaming Delta

```python title="Streaming Delta write"
from spark_expectations.config.user_config import Constants as user_config
from spark_expectations.core.expectations import (
    SparkExpectations,
    WrappedDataFrameWriter,
    WrappedDataFrameStreamWriter,
)

# Target/error tables: streaming writer
target_writer = (
    WrappedDataFrameStreamWriter()
    .outputMode("append")
    .format("delta")
    .queryName("se_streaming_target_query")
    .trigger(processingTime="5 seconds")
    .option("checkpointLocation", "/path/to/checkpoint/target")
    .partitionBy("date")
)

# Stats table: batch writer (always)
stats_writer = WrappedDataFrameWriter().mode("append").format("delta")

se = SparkExpectations(
    product_id="your_product",
    rules_df=spark.table("dq_rules"),
    stats_table="dq_stats",
    stats_table_writer=stats_writer,
    target_and_error_table_writer=target_writer,
    debugger=False,
    stats_streaming_options={user_config.se_enable_streaming: False},
)

@se.with_expectations(target_table="dq_streaming.customer_order", write_to_table=True)
def build_streaming():
    return spark.readStream.format("delta").table("source_table")
```

!!! important "Stats table is always batch"
    For streaming workloads, `target_and_error_table_writer` must be a `WrappedDataFrameStreamWriter`, but **`stats_table_writer` must remain a `WrappedDataFrameWriter`** (batch). Stats are written as batch tables.

---

## Checkpoint Location (Streaming)

For streaming writes, `checkpointLocation` is **required** in production to ensure fault tolerance and exactly-once processing.

!!! danger "Production requirement"
    Without `checkpointLocation`, Spark cannot recover from failures or guarantee exactly-once semantics. The writer logs a warning when it detects streaming without a checkpoint.

### Recommended configuration

```python title="Streaming with checkpoint"
target_writer = (
    WrappedDataFrameStreamWriter()
    .outputMode("append")
    .format("delta")
    .queryName("se_streaming_target_query")
    .trigger(processingTime="1 minute")
    .option("checkpointLocation", "/checkpoints/spark_expectations/prod/customer_orders_dq")
    .option("maxFilesPerTrigger", "500")
)
```

The writer automatically appends the table name to the checkpoint path to avoid conflicts when the same config is reused for multiple tables.

---

## Batch vs Streaming Data Path

| | Batch | Streaming |
|---|---|---|
| **Writer for target/error** | `WrappedDataFrameWriter` | `WrappedDataFrameStreamWriter` |
| **Writer for stats** | `WrappedDataFrameWriter` | `WrappedDataFrameWriter` (always batch) |
| **DQ rules applied** | Row DQ, Agg DQ, Query DQ | Row DQ only |
| **Write method** | `df.write.saveAsTable` | `df.writeStream.toTable` |
| **Checkpoint** | Not needed | Required (`checkpointLocation`) |

!!! info "Streaming limitations"
    For streaming DataFrames, only **row-level DQ** is applied. Aggregate and query DQ are not supported on streaming data.
