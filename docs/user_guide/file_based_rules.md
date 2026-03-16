# File-Based Rules (YAML & JSON)

Instead of managing data quality rules in a database table with SQL INSERT statements,
you can define them in **YAML** or **JSON** files and load them directly into a Spark
DataFrame. This approach makes rules easy to version-control, review in pull requests,
and share across environments.

## Loading Rules

```python
from spark_expectations.rules import load_rules

# Auto-detect format from file extension, selecting the "DEV" environment
rules_df = load_rules("path/to/rules.yaml", options={"dq_env": "DEV"})
rules_df = load_rules("path/to/rules.json", options={"dq_env": "DEV"})
```

You can also be explicit about the format or use the format-specific helpers:

```python
from spark_expectations.rules import load_rules, load_rules_from_yaml, load_rules_from_json

# Explicit format
rules_df = load_rules("path/to/rules.yaml", format="yaml", options={"dq_env": "DEV"})

# Convenience helpers
rules_df = load_rules_from_yaml("path/to/rules.yaml", options={"dq_env": "DEV"})
rules_df = load_rules_from_json("path/to/rules.json", options={"dq_env": "DEV"})
```

All loader functions accept an optional `spark` parameter. If omitted, the active
`SparkSession` is used automatically. You can pass one explicitly when needed:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
rules_df = load_rules_from_yaml("path/to/rules.yaml", spark, options={"dq_env": "DEV"})
```

The `options={"dq_env": "<env>"}` parameter selects which environment block to use
when the rules file contains a `dq_env` section. See [Environment-Aware Rules](#environment-aware-rules-dq_env)
below.

The returned `rules_df` has the same schema as the rules table and can be passed
directly to `SparkExpectations`:

```python
from spark_expectations.core.expectations import SparkExpectations, WrappedDataFrameWriter

se = SparkExpectations(
    product_id="your_product",
    rules_df=rules_df,          # <-- loaded from YAML/JSON
    stats_table="catalog.schema.stats",
    stats_table_writer=WrappedDataFrameWriter().mode("append").format("delta"),
    target_and_error_table_writer=WrappedDataFrameWriter().mode("append").format("delta"),
)
```

---

## Environment-Aware Rules (`dq_env`)

The recommended format uses a `dq_env` section to define per-environment settings
such as `table_name`, `action_if_failed`, and `priority`. This lets you keep a
single rules file that works across dev, QA, and production by simply switching
the `dq_env` option at load time.

=== "YAML"

    ```yaml
    product_id: your_product

    dq_env:
      DEV:
        table_name: catalog_dev.schema.orders
        action_if_failed: ignore
        is_active: true
        priority: medium
      QA:
        table_name: catalog_qa.schema.orders
        action_if_failed: ignore
        is_active: true
        priority: medium
      PROD:
        table_name: catalog_prod.schema.orders
        action_if_failed: fail
        is_active: true
        priority: high

    rules:
      - rule: order_id_not_null
        rule_type: row_dq
        column_name: order_id
        expectation: "order_id IS NOT NULL"
        action_if_failed: drop
        tag: completeness
        description: "Order ID must not be null"
        priority: high

      - rule: total_positive
        rule_type: row_dq
        column_name: total
        expectation: "total > 0"
        tag: validity
        description: "Total must be positive"

      - rule: row_count
        rule_type: agg_dq
        expectation: "count(*) > 0"
        action_if_failed: fail
        tag: completeness
        description: "Table must have rows"
    ```

=== "JSON"

    ```json
    {
      "product_id": "your_product",
      "dq_env": {
        "DEV": {
          "table_name": "catalog_dev.schema.orders",
          "action_if_failed": "ignore",
          "is_active": true,
          "priority": "medium"
        },
        "QA": {
          "table_name": "catalog_qa.schema.orders",
          "action_if_failed": "ignore",
          "is_active": true,
          "priority": "medium"
        },
        "PROD": {
          "table_name": "catalog_prod.schema.orders",
          "action_if_failed": "fail",
          "is_active": true,
          "priority": "high"
        }
      },
      "rules": [
        {
          "rule": "order_id_not_null",
          "rule_type": "row_dq",
          "column_name": "order_id",
          "expectation": "order_id IS NOT NULL",
          "action_if_failed": "drop",
          "tag": "completeness",
          "description": "Order ID must not be null",
          "priority": "high"
        },
        {
          "rule": "total_positive",
          "rule_type": "row_dq",
          "column_name": "total",
          "expectation": "total > 0",
          "tag": "validity",
          "description": "Total must be positive"
        },
        {
          "rule": "row_count",
          "rule_type": "agg_dq",
          "expectation": "count(*) > 0",
          "action_if_failed": "fail",
          "tag": "completeness",
          "description": "Table must have rows"
        }
      ]
    }
    ```

**Structure:**

- Top-level `product_id` identifies the product.
- `dq_env` is a mapping of environment names (e.g. `DEV`, `QA`, `PROD`) to
  environment-specific settings. Each environment block can contain:
    - `table_name` -- the table the rules apply to in that environment.
    - Any default field (`action_if_failed`, `is_active`, `priority`, etc.) that
      applies to all rules unless a rule overrides it.
- `rules` is a flat list of rule definitions. Each rule needs at least `rule` and
  `expectation`.
- When loading, pass `options={"dq_env": "<env>"}` to select the environment:

```python
# For development
rules_df = load_rules_from_yaml("rules.yaml", options={"dq_env": "DEV"})

# For production
rules_df = load_rules_from_yaml("rules.yaml", options={"dq_env": "PROD"})
```

**Defaults cascade:** built-in defaults --> `dq_env[<env>]` values --> per-rule overrides.

---

## Defaults

The `dq_env` environment values let you set values that
apply to every rule unless a rule explicitly overrides them. This avoids repeating
common settings on every single rule.

The built-in defaults (used when neither the file nor the rule specifies a value) are:

| Field                              | Default   |
|------------------------------------|-----------|
| `action_if_failed`                 | `ignore`  |
| `enable_for_source_dq_validation`  | `true`    |
| `enable_for_target_dq_validation`  | `true`    |
| `is_active`                        | `true`    |
| `enable_error_drop_alert`          | `false`   |
| `error_drop_threshold`             | `0`       |
| `priority`                         | `medium`  |

---

## Rules Schema Reference

Every rule, regardless of input format, is normalised into a row with these 17 columns:

| Column                             | Required | Description                                                                                       |
|------------------------------------|:--------:|---------------------------------------------------------------------------------------------------|
| `product_id`                       | Yes      | Unique product identifier for DQ execution                                                        |
| `table_name`                       | Yes      | The table the rule applies to                                                                     |
| `rule_type`                        | Yes      | `row_dq`, `agg_dq`, or `query_dq`                                                                |
| `rule`                             | Yes      | Short name for the rule                                                                           |
| `expectation`                      | Yes      | The DQ rule condition (SQL expression)                                                            |
| `column_name`                      |          | Column the rule applies to (relevant for `row_dq`)                                                |
| `action_if_failed`                 |          | `ignore`, `drop` (row_dq only), or `fail`                                                        |
| `tag`                              |          | Category tag (e.g. `completeness`, `validity`)                                                    |
| `description`                      |          | Human-readable description of the rule                                                            |
| `enable_for_source_dq_validation`  |          | Run agg/query rules on the source DataFrame                                                      |
| `enable_for_target_dq_validation`  |          | Run agg/query rules on the post-row_dq DataFrame                                                 |
| `is_active`                        |          | Whether the rule is active                                                                        |
| `enable_error_drop_alert`          |          | Send alert when rows are dropped                                                                  |
| `error_drop_threshold`             |          | Threshold for error drop alerts                                                                   |
| `query_dq_delimiter`               |          | Delimiter for custom query_dq alias queries (default `@`)                                         |
| `enable_querydq_custom_output`     |          | Capture custom query output in a separate table                                                   |
| `priority`                         |          | `low`, `medium`, or `high`                                                                        |

---

## Full Example

Here is a complete example loading rules from YAML with `dq_env` and running DQ checks:

```python
from pyspark.sql import DataFrame, SparkSession
from spark_expectations.rules import load_rules_from_yaml
from spark_expectations.core.expectations import SparkExpectations, WrappedDataFrameWriter
from spark_expectations.config.user_config import Constants as user_config

spark = SparkSession.builder.getOrCreate()

# Load rules from YAML, selecting the "DEV" environment
rules_df = load_rules_from_yaml("path/to/rules.yaml", spark, options={"dq_env": "DEV"})

# Configure writer and streaming
writer = WrappedDataFrameWriter().mode("append").format("delta")
streaming_config = {user_config.se_enable_streaming: False}

se = SparkExpectations(
    product_id="your_product",
    rules_df=rules_df,
    stats_table="catalog.schema.dq_stats",
    stats_table_writer=writer,
    target_and_error_table_writer=writer,
    stats_streaming_options=streaming_config,
)

@se.with_expectations(
    target_table="catalog.schema.orders",
    write_to_table=True,
    write_to_temp_table=True,
)
def process_orders() -> DataFrame:
    return spark.read.table("catalog.schema.raw_orders")

process_orders()
```

---

## Simple Format (Without `dq_env`)

If you don't need per-environment configuration, a simpler format is supported with a top-level `table_name` and optional `defaults`:

=== "YAML"

    ```yaml
    product_id: my_product
    table_name: catalog.schema.orders
    defaults:
      action_if_failed: ignore
      is_active: true
      priority: medium
    rules:
      - rule: order_id_not_null
        rule_type: row_dq
        column_name: order_id
        expectation: "order_id IS NOT NULL"
        action_if_failed: drop
        tag: completeness
        description: "Order ID must not be null"
    ```

=== "JSON"

    ```json
    {
      "product_id": "my_product",
      "table_name": "catalog.schema.orders",
      "defaults": {
        "action_if_failed": "ignore",
        "is_active": true,
        "priority": "medium"
      },
      "rules": [
        {
          "rule": "order_id_not_null",
          "rule_type": "row_dq",
          "column_name": "order_id",
          "expectation": "order_id IS NOT NULL",
          "action_if_failed": "drop",
          "tag": "completeness",
          "description": "Order ID must not be null"
        }
      ]
    }
    ```

---

## `query_dq` Rules in YAML/JSON

Query DQ rules use SQL queries as expectations. For composite queries with custom delimiters, encode them as a single string:

```yaml
rules:
  - rule: product_count_threshold
    rule_type: query_dq
    column_name: ""
    expectation: >-
      ((select count(*) from ({source_f1}) a) -
       (select count(*) from ({target_f1}) b)) > 3
      @source_f1@SELECT DISTINCT product_id, order_id FROM order_source
      @target_f1@SELECT DISTINCT product_id, order_id FROM order_target
    action_if_failed: ignore
    tag: validity
    description: "Row count difference between source and target must exceed 3"
    query_dq_delimiter: "@"
    enable_querydq_custom_output: true
```

!!! tip
    Use YAML's `>-` folded block scalar to write long expectations across multiple lines without embedded newlines.

---

## Additional Behaviors

- **Case-insensitive `dq_env` lookup**: `DEV`, `dev`, and `Dev` all match the same environment block.
- **Per-rule overrides**: Any field set directly on a rule overrides the value from `dq_env` or `defaults`. For example, a single rule can set its own `table_name` or `action_if_failed`.
- **Type casting**: String values like `"true"`, `"false"`, `"1"`, `"0"` are automatically cast to their correct Python types for boolean and integer columns.
- **Validation**: The loader validates that every rule has the required fields (`rule`, `expectation`) and a valid `rule_type` (`row_dq`, `agg_dq`, `query_dq`). Invalid rules raise `SparkExpectationsUserInputOrConfigInvalidException` with a descriptive message.

---

## When to Use File-Based vs Table-Based Rules

| | File-Based (YAML/JSON) | Table-Based (Delta/BigQuery) |
|---|---|---|
| **Version control** | Rules live alongside code in git, reviewable in PRs | Changes require SQL updates or a separate process |
| **Environment management** | `dq_env` selects dev/qa/prod from a single file | Separate tables or filtered queries per environment |
| **Dynamic updates** | Requires code deployment to change rules | Rules can be updated without redeploying code |
| **Discoverability** | Rules are visible in the repo | Rules live in the data platform |

!!! tip
    File-based rules are ideal for teams that want rules reviewed in pull requests. Table-based rules work well when business users or data stewards need to manage rules independently of code deployments.

---

## Working Examples

- **Sample YAML rules**: [`examples/resources/sample_rules.yaml`](https://github.com/Nike-Inc/spark-expectations/blob/main/examples/resources/sample_rules.yaml)
- **Sample JSON rules**: [`examples/resources/sample_rules.json`](https://github.com/Nike-Inc/spark-expectations/blob/main/examples/resources/sample_rules.json)
- **Runnable script**: [`examples/scripts/sample_dq_yaml_json.py`](https://github.com/Nike-Inc/spark-expectations/blob/main/examples/scripts/sample_dq_yaml_json.py)

---

## Custom Rule Loader Plugins

The rule loader system uses [pluggy](https://pluggy.readthedocs.io/), so you can register custom loaders for other formats (e.g., TOML, CSV, remote APIs). Implement the `SparkExpectationsRuleLoader` hookspec and register via a setuptools entry point:

```toml
[project.entry-points."spark_expectations_rule_loader"]
my_loader = "my_package.loaders:MyCustomRuleLoaderImpl"
```

The plugin manager loads all registered entry points automatically alongside the built-in YAML and JSON loaders.
