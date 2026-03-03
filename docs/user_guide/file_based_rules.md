# File-Based Rules (YAML & JSON)

Instead of managing data quality rules in a database table with SQL INSERT statements,
you can define them in **YAML** or **JSON** files and load them directly into a Spark
DataFrame. This approach makes rules easy to version-control, review in pull requests,
and share across environments.

## Loading Rules

```python
from spark_expectations.rules import load_rules

# Auto-detect format from file extension
rules_df = load_rules("path/to/rules.yaml")
rules_df = load_rules("path/to/rules.json")
```

You can also be explicit about the format or use the format-specific helpers:

```python
from spark_expectations.rules import load_rules, load_rules_from_yaml, load_rules_from_json

# Explicit format
rules_df = load_rules("path/to/rules.yaml", format="yaml")

# Convenience helpers
rules_df = load_rules_from_yaml("path/to/rules.yaml")
rules_df = load_rules_from_json("path/to/rules.json")
```

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

## File Formats

Rules files support **four authoring styles**. The loader auto-detects which style
you are using and normalises all of them into the same flat DataFrame. Pick whichever
structure fits your use case best.

### Defaults

All formats (except flat) support a `defaults` block. Values set in `defaults`
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

### Format 1: Validations (multi-product)

Best for: **multiple products or tables in a single file**.

=== "YAML"

    ```yaml
    defaults:
      action_if_failed: ignore
      is_active: true
      priority: medium

    validations:
      - product_id: product_a
        table_name: catalog.schema.orders
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

      - product_id: product_b
        table_name: catalog.schema.customers
        defaults:
          action_if_failed: fail    # override for this block
        rules:
          - rule: name_not_null
            rule_type: row_dq
            column_name: name
            expectation: "name IS NOT NULL"
            tag: completeness
            description: "Customer name is required"
    ```

=== "JSON"

    ```json
    {
      "defaults": {
        "action_if_failed": "ignore",
        "is_active": true,
        "priority": "medium"
      },
      "validations": [
        {
          "product_id": "product_a",
          "table_name": "catalog.schema.orders",
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
      ]
    }
    ```

**Structure:**

- Top-level `defaults` (optional) -- shared across all entries.
- `validations` -- a list where each entry has `product_id`, `table_name`, optional
  entry-level `defaults`, and a `rules` list.
- Defaults cascade: built-in --> top-level `defaults` --> entry-level `defaults` --> per-rule overrides.

---

### Format 2: Rules List (single product)

Best for: **one product and one table** with a simple flat list of rules.

=== "YAML"

    ```yaml
    product_id: your_product
    table_name: catalog.schema.orders
    defaults:
      action_if_failed: ignore
      rule_type: row_dq

    rules:
      - rule: id_not_null
        expectation: "id IS NOT NULL"
        action_if_failed: drop
        tag: completeness
        description: "ID must not be null"

      - rule: amount_positive
        expectation: "amount > 0"
        tag: validity
        description: "Amount must be positive"

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
      "table_name": "catalog.schema.orders",
      "defaults": {
        "action_if_failed": "ignore",
        "rule_type": "row_dq"
      },
      "rules": [
        {
          "rule": "id_not_null",
          "expectation": "id IS NOT NULL",
          "action_if_failed": "drop",
          "tag": "completeness",
          "description": "ID must not be null"
        },
        {
          "rule": "amount_positive",
          "expectation": "amount > 0",
          "tag": "validity",
          "description": "Amount must be positive"
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

- Top-level `product_id` and `table_name` apply to every rule.
- `defaults` (optional) can set any column value, including `rule_type`.
- Each rule only needs `rule` and `expectation`; everything else inherits from defaults.

---

### Format 3: Hierarchical (grouped by table and rule type)

Best for: **organising rules by table and then by rule type** (`row_dq`, `agg_dq`,
`query_dq`). The `table_name` and `rule_type` are inferred from the nesting, so you
never repeat them on individual rules.

=== "YAML"

    ```yaml
    product_id: your_product
    defaults:
      action_if_failed: ignore

    tables:
      catalog.schema.orders:
        row_dq:
          - rule: id_not_null
            column_name: id
            expectation: "id IS NOT NULL"
            action_if_failed: drop
            tag: completeness
            description: "ID must not be null"

          - rule: amount_positive
            column_name: amount
            expectation: "amount > 0"
            tag: validity
            description: "Amount must be positive"

        agg_dq:
          - rule: row_count
            expectation: "count(*) > 0"
            action_if_failed: fail
            tag: completeness
            description: "Table must have rows"

      catalog.schema.customers:
        row_dq:
          - rule: name_not_null
            column_name: name
            expectation: "name IS NOT NULL"
            tag: completeness
            description: "Customer name is required"
    ```

=== "JSON"

    ```json
    {
      "product_id": "your_product",
      "defaults": {
        "action_if_failed": "ignore"
      },
      "tables": {
        "catalog.schema.orders": {
          "row_dq": [
            {
              "rule": "id_not_null",
              "column_name": "id",
              "expectation": "id IS NOT NULL",
              "action_if_failed": "drop",
              "tag": "completeness",
              "description": "ID must not be null"
            },
            {
              "rule": "amount_positive",
              "column_name": "amount",
              "expectation": "amount > 0",
              "tag": "validity",
              "description": "Amount must be positive"
            }
          ],
          "agg_dq": [
            {
              "rule": "row_count",
              "expectation": "count(*) > 0",
              "action_if_failed": "fail",
              "tag": "completeness",
              "description": "Table must have rows"
            }
          ]
        }
      }
    }
    ```

**Structure:**

- `tables` is a nested mapping: `table_name` --> `rule_type` --> list of rules.
- `rule_type` must be one of `row_dq`, `agg_dq`, or `query_dq`.
- Each rule only needs `rule` and `expectation`.

---

### Format 4: Flat (fully explicit)

Best for: **maximum explicitness** -- every rule is a self-contained dict with all
required fields. No defaults, no nesting, no inference.

=== "YAML"

    ```yaml
    - product_id: your_product
      table_name: catalog.schema.orders
      rule_type: row_dq
      rule: id_not_null
      column_name: id
      expectation: "id IS NOT NULL"
      action_if_failed: drop
      tag: completeness
      description: "ID must not be null"

    - product_id: your_product
      table_name: catalog.schema.orders
      rule_type: agg_dq
      rule: row_count
      expectation: "count(*) > 0"
      action_if_failed: fail
      tag: completeness
      description: "Table must have rows"
    ```

=== "JSON"

    ```json
    [
      {
        "product_id": "your_product",
        "table_name": "catalog.schema.orders",
        "rule_type": "row_dq",
        "rule": "id_not_null",
        "column_name": "id",
        "expectation": "id IS NOT NULL",
        "action_if_failed": "drop",
        "tag": "completeness",
        "description": "ID must not be null"
      },
      {
        "product_id": "your_product",
        "table_name": "catalog.schema.orders",
        "rule_type": "agg_dq",
        "rule": "row_count",
        "expectation": "count(*) > 0",
        "action_if_failed": "fail",
        "tag": "completeness",
        "description": "Table must have rows"
      }
    ]
    ```

**Structure:**

- The file is a bare list (no wrapping object).
- Every rule must include all five required fields: `product_id`, `table_name`,
  `rule_type`, `rule`, and `expectation`.
- Optional fields that are omitted get filled in with built-in defaults.

---

## Format Comparison

| Format         | Top-level key  | Multi-table | Defaults | `rule_type` specified where  |
|----------------|----------------|:-----------:|:--------:|------------------------------|
| Validations    | `validations`  | Yes         | Yes      | On each rule                 |
| Rules List     | `rules`        | No          | Yes      | On each rule (or as default) |
| Hierarchical   | `tables`       | Yes         | Yes      | Inferred from nesting        |
| Flat           | *(bare list)*  | Yes         | No       | On each rule (required)      |

The format is auto-detected based on the top-level structure:

1. Dict with `validations` key --> **validations**
2. Dict with `rules` key --> **rules list**
3. Dict with `tables` key --> **hierarchical**
4. Bare list --> **flat**

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
| `query_dq_delimiter`               |          | Delimiter for custom query_dq alias queries (default `$`)                                         |
| `enable_querydq_custom_output`     |          | Capture custom query output in a separate table                                                   |
| `priority`                         |          | `low`, `medium`, or `high`                                                                        |

---

## Full Example

Here is a complete example loading rules from YAML and running DQ checks:

```python
from pyspark.sql import DataFrame
from spark_expectations.rules import load_rules_from_yaml
from spark_expectations.core.expectations import SparkExpectations, WrappedDataFrameWriter
from spark_expectations.config.user_config import Constants as user_config
from spark_expectations.core import load_configurations

load_configurations(spark)

# Load rules from YAML
rules_df = load_rules_from_yaml("path/to/rules.yaml")

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
def process_orders():
    return spark.read.table("catalog.schema.raw_orders")

process_orders()
```
