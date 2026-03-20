# ANSI Mode Testing Guide

This guide explains how to test Spark Expectations ANSI mode compatibility both locally and on Databricks Serverless.

## Background

Databricks Serverless Compute enables `spark.sql.ansi.enabled=true` by default. Under ANSI mode, operations like `CAST('abc' AS INT)` raise `CAST_INVALID_INPUT` instead of returning `NULL`. Spark Expectations uses `try_cast` internally to handle this safely.

## Running Tests Locally

### Prerequisites

- Python 3.10, 3.11, or 3.12
- Java 17+ (for Spark)
- [Hatch](https://hatch.pypa.io/) build tool

### Run All ANSI Tests

```bash
# Run all ANSI-related tests across all test modules
hatch run dev.py3.12:pytest -k "ansi" -v

# Or use the make target
make test-ansi
```

### Run Tests by Module

```bash
# Context ANSI detection tests
hatch run dev.py3.12:pytest tests/integration/core/test_context.py -k "ansi" -v

# Report generation with ANSI mode
hatch run dev.py3.12:pytest tests/integration/sinks/utils/test_report.py -k "ansi" -v

# DQ rule execution (row, agg, query) with ANSI mode
hatch run dev.py3.12:pytest tests/integration/utils/test_actions.py -k "ansi" -v
```

### Run the Full Test Suite

```bash
hatch run dev.py3.12:pytest -v
```

## Test Coverage

The following ANSI-specific tests are included:

### Context Tests (`tests/integration/core/test_context.py`)

| Test | Purpose |
|------|---------|
| `test_context_detects_ansi_enabled` | Verifies `get_ansi_enabled` returns `True` when ANSI is on |
| `test_context_detects_ansi_disabled` | Verifies `get_ansi_enabled` returns `False` when ANSI is off |
| `test_context_ansi_enabled_case_insensitive` | Handles `"True"`, `"TRUE"`, etc. |
| `test_context_ansi_property_is_bool` | Ensures property returns `bool`, not `str` |
| `test_context_ansi_toggle_independent_per_instance` | Each context captures ANSI at construction |

### Report Tests (`tests/integration/sinks/utils/test_report.py`)

| Test | Purpose |
|------|---------|
| `test_report_with_empty_string_row_counts_ansi_mode` | Empty strings in row-count columns don't raise `CAST_INVALID_INPUT` |
| `test_report_string_typed_columns_ansi_mode` | Full report produces valid `success_percentage` (DoubleType, 0-100) |
| `test_report_with_malformed_numeric_strings_ansi_mode` | Non-numeric strings like `"N/A"` handled gracefully |
| `test_report_with_null_row_counts_ansi_mode` | NULL row-count columns produce valid output |
| `test_report_failed_records_calculation_ansi_mode` | `failed_records` calculated correctly with ANSI |

### Actions Tests (`tests/integration/utils/test_actions.py`)

| Test | Purpose |
|------|---------|
| `test_agg_query_dq_detailed_result_ansi_mode` | Agg DQ produces identical results with ANSI on |
| `test_query_dq_detailed_result_ansi_mode` | Query DQ produces correct results with ANSI on |
| `test_row_dq_rules_ansi_mode` | Row-level DQ rules work identically with ANSI |
| `test_agg_dq_range_rule_ansi_mode` | Range-based agg rules work with ANSI |
| `test_agg_dq_with_try_cast_expression_ansi_mode` | Custom `try_cast` expressions in expectations work under ANSI |

## Testing on Databricks

### Using the Databricks Notebook

Import the `spark_expectations_ansi_serverless.ipynb` notebook from `examples/notebooks/` into your Databricks workspace.

1. **Attach to Serverless Compute** (ANSI mode is enabled by default)
2. **Run all cells** -- the notebook:
    - Verifies ANSI mode detection
    - Tests `try_cast` vs `CAST` behavior
    - Runs sample DQ rules with ANSI active
    - Validates report generation with edge-case data

### Manual Verification on Serverless

```python
# Confirm ANSI mode is active
spark.conf.get("spark.sql.ansi.enabled")
# Should return "true" on Serverless

# Verify try_cast works
spark.sql("SELECT try_cast('' AS INT)").show()
# Should return NULL, not an error

# Verify CAST would fail
try:
    spark.sql("SELECT CAST('' AS INT)").show()
except Exception as e:
    print(f"Expected error: {e}")
```

### Manual Verification on Standard Compute

```python
# Temporarily enable ANSI mode
spark.conf.set("spark.sql.ansi.enabled", "true")

# Run your SE pipeline as normal
# The framework should handle ANSI automatically

# Reset after testing
spark.conf.set("spark.sql.ansi.enabled", "false")
```

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `CAST_INVALID_INPUT` errors | Custom expectations using `CAST()` instead of `try_cast()` | Replace `CAST(col AS type)` with `try_cast(col AS type)` in your rules |
| `TypeError: int() argument must be a string...not NoneType` | Aggregation returning NULL on empty data | Already fixed in `actions.py` -- ensure you're on the latest version |
| `success_percentage` is always NULL | All row-count columns are empty strings | Expected behavior -- `try_cast` returns NULL for empty strings |
