# Rule Validation

Spark Expectations validates rules before the DQ pipeline runs. Invalid rules produce **warning logs** instead of failing the validation step. They are **not skipped**, however — malformed rules can still cause runtime failures during execution.

## What Gets Validated

Each rule is validated based on its `rule_type`:

| Rule Type | Validation Checks |
|---|---|
| `row_dq` | Expression must be a valid SQL predicate. Aggregate functions (`sum`, `avg`, `count`, etc.) are **not** allowed at the top level (they are allowed inside subqueries). |
| `agg_dq` | Expression must contain at least one aggregate function (`sum`, `avg`, `min`, `max`, `count`, `stddev`, etc.). |
| `query_dq` | Expression must be a valid SQL query. Composite queries with custom delimiters (e.g., `@source_f1@SELECT ...@target_f1@SELECT ...`) are parsed and each sub-query is validated. |

Additionally, for all rule types:

- **`action_if_failed`** must be one of `"drop"`, `"ignore"`, or `"fail"` for `row_dq`, or one of `"ignore"` or `"fail"` for `agg_dq` and `query_dq`.
- **`rule_type`** must be one of `"row_dq"`, `"agg_dq"`, or `"query_dq"`.

## Non-Blocking Validation

When a rule fails validation:

1. A warning is logged with details about the validation failure.
2. The validation step continues without raising an exception.
3. The rule remains in the execution set and is still applied during the DQ pipeline.

Monitor your driver logs for validation warnings and fix invalid rules in your rules table to avoid runtime failures.

!!! tip
    Check your Spark driver logs for messages containing `"Invalid rule detected"` or `"Some rules failed validation"` after a run.

## Validation Examples

### Valid `row_dq` Expressions

```sql
age IS NOT NULL
age > 18
salary BETWEEN 30000 AND 200000
lower(trim(status)) IN ('active', 'pending')
col1 > (SELECT avg(col1) FROM ref_table)
```

### Invalid `row_dq` Expressions

```sql
sum(age) > 100          -- Aggregate at top level; use agg_dq instead
AVG(salary) > 50000     -- Aggregate at top level; use agg_dq instead
```

### Valid `agg_dq` Expressions

```sql
count(*) > 0
sum(amount) > 10000
avg(score) BETWEEN 60 AND 100
count(DISTINCT category) <= 10
```

### Invalid `agg_dq` Expressions

```sql
age > 18                -- No aggregate function; use row_dq instead
col1 IS NOT NULL        -- No aggregate function; use row_dq instead
```

## `action_if_failed` Validation

| Rule Type | Valid Values |
|---|---|
| `row_dq` | `drop`, `ignore`, `fail` |
| `agg_dq` | `ignore`, `fail` |
| `query_dq` | `ignore`, `fail` |

Using `drop` with `agg_dq` or `query_dq` will trigger a validation warning. The rule is still executed unless you fix or deactivate it in your rules table.
