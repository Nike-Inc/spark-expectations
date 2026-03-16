# Rule Validation

Spark Expectations validates all rules before executing them. Invalid rules are **logged and skipped** rather than causing the entire job to fail. This non-blocking behavior ensures that a single malformed rule does not prevent the rest of your data quality checks from running.

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

## Non-Blocking Behavior

When a rule fails validation:

1. A warning is logged with details about the validation failure.
2. The invalid rule is **removed** from the set of rules to execute.
3. All remaining valid rules continue to run normally.

This means your job will not crash because of a typo in one expectation expression. However, you should monitor your logs for validation warnings to catch and fix issues.

!!! tip
    Check your Spark driver logs for messages containing `"rule validation"` to find any skipped rules after a run.

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

Using `drop` with `agg_dq` or `query_dq` will trigger a validation warning and the rule will be skipped.
