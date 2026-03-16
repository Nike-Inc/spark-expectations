# FAQ and Troubleshooting

## Common Questions

### My job fails with `SparkExpectOrFailException`

This means a rule with `action_if_failed = "fail"` has failed. Check the stats table for details on which rule triggered the failure:

```sql
SELECT source_agg_dq_results, final_agg_dq_results,
       source_query_dq_results, final_query_dq_results,
       row_dq_res_summary
FROM your_stats_table
WHERE meta_dq_run_id = 'your_run_id'
```

To make the job continue instead of failing, change the rule's `action_if_failed` to `"ignore"` or `"drop"`.

### How do I debug which rules failed?

1. **Stats table**: Check `row_dq_res_summary`, `source_agg_dq_results`, and `final_agg_dq_results` columns.
2. **Detailed stats table**: Enable `se_enable_agg_dq_detailed_result: True` in your user_conf to get per-rule results in the `<stats_table>_detailed` table.
3. **Error table**: Query `<target_table>_error` and inspect the `meta_dq_rule_fail_records` column to see which rules each row failed.
4. **Debugger mode**: Pass `debugger=True` to the `SparkExpectations` constructor to print intermediate DataFrames to the driver logs.

### Notifications are not being sent

1. **Check the enable flags**: Ensure `se_notifications_enable_email`, `se_notifications_enable_slack`, etc. are set to `True`.
2. **Check the trigger flags**: By default, `on_start` is `False`. Set `se_notifications_on_start: True`, `se_notifications_on_completion: True`, or `se_notifications_on_fail: True` as needed.
3. **Check connectivity**: For email, verify SMTP host/port. For Slack/Teams/Zoom, verify the webhook URL is reachable.
4. **Serverless**: Email notifications may not work in Databricks Serverless due to network restrictions. Use webhook-based notifications (Slack, Teams) instead.

### `SparkExpectationsDataframeNotReturnedException`

The decorated function must return a DataFrame. Make sure your function has a `return` statement that returns a Spark DataFrame.

### Rules from my YAML/JSON file are not loading

1. **Check `dq_env`**: If your file uses `dq_env`, you must pass `options={"dq_env": "DEV"}` (or your environment name).
2. **Check required fields**: Each rule must have at least `rule`, `expectation`, and `rule_type`.
3. **Check `rule_type`**: Must be one of `row_dq`, `agg_dq`, or `query_dq`.
4. **Check file format**: Use `load_rules_from_yaml()` for `.yaml`/`.yml` files and `load_rules_from_json()` for `.json` files, or `load_rules()` for auto-detection.

### Kafka streaming fails with connection timeout

1. Verify the Kafka broker URL and port are correct and reachable.
2. If using Cerberus or Databricks secrets, verify the secret keys resolve to valid values.
3. To disable Kafka streaming entirely, set `user_config.se_enable_streaming: False` in your `stats_streaming_options`.

### How do I disable the error table?

Set `se_enable_error_table: False` in your `user_conf`. Note that this means failed rows will be silently dropped with no record.

### How do I use dynamic parameters in rules?

Use `se_dq_rules_params` to pass a dictionary of parameters that get substituted into rule expressions:

```python
user_conf = {
    user_config.se_dq_rules_params: {
        "env": "prod",
        "table": "customer_order",
    },
}
```

In your rules, reference them with curly braces: `(select count(*) from dq_spark_{env}.{table}) > 0`.

### What tables does Spark Expectations create?

| Table | Auto-created | Description |
|---|---|---|
| Target table | Written if `write_to_table=True` | Clean data after DQ |
| `<target>_error` | Yes (unless disabled) | Rows that failed DQ rules |
| Stats table | Yes | Run-level metrics and results |
| `<stats>_detailed` | Only if `se_enable_agg_dq_detailed_result=True` | Per-rule execution results |
| `<stats>_querydq_output` | Only if `se_enable_query_dq_detailed_result=True` | Query DQ sub-query results |

## Getting Help

- **GitHub Issues**: [Nike-Inc/spark-expectations/issues](https://github.com/Nike-Inc/spark-expectations/issues)
- **Releases**: [GitHub Releases](https://github.com/Nike-Inc/spark-expectations/releases)
