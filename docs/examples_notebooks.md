# Jupyter Notebook Examples

Spark Expectations ships with ready-to-run Jupyter notebooks in the [`examples/notebooks/`](https://github.com/Nike-Inc/spark-expectations/tree/main/examples/notebooks) directory. These notebooks are the fastest way to explore the framework interactively.

## Running Locally with Docker

The repository provides a Docker Compose setup that launches Jupyter Lab alongside Kafka and Mailpit (a local SMTP test server):

```bash
# Generate self-signed certificates for the mail server
make generate-mailserver-certs

# Start all services (Jupyter + Kafka + Mailpit)
make local-se-server-start ARGS="--build"
```

Once running:

- **Jupyter Lab**: [http://localhost:8888](http://localhost:8888)
- **Mailpit UI** (view sent emails): [http://localhost:8025](http://localhost:8025)
- **Kafka**: `localhost:9092`

## Available Notebooks

| Notebook | Description |
|---|---|
| [`spark_expectation_basic_jlab.ipynb`](https://github.com/Nike-Inc/spark-expectations/blob/main/examples/notebooks/spark_expectation_basic_jlab.ipynb) | Basic DQ setup with Delta Lake. Demonstrates row, aggregate, and query DQ rules with a local SparkSession. |
| [`spark_expectation_basic_mail.ipynb`](https://github.com/Nike-Inc/spark-expectations/blob/main/examples/notebooks/spark_expectation_basic_mail.ipynb) | Email notifications. Sends DQ alerts to the local Mailpit SMTP server. |
| [`spark_expectation_basic_mail_templates.ipynb`](https://github.com/Nike-Inc/spark-expectations/blob/main/examples/notebooks/spark_expectation_basic_mail_templates.ipynb) | Jinja-templated email notifications. Demonstrates custom and default HTML email templates. |
| [`spark_expectations_basic_slack_notification.ipynb`](https://github.com/Nike-Inc/spark-expectations/blob/main/examples/notebooks/spark_expectations_basic_slack_notification.ipynb) | Slack notifications via incoming webhooks. |
| [`spark_expectation_basic_pagerduty_notification.ipynb`](https://github.com/Nike-Inc/spark-expectations/blob/main/examples/notebooks/spark_expectation_basic_pagerduty_notification.ipynb) | PagerDuty notifications via Events API v2. |
| [`spark_expectation_notifications_integration.ipynb`](https://github.com/Nike-Inc/spark-expectations/blob/main/examples/notebooks/spark_expectation_notifications_integration.ipynb) | Multi-channel notification integration (email + Slack + more). |
| [`spark_expectations_aggregation_rules.ipynb`](https://github.com/Nike-Inc/spark-expectations/blob/main/examples/notebooks/spark_expectations_aggregation_rules.ipynb) | Aggregate and query DQ rules with detailed stats tables. |
| [`spark_expectation_basic_dbx.ipynb`](https://github.com/Nike-Inc/spark-expectations/blob/main/examples/notebooks/spark_expectation_basic_dbx.ipynb) | Databricks-specific setup. Designed for import into Databricks Repos. |
| [`spark_expectation_streaming_dbx.ipynb`](https://github.com/Nike-Inc/spark-expectations/blob/main/examples/notebooks/spark_expectation_streaming_dbx.ipynb) | Streaming DQ with `WrappedDataFrameStreamWriter` on Databricks. |

## Sample Scripts

In addition to notebooks, runnable Python scripts are available in [`examples/scripts/`](https://github.com/Nike-Inc/spark-expectations/tree/main/examples/scripts):

| Script | Description |
|---|---|
| `sample_dq_delta.py` | Delta Lake batch DQ with order/product/customer data |
| `sample_dq_bigquery.py` | BigQuery batch DQ |
| `sample_dq_iceberg.py` | Iceberg batch DQ |
| `sample_dq_delta_streaming.py` | Streaming DQ with `WrappedDataFrameStreamWriter` |
| `sample_dq_yaml_json.py` | Loading rules from YAML/JSON files via `load_rules_from_yaml` |
| `base_setup.py` | Shared SparkSession setup for Delta, Iceberg, and BigQuery |
