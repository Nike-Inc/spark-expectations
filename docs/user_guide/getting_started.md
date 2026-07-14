# Getting Started with Spark-Expectations

This guide will help you set up your environment, install the library, and understand the basic requirements for using Spark-Expectations in your data workflows.

## Prerequisites

### Python
- **Supported versions:** 3.9, 3.10, 3.11, 3.12, 3.13 (recommended: latest 3.12.x or 3.13.x)
### Java
- **Supported versions:** 8, 11, 17 

### Apache Spark
- **Supported versions:** 3.0 through 4.0 (including Spark Connect for Spark 3.4+)
- **Scala:** 2.12 or 2.13


## Installation

You can install Spark-Expectations directly from [PyPI](https://pypi.org/project/spark-expectations/):

```sh
pip install -U spark-expectations
```

Or add it to your `requirements.txt` or dependency manager (e.g., poetry, hatch, uv).

### PySpark Connect

Spark Expectations supports [PySpark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) (available in Spark 3.4+). If you are using Databricks Connect v2 or a standalone Spark Connect server, Spark Expectations automatically detects and uses the Connect DataFrame and SparkSession types. No additional configuration is needed.

---

Ready to get started? Head to the [Quickstart](quickstart.md) for setting up a hello world SparkExpectations job!