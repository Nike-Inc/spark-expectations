"""Example: Loading DQ rules from YAML or JSON files.

This script demonstrates how to use ``spark_expectations.rules`` to load
data-quality rules from a YAML (or JSON) file instead of a Delta table.
The loaded rules are passed as a DataFrame to ``SparkExpectations``.
"""

import os
from typing import Dict, Union

from pyspark.sql import DataFrame

from spark_expectations import _log
from spark_expectations.config.user_config import Constants as user_config
from spark_expectations.core.expectations import (
    SparkExpectations,
    WrappedDataFrameWriter,
)
from spark_expectations.rules import load_rules_from_yaml, load_rules_from_json

from examples.scripts.base_setup import set_up_delta

RESOURCES_DIR = os.path.join(os.path.dirname(__file__), "..", "resources")

writer = WrappedDataFrameWriter().mode("append").format("delta")
spark = set_up_delta()

dic_job_info = {
    "job": "job_name",
    "Region": "NA",
    "env": "dev",
    "Snapshot": "2024-04-15",
    "data_object_name": "customer_order",
}
job_info = str(dic_job_info)

your_product = 'your_product'
# ── Load rules from YAML ────────────────────────────────────────────────
rules_df = load_rules_from_yaml(
    os.path.join(RESOURCES_DIR, "sample_rules.yaml"),
    spark,
    options={"dq_env": "DEV"},
)

# ── Alternatively, load rules from JSON ─────────────────────────────────
# rules_df = load_rules_from_json(
#     os.path.join(RESOURCES_DIR, "sample_rules.json"),
#     spark,
#     options={"dq_env": "DEV"},
# )

# ── Or auto-detect the format from the file extension ───────────────────
# from spark_expectations.rules import load_rules
# rules_df = load_rules(
#     os.path.join(RESOURCES_DIR, "sample_rules.yaml"),
#     spark,
#     options={"dq_env": "DEV"},
# )

se: SparkExpectations = SparkExpectations(
    product_id="your_product",
    rules_df=rules_df,
    stats_table="dq_spark_dev.dq_stats",
    stats_table_writer=writer,
    target_and_error_table_writer=writer,
    debugger=False,
    stats_streaming_options={
        user_config.se_enable_streaming: False,
        user_config.se_streaming_stats_topic_name: "dq-sparkexpectations-stats",
    },
)

user_conf: Dict[str, Union[str, int, bool, Dict[str, str]]] = {
    user_config.se_notifications_enable_email: False,
    user_config.se_notifications_enable_slack: False,
    user_config.se_notifications_on_start: False,
    user_config.se_notifications_on_completion: False,
    user_config.se_notifications_on_fail: False,
    user_config.se_notifications_on_error_drop_exceeds_threshold_breach: True,
    user_config.se_notifications_on_error_drop_threshold: 15,
    user_config.se_enable_query_dq_detailed_result: True,
    user_config.se_enable_agg_dq_detailed_result: True,
    user_config.se_enable_error_table: True,
    user_config.se_dq_rules_params: {
        "env": "dev",
        "table": "product",
        "data_object_name": "customer_order",
        "data_source": "customer_source",
        "data_layer": "Integrated",
    },
    user_config.se_job_metadata: job_info,
}


@se.with_expectations(
    target_table="dq_spark_dev.customer_order",
    write_to_table=True,
    user_conf=user_conf,
    target_table_view="order",
)
def build_new() -> DataFrame:
    _df_order: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(RESOURCES_DIR, "order.csv"))
    )
    _df_order.createOrReplaceTempView("order_source")
    _df_order.createOrReplaceTempView("order_target")

    _df_product: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(RESOURCES_DIR, "product.csv"))
    )
    _df_product.createOrReplaceTempView("product")

    _df_customer: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(RESOURCES_DIR, "customer_source.csv"))
    )
    _df_customer.createOrReplaceTempView("customer_source")
    _df_customer.createOrReplaceTempView("customer_target")

    return _df_order


if __name__ == "__main__":
    build_new()

    spark.sql("use dq_spark_dev")
    spark.sql("select * from dq_spark_dev.dq_stats").show(truncate=False)
    spark.sql("select * from dq_spark_dev.customer_order").show(truncate=False)

    _log.info("DQ run with YAML rules completed.")
