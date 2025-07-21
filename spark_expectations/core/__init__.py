import json
import os
import yaml
from pyspark.sql.session import SparkSession

current_dir = os.path.dirname(os.path.abspath(__file__))


def load_configurations(spark: SparkSession) -> None:
    with open(f"{current_dir}/../config/spark-default-config.yaml", "r", encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)
    streaming_config = {}
    notification_config = {}
    for key, value in config.items():
        if key.startswith("se.streaming."):
            streaming_config[key] = value
        elif key.startswith("spark.expectations."):
            notification_config[key] = value
        else:
            spark.conf.set(key, str(value))
    spark.conf.set("default_streaming_dict", json.dumps(streaming_config))
    spark.conf.set("default_notification_dict", json.dumps(notification_config))


def get_spark_session() -> SparkSession:
    if (os.environ.get("UNIT_TESTING_ENV") == "spark_expectations_unit_testing_on_github_actions") or (
        os.environ.get("SPARKEXPECTATIONS_ENV") == "local"
    ):
        builder = (
            SparkSession.builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.warehouse.dir", "/tmp/hive/warehouse")
            .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby")
            .config("spark.jars.ivy", "/tmp/ivy2")
            .config(  # below jars are used only in the local env, not coupled with databricks or EMR
                "spark.jars",
                f"{current_dir}/../../jars/spark-sql-kafka-0-10_2.12-3.0.0.jar,"
                f"{current_dir}/../../jars/kafka-clients-3.0.0.jar,"
                f"{current_dir}/../../jars/commons-pool2-2.8.0.jar,"
                f"{current_dir}/../../jars/spark-token-provider-kafka-0-10_2.12-3.0.0.jar",
            )
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.dynamicAllocation.enabled", "false")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
        )
        return builder.getOrCreate()

    spark = SparkSession.getActiveSession()
    load_configurations(spark)
    return spark
