import json
import os
import yaml
from pyspark.sql.session import SparkSession

current_dir = os.path.dirname(os.path.abspath(__file__))


def load_configurations(spark: SparkSession) -> None:
    """
    Load Spark configuration settings from a YAML file and apply them to the provided SparkSession.

    This function:
    - Reads the configuration file located at `../config/spark-default-config.yaml`.
    - Separates streaming (`se.streaming.*`) and notification (`spark.expectations.*`) configurations into dictionaries.
    - Sets other configuration values directly in the Spark session.
    - Stores streaming and notification configs as JSON strings in Spark session configs.
    - Raises RuntimeError for file not found, YAML parsing errors, permission issues, or other exceptions.

    Args:
        spark (SparkSession): The SparkSession to apply configurations to.

    Raises:
        RuntimeError: If the configuration file is not found, cannot be parsed, or other errors occur.
    """
    try:
        with open(f"{current_dir}/../config/spark-default-config.yaml", "r", encoding="utf-8") as config_file:
            config = yaml.safe_load(config_file)
        if config is None:
            config = {}
        elif not isinstance(config, dict):
            raise yaml.YAMLError("Spark config YAML file is not valid.")
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

    except FileNotFoundError as e:
        raise RuntimeError(f"Spark config YAML file not found: {e}") from e
    except yaml.YAMLError as e:
        raise RuntimeError(f"Error parsing Spark config YAML configuration file: {e}") from e
    except Exception as e:
        raise RuntimeError(f"An unexpected error occurred while loading spark configurations: {e}") from e


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

        sparksession = builder.getOrCreate()
    else:
        sparksession = SparkSession.getActiveSession()
    load_configurations(sparksession)
    return sparksession
