import ast
import os
from typing import Any, Dict, Union

import yaml
from pyspark.sql.session import SparkSession

current_dir = os.path.dirname(os.path.abspath(__file__))


def load_configurations(spark: SparkSession) -> tuple[Dict[str, Union[str, int, bool, Dict[str, str]]], Dict[str, Union[str, int, bool, Dict[str, str]]]]:
    """
    Load Spark configuration settings from a YAML file and apply them to the provided SparkSession.

    This function:
    - Reads the configuration file located at `../config/spark-expectations-default-config.yaml`.
    - Separates streaming (`se.streaming.*`) and notification (`spark.expectations.*`) configurations into dictionaries.
    - Sets other configuration values directly in the Spark session.
    - Stores streaming and notification configs as JSON strings in Spark session configs.
    - In serverless environments, skips configuration setting to avoid CONFIG_NOT_AVAILABLE errors.
    - Raises RuntimeError for file not found, YAML parsing errors, permission issues, or other exceptions.

    Args:
        spark (SparkSession): The SparkSession to apply configurations to.

    Raises:
        RuntimeError: If the configuration file is not found, cannot be parsed, or other errors occur.
    """
    try:
        with open(f"{current_dir}/../config/spark-expectations-default-config.yaml", "r", encoding="utf-8") as cfg_file:
            config = yaml.safe_load(cfg_file)
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
        
        return streaming_config, notification_config      
        

    except FileNotFoundError as e:
        raise RuntimeError(f"Spark config YAML file not found: {e}") from e
    except yaml.YAMLError as e:
        raise RuntimeError(f"Error parsing Spark config YAML configuration file: {e}") from e
    except Exception as e:
        raise RuntimeError(f"An unexpected error occurred while loading spark configurations: {e}") from e


def get_config_dict(
    spark: SparkSession, user_conf: Dict[str, Union[str, int, bool, Dict[str, str]]] = None
) -> tuple[Dict[str, Union[str, int, bool, Dict[str, str]]], Dict[str, Union[str, int, bool, Dict[str, str]]],]:
    """
    Retrieve both notification and streaming config dictionaries from the user configuration or Spark session or default configuration.

    Args:
        spark (SparkSession): The Spark session to retrieve the configuration from.
        user_conf ([Dict[str, Any]]): User configuration to merge with default configuration.

    Returns:
        tuple: A tuple containing (notification_dict, streaming_dict).

    Raises:
        RuntimeError: If there are errors parsing or retrieving the configuration.
    """

    def _build_config_dict(
        default_dict: Dict[str, Union[str, int, bool, Dict[str, str]]],
        user_conf: Dict[str, Union[str, int, bool, Dict[str, str]]] = None,
    ) -> Dict[str, Union[str, int, bool, Dict[str, str]]]:
        """Helper function to build configuration dictionary with type inference."""
        is_serverless_mode = user_conf.get("spark.expectations.is.serverless", False) if user_conf else False
        
        if user_conf:
            if is_serverless_mode:
                config_dict = {
                    key: infer_safe_cast(user_conf.get(key, str(value)))
                    for key, value in default_dict.items()
                }
            else:
                config_dict = {
                    key: infer_safe_cast(user_conf.get(key, spark.conf.get(key, str(value))))
                    for key, value in default_dict.items()
                }
        else:
            config_dict = {key: infer_safe_cast(spark.conf.get(key, str(value))) for key, value in default_dict.items()}
        return config_dict

    try:
        # Get configurations directly from load_configurations
        default_streaming_dict, default_notification_dict = load_configurations(spark)

        # Build both dictionaries using the helper function
        notification_dict = _build_config_dict(default_notification_dict, user_conf)
        streaming_dict = _build_config_dict(default_streaming_dict, user_conf)

        return notification_dict, streaming_dict

    except Exception as e:
        raise RuntimeError(f"Error retrieving configuration: {e}") from e


def get_spark_session() -> SparkSession:
    if (os.environ.get("UNIT_TESTING_ENV") == "spark_expectations_unit_testing_on_github_actions") or (
        os.environ.get("SPARKEXPECTATIONS_ENV") == "local"
    ):
        builder = (
            SparkSession.builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.warehouse.dir", "/tmp/hive/warehouse")
            .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby")
            .config("spark.jars.ivy", "/tmp/ivy2")
            .config(  # below jars are used only in the local env, not coupled with databricks or EMR
                "spark.jars",
                f"{current_dir}/../../jars/spark-sql-kafka-0-10_2.13-4.0.0.jar,"
                f"{current_dir}/../../jars/kafka-clients-3.7.0.jar,"
                f"{current_dir}/../../jars/commons-pool2-2.12.0.jar,"
                f"{current_dir}/../../jars/spark-token-provider-kafka-0-10_2.13-4.0.0.jar",
            )
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.dynamicAllocation.enabled", "false")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
        )

        sparksession = builder.getOrCreate()
    else:
        sparksession = SparkSession.getActiveSession()
    load_configurations(sparksession)  # Load default configs, return values not needed here
    return sparksession


def infer_safe_cast(input_value: Any) -> Union[int, float, bool, dict, str, None]:  # pylint: disable=R0911
    """
    Infers and safely casts the input value to int, float, bool, dict, str, or None.

    Args:
        input_value: The value to analyze (can be any type)

    Returns:
        Union[int, float, bool, dict, str, None]: The inferred and converted value
    """
    if input_value is None:
        return None

    # Return early for already acceptable types
    if isinstance(input_value, (int, float, bool, dict, list)):
        return input_value

    # Convert to string and clean
    cleaned_input = str(input_value).strip()

    # Handle string representations of None
    if cleaned_input.lower() in {"none", "null"}:
        return None

    # Handle booleans (case-insensitive)
    if cleaned_input.lower() in {"true", "false"}:
        return cleaned_input.lower() == "true"

    # Try integer
    try:
        return int(cleaned_input)
    except ValueError:
        pass

    # Try float
    try:
        return float(cleaned_input)
    except ValueError:
        pass

    # Try dictionary
    try:
        parsed = ast.literal_eval(cleaned_input)
        if isinstance(parsed, dict):
            return parsed
    except (ValueError, SyntaxError):
        pass

    # Fallback to original string (not lowercased)
    return str(input_value).strip()