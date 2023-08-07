import os
from pyspark.sql.session import SparkSession
from delta import configure_spark_with_delta_pip

current_dir = os.path.dirname(os.path.abspath(__file__))


def get_spark_session() -> SparkSession:
    builder = SparkSession.builder

    if (
        os.environ.get("UNIT_TESTING_ENV")
        == "spark_expectations_unit_testing_on_github_actions"
    ) or (os.environ.get("SPARKEXPECTATIONS_ENV") == "local"):
        builder = (
            builder.config(
                "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
            )
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
            # .config("spark.databricks.delta.checkLatestSchemaOnRead", "false")
        )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark
