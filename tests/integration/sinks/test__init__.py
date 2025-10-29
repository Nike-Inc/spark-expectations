import os
import pytest
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from spark_expectations.core import get_spark_session
from spark_expectations.sinks import _sink_hook


spark = get_spark_session()


@pytest.fixture(name="_fixture_create_database")
def fixture_create_database():
    # drop and create dq_spark if exists
    os.system("rm -rf /tmp/hive/warehouse/dq_spark.db")
    spark.sql("create database if not exists dq_spark")
    spark.sql("use dq_spark")

    yield "dq_spark"

    # drop dq_spark if exists
    spark.sql("drop table if exists test_dq_stats_table")
    os.system("rm -rf /tmp/hive/warehouse/dq_spark.db/test_dq_stats_table")


@pytest.fixture(name="_fixture_local_kafka_topic",scope="session",autouse=True)
def fixture_setup_local_kafka_topic():
    current_dir = os.path.dirname(os.path.abspath(__file__))

    if os.getenv("UNIT_TESTING_ENV") != "spark_expectations_unit_testing_on_github_actions":
        # remove if docker container is running
        os.system(f"sh {current_dir}/../../../containers/kafka/scripts/docker_kafka_stop_script.sh")

        # start docker container and create the topic
        os.system(f"sh {current_dir}/../../../containers/kafka/scripts/docker_kafka_start_script.sh")

        yield "docker container started"

        # remove docker container
        os.system(f"sh {current_dir}/../../../containers/kafka/scripts/docker_kafka_stop_script.sh")

    else:
        yield (
            "A Kafka server has been launched within a Docker container for the purpose of conducting tests "
            "in a Jenkins environment"
        )


@pytest.fixture(name="_fixture_dataset")
def fixture_dataset():
    # Create a mock dataframe
    data = [("product1", "dq_spark.test_table", 100, 25, 75)]
    schema = StructType(
        [
            StructField("product_id", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("input_count", IntegerType(), True),
            StructField("output_count", IntegerType(), True),
            StructField("error_count", IntegerType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


def test_sink_hook_write(_fixture_create_database, _fixture_local_kafka_topic, _fixture_dataset):
    write_args = {
        "stats_df": _fixture_dataset,
        "kafka_write_options": {
            "kafka.bootstrap.servers": "localhost:9092",
            "topic": "dq-sparkexpectations-stats-local",
            "failOnDataLoss": "true",
        },
        "enable_se_streaming": True,
    }

    _sink_hook.writer(_write_args=write_args)

    # expected_delta_df = spark.table("dq_spark.test_table")

    expected_kafka_df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "dq-sparkexpectations-stats-local")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .orderBy(col("timestamp").desc())
        .limit(1)
        .selectExpr("cast(value as string) as stats_records")
    )

    # assert expected_delta_df.collect() == _fixture_dataset.collect()
    assert (
        expected_kafka_df.collect()
        == _fixture_dataset.selectExpr("cast(to_json(struct(*)) as string) AS stats_records").collect()
    )
