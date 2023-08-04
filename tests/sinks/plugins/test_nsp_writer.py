import os
import pytest
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from spark_expectations.core import get_spark_session
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.sinks.plugins.nsp_writer import SparkExpectationsNspWritePluginImpl

spark = get_spark_session()


@pytest.fixture(name="_fixture_local_nsp_topic")
def fixture_setup_local_nsp_topic():
    current_dir = os.path.dirname(os.path.abspath(__file__))

    if os.getenv('UNIT_TESTING_ENV') != "spark_expectations_unit_testing_on_jenkins_bmx_server":

        # remove if docker conatiner is running
        os.system(f"sh {current_dir}/../../../spark_expectations/examples/docker_scripts/docker_nsp_stop_script.sh")

        # start docker container and create the topic
        os.system(f"sh {current_dir}/../../../spark_expectations/examples/docker_scripts/docker_nsp_start_script.sh")

        yield "docker container started"

        # remove docker container
        os.system(f"sh {current_dir}/../../../spark_expectations/examples/docker_scripts/docker_nsp_stop_script.sh")

    else:
        yield "A Kafka server has been launched within a Docker container for the purpose of conducting tests in " \
              "a Jenkins environment"


@pytest.fixture(name="_fixture_dataset")
def fixture_dataset():
    # Create a mock dataframe
    data = [(1, "John", 25)]
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    return spark.createDataFrame(data, schema)


def test_nsp_writer(_fixture_local_nsp_topic, _fixture_dataset):
    nsp_writer_handler = SparkExpectationsNspWritePluginImpl()

    write_args = {
        "stats_df": _fixture_dataset,
        "kafka_write_options": {
            "kafka.bootstrap.servers": "localhost:9092",
            "topic": "dq-sparkexpectations-stats",
            "failOnDataLoss": "true",
        },
        "enable_se_streaming": True,
    }

    nsp_writer_handler.writer(_write_args=write_args)

    expected_df = spark.read.format("kafka").option(
        "kafka.bootstrap.servers", "localhost:9092"
    ).option("subscribe", "dq-sparkexpectations-stats").option(
        "startingOffsets", "earliest"
    ).option(
        "endingOffsets", "latest"
    ).load().orderBy(col('timestamp').desc()).limit(1).selectExpr(
        "cast(value as string) as stats_records")

    assert expected_df.collect() == _fixture_dataset.selectExpr("cast(to_json(struct(*)) as string) AS stats_records") \
        .collect()


def test_nsp_writer_exception(_fixture_local_nsp_topic, _fixture_dataset):
    delta_writer_handler = SparkExpectationsNspWritePluginImpl()

    write_args = {
        "kafka_write_options": {
            "kafka.bootstrap.servers": "localhost:9092",
            "topic": "dq-sparkexpectations-stats",
            "failOnDataLoss": "true",
        }
    }

    with pytest.raises(SparkExpectationsMiscException, match=r"error occurred while saving data into NSP .*"):
        delta_writer_handler.writer(_write_args=write_args)
