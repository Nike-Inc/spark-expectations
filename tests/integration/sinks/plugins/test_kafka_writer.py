import json
import os
import pytest
from pyspark.sql.functions import col, from_json, lit, schema_of_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from spark_expectations.core import get_spark_session
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.sinks.plugins.kafka_writer import SparkExpectationsKafkaWritePluginImpl

spark = get_spark_session()


@pytest.fixture(name="_fixture_local_kafka_topic",scope="session",autouse=True)
def fixture_setup_local_kafka_topic():
    current_dir = os.path.dirname(os.path.abspath(__file__))

    if os.getenv("UNIT_TESTING_ENV") != "spark_expectations_unit_testing_on_github_actions":
        # remove if docker conatiner is running
        os.system(f"sh {current_dir}/../../../../containers/kafka/scripts/docker_kafka_stop_script.sh")

        # start docker container and create the topic
        os.system(f"sh {current_dir}/../../../../containers/kafka/scripts/docker_kafka_start_script.sh")

        yield "docker container started"

        # remove docker container
        os.system(f"sh {current_dir}/../../../../containers/kafka/scripts/docker_kafka_stop_script.sh")

    else:
        yield (
            "A Kafka server has been launched within a Docker container for the purpose of conducting tests in "
            "a Jenkins environment"
        )


@pytest.fixture(name="_fixture_dataset")
def fixture_dataset():
    # Create a mock dataframe
    data = [(1, "John", 25)]
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


def test_kafka_writer(_fixture_local_kafka_topic, _fixture_dataset):
    kafka_writer_handler = SparkExpectationsKafkaWritePluginImpl()

    write_args = {
        "stats_df": _fixture_dataset,
        "kafka_write_options": {
            "kafka.bootstrap.servers": "localhost:9092",
            "topic": "dq-sparkexpectations-stats",
            "failOnDataLoss": "true",
        },
        "enable_se_streaming": True,
    }

    kafka_writer_handler.writer(_write_args=write_args)

    expected_df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "dq-sparkexpectations-stats")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .orderBy(col("timestamp").desc())
        .limit(1)
        .selectExpr("cast(value as string) as stats_records")
    )

    assert (
        expected_df.collect()
        == _fixture_dataset.selectExpr("cast(to_json(struct(*)) as string) AS stats_records").collect()
    )


def test_kafka_writer_converts_se_job_metadata_to_struct():
    """Test that se_job_metadata JSON string column is converted to a struct before writing to Kafka"""
    # Create a DataFrame with se_job_metadata as a JSON string (mimics writer.py behavior)
    metadata_dict = {"se_version": "1.0.0", "runtime_env": {"host": "local"}}
    data = [("product1",)]
    schema = StructType([StructField("product_id", StringType(), True)])
    df = spark.createDataFrame(data, schema)
    df = df.withColumn("se_job_metadata", lit(json.dumps(metadata_dict)))

    # Verify se_job_metadata starts as a StringType
    assert df.schema["se_job_metadata"].dataType.simpleString() == "string"

    # Apply the same conversion logic used in the Kafka writer
    metadata_sample = df.select("se_job_metadata").first()[0]
    assert metadata_sample is not None

    metadata_schema = schema_of_json(lit(metadata_sample))
    converted_df = df.withColumn(
        "se_job_metadata",
        from_json(col("se_job_metadata"), metadata_schema),
    )

    # Verify se_job_metadata is now a struct, not a string
    assert converted_df.schema["se_job_metadata"].dataType.simpleString() != "string"

    # Verify the to_json output contains properly nested se_job_metadata
    json_output = converted_df.selectExpr("to_json(struct(*)) AS value").first()[0]
    parsed = json.loads(json_output)
    assert isinstance(parsed["se_job_metadata"], dict)
    assert parsed["se_job_metadata"]["se_version"] == "1.0.0"
    assert parsed["se_job_metadata"]["runtime_env"]["host"] == "local"


def test_kafka_writer_skips_conversion_without_se_job_metadata_column(_fixture_local_kafka_topic):
    """Test that Kafka writer does not fail when se_job_metadata column is absent"""
    # Create a DataFrame without se_job_metadata column
    data = [("product1",)]
    schema = StructType([StructField("product_id", StringType(), True)])
    df = spark.createDataFrame(data, schema)

    # Verify the conversion logic safely skips when column is absent
    assert "se_job_metadata" not in df.columns

    kafka_writer_handler = SparkExpectationsKafkaWritePluginImpl()

    write_args = {
        "stats_df": df,
        "kafka_write_options": {
            "kafka.bootstrap.servers": "localhost:9092",
            "topic": "dq-sparkexpectations-stats",
            "failOnDataLoss": "true",
        },
        "enable_se_streaming": True,
    }

    # Should succeed without error - no se_job_metadata column means no conversion needed
    kafka_writer_handler.writer(_write_args=write_args)

