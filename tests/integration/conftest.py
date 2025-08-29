import os
import pytest

from spark_expectations.core import get_spark_session
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.utils.actions import SparkExpectationsActions

spark = get_spark_session()

@pytest.fixture(name="_fixture_local_kafka_topic",scope="session",autouse=True)
def fixture_setup_local_kafka_topic():
    current_dir = os.path.dirname(os.path.abspath(__file__))

    if os.getenv("UNIT_TESTING_ENV") != "spark_expectations_unit_testing_on_github_actions":
        # remove if docker container is running
        os.system(f"sh {current_dir}/../../../../spark_expectations/examples/docker_scripts/docker_kafka_stop_script.sh")

        # start docker container and create the topic
        os.system(f"sh {current_dir}/../../../../spark_expectations/examples/docker_scripts/docker_kafka_start_script.sh")

        yield "docker container started"

        # remove docker container
        os.system(f"sh {current_dir}/../../../../spark_expectations/examples/docker_scripts/docker_kafka_stop_script.sh")

    else:
        yield (
            "A Kafka server has been launched within a Docker container for the purpose of conducting tests in"
            " a Jenkins environment"
        )

@pytest.fixture(name="_fixture_df")
def fixture_df():
    # Create a sample input dataframe
    _fixture_df = spark.createDataFrame(
        [
            {"row_id": 0, "col1": 1, "col2": "a"},
            {"row_id": 1, "col1": 2, "col2": "b"},
            {"row_id": 2, "col1": 3, "col2": "c"},
        ]
    )

    return _fixture_df