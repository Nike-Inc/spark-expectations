from spark_expectations.core import get_spark_session
from spark_expectations.sinks.plugins.base_writer import SparkExpectationsSinkWriter


spark = get_spark_session()


def test_base_writer():
    # Create an instance of the class that implements the writer method
    writer_object = SparkExpectationsSinkWriter()

    # Prepare test data
    write_args = {
        "key1": "value1",
        "key2": True,
        "key3": spark.createDataFrame(
            [
                {"row_id": 0, "col1": 1, "col2": "a"},
                {"row_id": 1, "col1": 2, "col2": "b"},
                {"row_id": 2, "col1": 3, "col2": "c"},
            ]
        ),
    }

    # Call the writer method and assert that it does not return any value
    assert writer_object.writer(_write_args=write_args) is None
