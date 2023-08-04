import os
import pytest
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from spark_expectations.core import get_spark_session
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.sinks.plugins.delta_writer import SparkExpectationsDeltaWritePluginImpl

spark = get_spark_session()


@pytest.fixture(name="_fixture_create_database")
def fixture_create_database():
    # drop and create dq_spark if exists
    os.system("rm -rf /tmp/hive/warehouse/dq_spark.db")
    spark.sql("create database if not exists dq_spark")
    spark.sql("use dq_spark")

    yield "dq_spark"

    # drop dq_spark if exists
    os.system("rm -rf /tmp/hive/warehouse/dq_spark.db")


@pytest.fixture(name="_fixture_dataset")
def fixture_dataset():
    # Create a mock dataframe
    data = [(1, "John", 25), (2, "Jane", 30), (3, "Jim", 35)]
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture(name="_fixture_create_test_table")
def fixture_create_test_table():
    # drop if exist dq_spark database and create with test_dq_stats_table
    os.system("rm -rf /tmp/hive/warehouse/dq_spark.db")
    spark.sql("create database if not exists dq_spark")
    spark.sql("use dq_spark")

    spark.sql("drop table if exists test_table")
    os.system("rm -rf /tmp/hive/warehouse/dq_spark.db/test_table")
    spark.sql(
        """
        create table test_table_write (
        id integer,
        name string,
        age integer
        )
        USING delta
        """
    )

    yield "test_table"

    spark.sql("drop table if exists test_table_write")
    os.system("rm -rf /tmp/hive/warehouse/dq_spark.db/test_table_write")

    # remove database
    os.system("rm -rf /tmp/hive/warehouse/dq_spark.db")


# Write the test function
def test_writer(_fixture_create_database, _fixture_dataset, _fixture_create_test_table):
    delta_writer_handler = SparkExpectationsDeltaWritePluginImpl()

    write_args = {
        "stats_df": _fixture_dataset,
        "table_name": "dq_spark.test_table_write"
    }

    delta_writer_handler.writer(_write_args=write_args)

    expected_df = spark.table("test_table_write")

    assert expected_df.orderBy("id").collect() == _fixture_dataset.orderBy("id").collect()


def test_writer_exception(_fixture_create_database, _fixture_create_test_table):
    delta_writer_handler = SparkExpectationsDeltaWritePluginImpl()

    write_args = {
        "table_name": "dq_spark.test_table"
    }

    with pytest.raises(SparkExpectationsMiscException,
                       match=r"error occurred while saving data into delta stats table .*"):
        delta_writer_handler.writer(_write_args=write_args)
