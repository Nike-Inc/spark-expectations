import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql.types import StructType, StructField, IntegerType


def filter_and_group_positive_numbers(df, column_name):
    """
    Filters positive numbers in the specified column, groups them by modulo 3,
    and returns the sum of values in each group.
    """
    filtered_df = df.filter(col(column_name) > 0)
    grouped_df = filtered_df.withColumn("mod_group", col(column_name) % 3)
    result_df = grouped_df.groupBy("mod_group").agg(spark_sum(col(column_name)).alias("sum_value"))
    return result_df


@pytest.fixture(scope="session")
def spark_session():
    print("starting spark")
    spark = SparkSession.builder \
        .appName("PySparkUnitTest") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()
    print("stopping spark")


# Unit test

# Test with a small dataset
def test_small_dataset(spark_session):
    spark = spark_session
    data = [(-2,), (0,), (1,), (2,), (3,), (4,)]
    df = spark.createDataFrame(data, ["value"])
    result_df = filter_and_group_positive_numbers(df, "value")
    result = {row.mod_group: row.sum_value for row in result_df.collect()}
    expected = {
        0: 3,
        1: 5,
        2: 2
    }
    assert result == expected

# Test with all negative values
def test_all_negative_values(spark_session):
    spark = spark_session
    data = [(-5,), (-4,), (-3,), (-2,), (-1,)]
    df = spark.createDataFrame(data, ["value"])
    result_df = filter_and_group_positive_numbers(df, "value")
    result = [row.sum_value for row in result_df.collect()]
    assert result == []

# Test with all positive values
def test_all_positive_values(spark_session):
    spark = spark_session
    data = [(1,), (2,), (3,), (4,), (5,)]
    df = spark.createDataFrame(data, ["value"])
    result_df = filter_and_group_positive_numbers(df, "value")
    result = {row.mod_group: row.sum_value for row in result_df.collect()}
    expected = {
        0: 3,   # 3
        1: 1 + 4,   # 1 + 4 = 5
        2: 2 + 5    # 2 + 5 = 7
    }
    assert result == expected

# Test with empty DataFrame
def test_empty_dataframe(spark_session):
    spark = spark_session
    data = []
    schema = StructType([StructField("value", IntegerType(), True)])
    df = spark.createDataFrame(data, schema)
    result_df = filter_and_group_positive_numbers(df, "value")
    result = [row.sum_value for row in result_df.collect()]
    assert result == []

# Test with a large dataset (original test)
def test_large_dataset(spark_session):
    spark = spark_session
    data = [(i,) for i in range(-1000, 1000)]
    df = spark.createDataFrame(data, ["value"])
    result_df = filter_and_group_positive_numbers(df, "value")
    result = {row.mod_group: row.sum_value for row in result_df.collect()}
    expected = {i: sum(x for x in range(1, 1000) if x % 3 == i) for i in range(3)}
    assert result == expected

# Parametrized test for different modulo values
@pytest.mark.parametrize("modulo", [2, 3, 5])
def test_parametrized_modulo(spark_session, modulo):
    spark = spark_session
    data = [(i,) for i in range(1, 21)]
    df = spark.createDataFrame(data, ["value"])
    filtered_df = df.filter(col("value") > 0)
    grouped_df = filtered_df.withColumn("mod_group", col("value") % modulo)
    result_df = grouped_df.groupBy("mod_group").agg(spark_sum(col("value")).alias("sum_value"))
    result = {row.mod_group: row.sum_value for row in result_df.collect()}
    expected = {i: sum(x for x in range(1, 21) if x % modulo == i) for i in range(modulo)}
    assert result == expected

# Test parallel execution (pytest-xdist required)
def test_parallel_execution(spark_session):
    spark = spark_session
    data = [(i,) for i in range(1, 10001)]
    df = spark.createDataFrame(data, ["value"])
    result_df = filter_and_group_positive_numbers(df, "value")
    result = {row.mod_group: row.sum_value for row in result_df.collect()}
    expected = {i: sum(x for x in range(1, 10001) if x % 3 == i) for i in range(3)}
    assert result == expected

def test_very_large_dataset(spark_session):
    spark = spark_session
    # 10 million rows, will take noticeably longer
    data = [(i,) for i in range(-100000, 100000)]
    df = spark.createDataFrame(data, ["value"])
    result_df = filter_and_group_positive_numbers(df, "value")
    result = {row.mod_group: row.sum_value for row in result_df.collect()}
    expected = {i: sum(x for x in range(1, 100000) if x % 3 == i) for i in range(3)}
    assert result == expected