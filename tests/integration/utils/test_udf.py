from pyspark.sql.types import LongType, DoubleType
from spark_expectations.core import get_spark_session
from spark_expectations.utils.udf import remove_empty_maps, get_actions_list, safe_cast

spark = get_spark_session()


def test_remove_empty_maps():
    # create a dummy Spark DataFrame with a single column 'column' containing a list of maps
    df = spark.createDataFrame(
        [
            ([{"a": "1"}, {}, {"b": "2"}, {"c": "3"}, {}, {"d": "4"}],),
            ([{}, {}],),
            ([{"g": "7"}, {"h": "8"}, {"i": "9"}],),
            ([],),
        ],
        ["column"],
    )

    expected_output = [[{"a": "1"}, {"b": "2"}, {"c": "3"}, {"d": "4"}], [], [{"g": "7"}, {"h": "8"}, {"i": "9"}], []]

    # apply the remove_empty_map UDF to the 'column' column of the DataFrame
    result = df.withColumn("result", remove_empty_maps(df["column"]))

    for i in range(0, 4):
        # assert that the result of the UDF is as expected
        assert result.select("result").collect()[i]["result"] == expected_output[i]


def test_get_actions_list():
    # Create a test DataFrame
    data = [
        (
            1,
            [
                {"action_if_failed": "drop", "status": "fail", "other_key": "value1"},
                {"action_if_failed": "ignore", "status": "fail", "other_key": "value2"},
            ],
        ),
        (2, [{"action_if_failed": "ignore", "status": "fail", "other_key": "value3"}]),
        (3, []),
        (
            4,
            [
                {"action_if_failed": "ignore", "status": "fail", "other_key": "value4"},
                {"action_if_failed": "fail", "status": "fail", "other_key": "value5"},
            ],
        ),
    ]
    df = spark.createDataFrame(data, ["id", "dq_res"])

    df = df.withColumn("actions", get_actions_list(df["dq_res"]))

    # Collect the results and check if they are correct
    results = df.select("actions").collect()

    expected_output = [["drop", "ignore"], ["ignore"], ["ignore"], ["ignore", "fail"]]

    for itr in range(0, 4):
        assert results[itr].actions == expected_output[itr]

def test_safe_cast_ansi_disabled():
    # with default spark.sql.ansi.enabled=false, safe cast should return
    #  a column equavalent to cast(col as type)
    spark.conf.set("spark.sql.ansi.enabled", "false")

    columns = ["Name", "Value"]
    data = [("thing", "123")]
    df = spark.createDataFrame(data, schema=columns)

    result_df = df.withColumn("casted_value", safe_cast(spark, "Value", "bigint"))
    result = result_df.select("casted_value").collect()[0]["casted_value"]

    assert result == 123
    assert result_df.schema["casted_value"].dataType == LongType()


def test_safe_cast_ansi_enabled():
    # with spark.sql.ansi.enabled=true, safe cast should succeed if it's a castable value and should 
    #  return Null if it's not a valid cast (without safe cast it would throw an exception insead)
    spark.conf.set("spark.sql.ansi.enabled", "true")

    columns = ["Name", "Value", "Color"]
    data = [("mug", "10", "red")]
    df = spark.createDataFrame(data, schema=columns)

    success_result_df = df.withColumn("success_casted_value", safe_cast(spark, "Value", "double"))
    success_result = success_result_df.select("success_casted_value").collect()[0]["success_casted_value"]

    fail_result_df = df.withColumn("fail_casted_value", safe_cast(spark, "Color", "double"))
    fail_result = fail_result_df.select("fail_casted_value").collect()[0]["fail_casted_value"]

    # re-set ansi mode so it doesn't affect other tests
    spark.conf.set("spark.sql.ansi.enabled", "false")

    assert success_result == 10
    assert success_result_df.schema["success_casted_value"].dataType == DoubleType()

    assert fail_result is None
