from spark_expectations.core import get_spark_session
from spark_expectations.utils.udf import remove_empty_maps, get_actions_list

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
