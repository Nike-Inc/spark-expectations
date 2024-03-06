from unittest.mock import Mock
from unittest.mock import patch

import pytest
from pyspark.sql.functions import lit, struct, array, udf

from spark_expectations.core import get_spark_session
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core.exceptions import (
    SparkExpectationsMiscException
)
from spark_expectations.utils.actions import SparkExpectationsActions

spark = get_spark_session()

@pytest.fixture(name="_fixture_df")
def fixture_df():
    # Create a sample input dataframe
    return spark.createDataFrame(
        [
            {"row_id": 0, "col1": 1, "col2": "a"},
            {"row_id": 1, "col1": 2, "col2": "b"},
            {"row_id": 2, "col1": 3, "col2": "c"},
        ]
    )


@pytest.fixture(name="_fixture_mock_context")
def fixture_mock_context():
    # fixture for mock context
    mock_object = Mock(spec=SparkExpectationsContext)
    mock_object.product_id = "product1"
    mock_object.spark=spark
    mock_object.get_row_dq_rule_type_name = "row_dq"
    mock_object.get_agg_dq_rule_type_name = "agg_dq"
    mock_object.get_query_dq_rule_type_name = "query_dq"
    mock_object.get_supported_df_query_dq = spark.createDataFrame(
        [
            {
                "spark_expectations_test": "se_query_dq"
            }
        ]
    )
    return mock_object


@pytest.fixture(name="_fixture_expectations")
def fixture_expectations():
    # Define the expectations for the data quality rules
    return {
        "row_dq_rules": [
            {
                "rule_type": "row_dq",
                "rule": "col1_gt_eq_1",
                "expectation": "col1 >=1",
                "action_if_failed": "ignore",
                "table_name": "test_table",
                "tag": "validity",
                "description": "col1 gt or eq 1"
            },
            {
                "rule_type": "row_dq",
                "rule": "col1_gt_eq_2",
                "expectation": "col1 >= 2",
                "action_if_failed": "drop",
                "table_name": "test_table",
                "tag": "accuracy",
                "description": "col1 gt or eq 2"
            },
            {
                "rule_type": "row_dq",
                "rule": "col1_gt_eq_3",
                "expectation": "col1 >= 3",
                "action_if_failed": "fail",
                "table_name": "test_table",
                "tag": "completeness",
                "description": "col1 gt or eq 3"
            },
        ],
        "agg_dq_rules": [
            {
                "rule_type": "agg_dq",
                "rule": "col1_sum_gt_eq_6",
                "expectation": "sum(col1)>=6",
                "action_if_failed": "ignore",
                "table_name": "test_table",
                "tag": "validity",
                "enable_for_source_dq_validation": True,
                "description": "col1 sum gt 1"
            },
            {
                "rule_type": "agg_dq",
                "rule": "col2_unique_value_gt_3",
                "expectation": "count(distinct col2)>3",
                "action_if_failed": "fail",
                "table_name": "test_table",
                "tag": "accuracy",
                "enable_for_source_dq_validation": True,
                "description": "col2 unique value grater than 3"
            }
        ],
        "query_dq_rules": [
            {
                "rule_type": "query_dq",
                "rule": "table_row_count_gt_1",
                "expectation": "(select count(*) from query_test_table)>1",
                "action_if_failed": "ignore",
                "table_name": "test_table",
                "tag": "validity",
                "enable_for_target_dq_validation": True,
                "description": "table count should be greater than 1"
            },
            {
                "rule_type": "query_dq",
                "rule": "table_distinct_count",
                "expectation": "(select count(*) from (select distinct col1, col2 from query_test_table))>3",
                "action_if_failed": "fail",
                "table_name": "test_table",
                "tag": "accuracy",
                "enable_for_target_dq_validation": True,
                "description": "table distinct row count should be greater than 3"
            }
        ]

    }


@pytest.fixture(name="_fixture_row_dq_expected_result")
def fixture_row_dq_expected_result():
    # define the expected result for row dq operations
    return {
        "result": [
            {
                "row_dq_col1_gt_eq_1": {},
                "row_dq_col1_gt_eq_2": {
                    "rule_type": "row_dq",
                    "rule": "col1_gt_eq_2",
                    "action_if_failed": "drop",
                    "tag": "accuracy",
                    "description": "col1 gt or eq 2"
                },
                "row_dq_col1_gt_eq_3": {"rule_type": "row_dq",
                                        "rule": "col1_gt_eq_3",
                                        "action_if_failed": "fail",
                                        "tag": "completeness",
                                        "description": "col1 gt or eq 3"
                                        }
            },

            {
                "row_dq_col1_gt_eq_1": {},
                "row_dq_col1_gt_eq_2": {},
                "row_dq_col1_gt_eq_3": {"rule_type": "row_dq",
                                        "rule": "col1_gt_eq_3",
                                        "action_if_failed": "fail",
                                        "tag": "completeness",
                                        "description": "col1 gt or eq 3"
                                        }
            },

            {
                "row_dq_col1_gt_eq_1": {},
                "row_dq_col1_gt_eq_2": {},
                "row_dq_col1_gt_eq_3": {}
            }

        ]}


@pytest.fixture(name="_fixture_agg_dq_expected_result")
def fixture_agg_dq_expected_result():
    # define the expected result for agg dq operations
    return {
        "result":
            [{
                "rule_type": "agg_dq",
                "rule": "col2_unique_value_gt_3",
                "action_if_failed": "fail",
                "tag": "accuracy",
                "description": "col2 unique value grater than 3"
            }
            ]
    }


@pytest.fixture(name="_fixture_query_dq_expected_result")
def fixture_query_dq_expected_result():
    # define the expected result for agg dq operations
    return {
        "result":
            [{
                "rule_type": "query_dq",
                "rule": "table_distinct_count",
                "action_if_failed": "fail",
                "tag": "accuracy",
                "description": "table distinct row count should be greater than 3"
            }
            ]
    }


import pytest


@pytest.mark.parametrize(
    "rule, rule_type_name, source_dq_enabled, target_dq_enabled, expected",
    [
        (
                {"enable_for_source_dq_validation": True},
                "query_dq",
                True,
                False,
                True,
        ),
        (
                {"enable_for_target_dq_validation": False},
                "agg_dq",
                False,
                True,
                False,
        ),
        (
                {"enable_for_source_dq_validation": True},
                "query_dq",
                False,
                False,
                False,
        ),
    ],
)
def test_get_rule_is_active(rule, rule_type_name,
                            source_dq_enabled,
                            target_dq_enabled,
                            expected,
                            _fixture_mock_context):
    assert SparkExpectationsActions.get_rule_is_active(_fixture_mock_context,
                                                       rule,
                                                       rule_type_name,
                                                       source_dq_enabled,
                                                       target_dq_enabled) == expected


@pytest.mark.parametrize("_rule_map, expected_output", [
    # expectations rule
    ({"rule_type": "type1", "rule": "rule1", "action_if_failed": "action1", "description": "desc1"},
     # result in spark col object
     array([struct(lit("rule_type"), lit("type1")),
            struct(lit("rule"), lit("rule1")),
            struct(lit("action_if_failed"), lit("action1")),
            struct(lit("description"), lit("desc1"))])),
    # expectations rule
    ({"rule_type": "type2", "rule": "rule2", "action_if_failed": "action2", "description": "desc2"},
     # result in spark col object
     array([struct(lit("rule_type"), lit("type2")),
            struct(lit("rule"), lit("rule2")),
            struct(lit("action_if_failed"), lit("action2")),
            struct(lit("description"), lit("desc2"))])),
])
def test_create_rules_map(_rule_map, expected_output):
    # test function which creates the dq_results based on the expectations
    actual_output = SparkExpectationsActions.create_rules_map(_rule_map)

    @udf
    def compare_result(_actual_output, _expected_output):
        for itr in (0, 4):
            assert _actual_output[itr] == _expected_output[itr]

    # assert dataframe column object using udf
    compare_result(actual_output, expected_output)


@pytest.mark.parametrize("input_df, rule_type_name, expected_output",
                         # input_df
                         [(spark.createDataFrame(
                             [{"meta_agg_dq_results": [{"action_if_failed": "ignore", "description": "desc1"},
                                                       {"action_if_failed": "ignore", "description": "desc2"},
                                                       {"action_if_failed": "ignore", "description": "desc3"}]
                               }]),
                           "agg_dq",  # rule_type_name
                           # expected_output
                           [{"action_if_failed": "ignore", "description": "desc1"},
                            {"action_if_failed": "ignore", "description": "desc2"},
                            {"action_if_failed": "ignore", "description": "desc3"}],
                         ),  # input_df
                             # input_df
                             (spark.createDataFrame(
                                 [{"meta_query_dq_results": [{"action_if_failed": "ignore", "description": "desc1"},
                                                             ]
                                   }]),
                              # expected_output
                              "query_dq",  # rule_type_name
                              [{'action_if_failed': 'ignore', 'description': 'desc1'}]
                             ),
                             (spark.createDataFrame(
                                 [{"meta_query_dq_results": [{"action_if_failed": "ignore", "description": "desc1"},
                                                             ]
                                   }]),
                              # expected_output
                              "row_dq",  # rule_type_name
                              None
                             )
                         ])
def test_create_agg_dq_results(input_df,
                               rule_type_name,
                               expected_output, _fixture_mock_context):
    # unit test case on create_agg_dq_results
    assert SparkExpectationsActions().create_agg_dq_results(_fixture_mock_context,input_df, rule_type_name, ) == expected_output


@pytest.mark.parametrize("input_df",
                         [(spark.createDataFrame(
                             [{"agg_dq_results": ""}]),
                         ),
                             (None,
                              "agg_dq",  # rule_type_name
                              # expected_output
                              None),
                         ])
def test_create_agg_dq_results_exception(input_df,
                                         _fixture_mock_context):
    # faulty user input is given to test the exception functionality of the agg_dq_result
    with pytest.raises(SparkExpectationsMiscException,
                       match=r"error occurred while running create agg dq results .*"):
        SparkExpectationsActions().create_agg_dq_results(_fixture_mock_context, input_df, "<test>", )


def test_run_dq_rules_row(_fixture_df,
                          _fixture_expectations,
                          _fixture_row_dq_expected_result,
                          _fixture_mock_context):
    # Apply the data quality rules
    result_df = SparkExpectationsActions.run_dq_rules(_fixture_mock_context, _fixture_df, _fixture_expectations,
                                                      "row_dq")

    # Assert that the result dataframe has the expected number of columns
    assert len(result_df.columns) == 6

    result_df.show(truncate=False)

    # Assert that the result dataframe has the expected values for each rule
    for row in result_df.collect():
        row_id = row["row_id"]
        assert row.row_dq_col1_gt_eq_1 == _fixture_row_dq_expected_result.get("result")[row_id].get(
            "row_dq_col1_gt_eq_1")
        assert row.row_dq_col1_gt_eq_2 == _fixture_row_dq_expected_result.get("result")[row_id].get(
            "row_dq_col1_gt_eq_2")
        assert row.row_dq_col1_gt_eq_3 == _fixture_row_dq_expected_result.get("result")[row_id].get(
            "row_dq_col1_gt_eq_3")


def test_run_dq_rules_agg(_fixture_df,
                          _fixture_expectations,
                          _fixture_agg_dq_expected_result,
                          _fixture_mock_context):
    # Apply the data quality rules
    result_df = SparkExpectationsActions.run_dq_rules(_fixture_mock_context, _fixture_df, _fixture_expectations,
                                                      "agg_dq", True)

    # Assert that the result dataframe has the expected number of columns
    assert len(result_df.columns) == 1

    result_df.show(truncate=False)

    # Assert that the result dataframe has the expected values for each rule
    row = result_df.collect()[0]
    assert row.meta_agg_dq_results == _fixture_agg_dq_expected_result.get("result")


def test_run_dq_rules_query(_fixture_df,
                            _fixture_expectations,
                            _fixture_query_dq_expected_result,
                            _fixture_mock_context):
    # Apply the data quality rules
    _fixture_df.createOrReplaceTempView("query_test_table")
    print(_fixture_expectations["query_dq_rules"])
    result_df = SparkExpectationsActions.run_dq_rules(_fixture_mock_context, _fixture_df, _fixture_expectations,
                                                      "query_dq", False, True)

    # Assert that the result dataframe has the expected number of columns
    assert len(result_df.columns) == 1

    result_df.show(truncate=False)

    # Assert that the result dataframe has the expected values for each rule
    row = result_df.collect()[0]
    assert row.meta_query_dq_results == _fixture_query_dq_expected_result.get("result")


def test_run_dq_rules_negative_case(_fixture_df, _fixture_mock_context):
    expectations = {"row_dq_rules": []}

    with pytest.raises(SparkExpectationsMiscException,
                       match=r"error occurred while running expectations .*"):
        SparkExpectationsActions.run_dq_rules(_fixture_mock_context, _fixture_df, expectations, "row_dq")


@pytest.mark.parametrize(
    "input_df, table_name, input_count, error_count, output_count, "
    "rule_type, row_dq_flag, source_agg_flag, final_agg_flag, source_query_dq_flag, final_query_dq_flag, expected_output",
    [
        (
                # test case 1
                # In this test case, action_if_failed consist "ignore"
                # function should return dataframe with all the rows
                spark.createDataFrame(
                    [  # log into err & final
                        {"col1": 1, "col2": "a", "meta_row_dq_results": [{"action_if_failed": "ignore"},
                                                                         {"action_if_failed": "ignore"},
                                                                         {"action_if_failed": "ignore"}]},
                        # log into err & final
                        {"col1": 2, "col2": "b", "meta_row_dq_results":
                            [{"action_if_failed": "ignore"}]},
                        # log into err & final
                        {"col1": 3, "col2": "c", "meta_row_dq_results": [{"action_if_failed": "ignore"}]}
                    ]
                ),
                'test_dq_stats_table',  # table name
                3,  # input count
                3,  # error count
                3,  # output count
                "row_dq",  # rule type
                True,  # row_dq_flag
                False,  # source_agg_dq_flag
                False,  # final_dg_flag
                False,  # source_query_dq_flag
                False,  # final_query_dq_flag
                # expected dataframe
                spark.createDataFrame([
                    {"col1": 1, "col2": "a"},
                    {"col1": 2, "col2": "b"},
                    {"col1": 3, "col2": "c"}
                ])
        ),
        (
                # In this test case, action_if_failed consist "drop"
                # function should return dataframe with all the rows
                spark.createDataFrame(
                    [  # drop & log into error
                        {"col1": 1, "col2": "a", "meta_row_dq_results": [{"action_if_failed": "drop"},
                                                                         {"action_if_failed": "drop"},
                                                                         {"action_if_failed": "drop"}]},
                        # drop & log into error
                        {"col1": 2, "col2": "b", "meta_row_dq_results": [
                            {"action_if_failed": "drop"}]},
                        # log into final
                        {"col1": 3, "col2": "c", "meta_row_dq_results": []}
                    ]
                ),
                'test_dq_stats_table',  # table name
                3,  # input count
                2,  # error count
                1,  # output count
                "row_dq",  # rule type
                True,  # row_dq_flag
                False,  # source_agg_dq_flag
                False,  # final_agg_dq_flag
                False,  # source_query_dq_flag
                False,  # final_query_dq_flag
                # expected df
                spark.createDataFrame([
                    {"col1": 3, "col2": "c"}
                ])

        ),
        (
                # test case 3
                # In this test case, action_if_failed consist "fail"
                # spark expectations expected to fail
                spark.createDataFrame(
                    [  # log into error and raise error
                        {"col1": 1, "col2": "a", "meta_row_dq_results": [{"action_if_failed": "fail"},
                                                                         {"action_if_failed": "fail"},
                                                                         {"action_if_failed": "fail"}]},
                        {"col1": 2, "col2": "b", "meta_row_dq_results": []},
                        {"col1": 3, "col2": "c", "meta_row_dq_results": []}
                    ]
                ),
                'test_dq_stats_table',  # table name
                3,  # input count
                1,  # error count
                0,  # output count
                "row_dq",  # rule tye
                True,  # row_dq_flag
                False,  # source_agg_dq_flag
                False,  # final_agg_dq_flag
                False,  # source_query_dq_flag
                False,  # final_query_dq_flag
                SparkExpectationsMiscException  # expected res
        ),
        (
                # test case 4
                # In this test case, action_if_failed consist "ignore" & "drop"
                # function should return dataframe with 2 the rows
                spark.createDataFrame(
                    [
                        # drop, log into error
                        {"col1": 1, "col2": "a", "meta_row_dq_results": [{"action_if_failed": "ignore"},
                                                                         {"action_if_failed": "ignore"},
                                                                         {"action_if_failed": "drop"}]},
                        # log into error and final
                        {"col1": 2, "col2": "b", "meta_row_dq_results": [{"action_if_failed": "ignore"}]},
                        # log into error and final
                        {"col1": 2, "col2": "c", "meta_row_dq_results": [
                            {"action_if_failed": "ignore"}]}
                    ]
                ),
                'test_dq_stats_table',  # table name
                3,  # input count
                3,  # error count
                2,  # output count
                "row_dq",  # rule type
                True,  # row_dq_flag
                False,  # source_agg_dq_flag
                False,  # final_agg_dq_flag
                False,  # source_query_dq_flag
                False,  # final_query_dq_flag
                # expected df
                spark.createDataFrame([
                    {"col1": 2, "col2": "b"},
                    {"col1": 2, "col2": "c"}
                ])
        ),

        (
                # test case 5
                # In this test case, action_if_failed consist "ignore" & "fail"
                # spark expectations expected to fail
                spark.createDataFrame(
                    [
                        # log into to error & fail program
                        {"col1": 1, "col2": "a", "meta_row_dq_results": [{"action_if_failed": "ignore"},
                                                                         {"action_if_failed": "ignore"}
                            , {"action_if_failed": "fail"}]},
                        # log into error
                        {"col1": 2, "col2": "b",
                         "meta_row_dq_results": [{"action_if_failed": "ignore"}]},
                        {"col1": 3, "col2": "c", "meta_row_dq_results": []}
                    ]
                ),
                'test_dq_stats_table',  # table name
                3,  # input count
                2,  # error count
                0,  # output count
                "row_dq",  # rule type
                True,  # row_dq_flag
                False,  # source_agg_dq_flag
                False,  # final_agg_dq_flag
                False,  # source_query_dq_flag
                False,  # final_query_dq_flag
                SparkExpectationsMiscException  # expected res
        ),
        # test case 6
        # In this test case, action_if_failed consist "drop" & "fail"
        # spark expectations expected to fail
        (spark.createDataFrame(
            [
                # log into error & fail
                {"col1": 1, "col2": "a", "meta_row_dq_results": [{"action_if_failed": "drop"},
                                                                 {"action_if_failed": "drop"},
                                                                 {"action_if_failed": "fail"}]},
                # log into error & drop
                {"col1": 2, "col2": "b", "meta_row_dq_results": [{"action_if_failed": "drop"}]},
                {"col1": 3, "col2": "c", "meta_row_dq_results": []}
            ]
        ),
         'test_dq_stats_table',  # table name
         3,  # input count
         2,  # error count
         0,  # output count
         "row_dq",  # rule type
         True,  # row_dq_flag
         False,  # source_agg_dq_flag
         False,  # final_agg_dq_flag
         False,  # source_query_dq_flag
         False,  # final_query_dq_flag
         SparkExpectationsMiscException  # expected res
        ),

        (
                # test case 7
                # In this test case, action_if_failed consist "ignore" & "drop"
                # function returns dataframe with 2 rows
                spark.createDataFrame(
                    [
                        # drop and log into error
                        {"col1": 1, "col2": "a", "meta_row_dq_results": [{"action_if_failed": "drop"},
                                                                         {"action_if_failed": "ignore"},
                                                                         {"action_if_failed": "ignore"}]},
                        # log into final
                        {"col1": 2, "col2": "b", "meta_row_dq_results":
                            [{"action_if_failed": "test"}]},
                        # log into final
                        {"col1": 3, "col2": "c", "meta_row_dq_results": []}
                    ]
                ),
                'test_dq_stats_table',  # table name
                3,  # input count
                1,  # error count
                2,  # output count
                "row_dq",  # rule type
                True,  # row_dq_flag
                False,  # source_agg_dq_flag
                False,  # final_agg_dq_flag
                False,  # source_query_dq_flag
                False,  # final_query_dq_flag
                # expected df
                spark.createDataFrame([
                    {"col1": 2, "col2": "b"},
                    {"col1": 3, "col2": "c"}
                ])
        ),

        # (
        #             # test case 8
        #             spark.createDataFrame(
        #                 [
        #                     {"col1": 1, "col2": "a"},
        #                     {"col1": 2, "col2": "b"},
        #                     {"col1": 3, "col2": "c"},
        #
        #                 ]
        #             ).withColumn("dq_rule_col_gt_eq_1", create_map()),
        #             'test_dq_stats_table',
        #             3,
        #             0,
        #             spark.createDataFrame([
        #                 {"col1": 1, "col2": "a"},
        #                 {"col1": 2, "col2": "b"},
        #                 {"col1": 3, "col2": "c"}
        #             ])
        #     )
        (
                # test case 9
                # In this test case, action_if_failed consist "ignore"  for agg dq on source_agg_dq
                # function returns empty dataframe
                spark.createDataFrame(
                    [
                        {"meta_agg_dq_results": [{"action_if_failed": "ignore"}]},
                    ]
                ),
                'test_dq_stats_table',  # table name
                100,  # input count
                30,  # output count
                0,  # error count
                "agg_dq",  # rule type
                False,  # row_dq_flag
                True,  # source_agg_dq_flag
                False,  # final_agg_dq_flag
                False,  # source_query_dq_flag
                False,  # final_query_dq_flag
                # expected df
                spark.createDataFrame([{"meta_agg_dq_results": [{"action_if_failed": "ignore"}]}]).drop(
                    "meta_agg_dq_results")
        ),
        (
                # test case 10
                # In this test case, action_if_failed consist "fail" for agg dq on source_agg_dq
                # function returns empty dataframe
                spark.createDataFrame(
                    [
                        {"meta_agg_dq_results": [{"action_if_failed": "fail"}]},
                    ]
                ),
                'test_dq_stats_table',  # table name
                100,  # input count
                10,  # output count
                0,  # error count
                "agg_dq",  # rule type
                False,  # row_dq_flag
                True,  # source_agg_dq_flag
                False,  # final_agg_dq_flag
                False,  # source_query_dq_flag
                False,  # final_query_dq_flag
                SparkExpectationsMiscException  # expected res
        ),
        (
                # test case 11
                # In this test case, action_if_failed consist "ignore" for agg dq on final_agg_dq
                # function returns empty dataframe
                spark.createDataFrame(
                    [
                        {"meta_agg_dq_results": [{"action_if_failed": "ignore"}]},
                    ]
                ),
                'test_dq_stats_table',  # table name
                100,  # input count
                20,  # error count
                0,  # output count
                "agg_dq",  # rule type
                False,  # row_dq_flag
                False,  # source_agg_flag
                True,  # final_agg_flag
                False,  # source_query_dq_flag
                False,  # final_query_dq_flag
                # expected df
                spark.createDataFrame(
                    [
                        {"meta_agg_dq_results": [{"action_if_failed": "ignore"}]},
                    ]
                ).drop("meta_agg_dq_results")
        ),
        (
                # test case 12
                # In this test case, action_if_failed consist "fail" for agg dq on final_agg_aq
                # spark expectations set to fail for agg_dq on final_agg-dq
                spark.createDataFrame(
                    [
                        {"meta_agg_dq_results": [{"action_if_failed": "fail"}]},
                    ]
                ),
                'test_dq_stats_table',  # table name
                100,  # input count
                30,  # error count
                0,  # output count
                "agg_dq",  # rule type
                False,  # row_dq_flag
                False,  # source_agg_flag
                True,  # final_agg_flag
                False,  # source_query_dq_flag
                False,  # final_query_dq_flag
                SparkExpectationsMiscException  # expected res
        ),

        (
                # test case 13
                # In this test case, action_if_failed consist "fail" for agg dq on final_agg_dq
                # spark expectations set to fail for agg_dq on final_agg_dq
                spark.createDataFrame(
                    [
                        {"meta_agg_dq_results": [{"action_if_failed": "fail"}]},
                    ]
                ),
                'test_dq_stats_table',  # table name
                100,  # input count
                0,  # error count
                0,  # output count
                "agg_dq",  # rule type
                False,  # row_dq_flag
                False,  # source_agg_flag
                True,  # final_agg_flag
                False,  # source_query_dq_flag
                False,  # final_query_dq_flag
                SparkExpectationsMiscException  # expected res
        ),

        (
                # test case 14
                # In this test case, action_if_failed consist "ignore", "fail"  for agg dq on final_agg_dq
                # spark expectations set to fail for agg_dq on final_agg_dq
                spark.createDataFrame(
                    [
                        {"meta_agg_dq_results": [{"action_if_failed": "ignore"},
                                                 {"action_if_failed": "fail"}]},
                    ]
                ),
                'test_dq_stats_table',  # table name
                100,  # input count
                0,  # error count
                0,  # output count
                "agg_dq",  # rule type
                False,  # row_dq_flag
                False,  # source_agg_flag
                True,  # final_agg_flag
                False,  # source_query_dq_flag
                False,  # final_query_dq_flag
                SparkExpectationsMiscException  # expected res
        ),

        (
                # test case 15
                # In this test case, action_if_failed consist "ignore" for agg dq on source_agg_dq
                # function returns a empty datatset
                spark.createDataFrame(
                    [
                        {"meta_agg_dq_results": [{"action_if_failed": "ignore"},
                                                 {"action_if_failed": "ignore"}]},
                    ]
                ),
                'test_dq_stats_table',  # table name
                100,  # input count
                0,  # error count
                0,  # output count
                "agg_dq",  # rule type
                False,  # row_dq_flag
                True,  # source_agg_flag
                False,  # final_agg_flag
                False,  # source_query_dq_flag
                False,  # final_query_dq_flag
                spark.createDataFrame(
                    [
                        {"meta_agg_dq_results": [{"action_if_failed": "ignore"}]},
                    ]
                ).drop("meta_agg_dq_results")  # expected df
        ),

        (
                # test case 16
                # In this test case, action_if_failed consist "ignore" for agg dq on final_agg_dq
                # function returns a empty datatset
                spark.createDataFrame(
                    [
                        {"meta_agg_dq_results": [{"action_if_failed": "ignore"},
                                                 {"action_if_failed": "ignore"},
                                                 {"action_if_failed": "ignore"}]},
                    ]
                ),
                'test_dq_stats_table',  # table name
                100,  # input count
                15,  # error count
                0,  # output count
                "agg_dq",  # rule type
                False,  # row_dq_flag
                False,  # source_agg_flag
                True,  # final_agg_flag
                False,  # source_query_dq_flag
                False,  # final_query_dq_flag
                spark.createDataFrame(
                    [
                        {"meta_agg_dq_results": [{"action_if_failed": "ignore"}]},
                    ]
                ).drop("meta_agg_dq_results")  # expected df
        ),
        (
                # test case 17
                # In this test case, action_if_failed consist "ignore" for query dq on source_query_dq
                # function returns a empty datatset
                spark.createDataFrame(
                    [
                        {"meta_query_dq_results": [{"action_if_failed": "ignore"},
                                                   {"action_if_failed": "ignore"},
                                                   {"action_if_failed": "ignore"}]},
                    ]
                ),
                'test_dq_stats_table',  # table name
                100,  # input count
                0,  # error count
                0,  # output count
                "query_dq",  # rule type
                False,  # row_dq_flag
                False,  # source_agg_flag
                True,  # final_agg_flag
                True,  # source_query_dq_flag
                False,  # final_query_dq_flag
                spark.createDataFrame(
                    [
                        {"meta_query_dq_results": [{"action_if_failed": "ignore"}]},
                    ]
                ).drop("meta_query_dq_results")  # expected df
        ),
        (
                # test case 18
                # In this test case, action_if_failed consist "ignore" & "fail" for query dq on source_query_dq
                # function set to fail with exceptions
                spark.createDataFrame(
                    [
                        {"meta_query_dq_results": [{"action_if_failed": "ignore"},
                                                   {"action_if_failed": "fail"},
                                                   {"action_if_failed": "ignore"},
                                                   {"action_if_failed": "ignore"},
                                                   {"action_if_failed": "ignore"}

                                                   ]},
                    ]
                ),
                'test_dq_stats_table',  # table name
                100,  # input count
                0,  # error count
                0,  # output count
                "query_dq",  # rule type
                False,  # row_dq_flag
                False,  # source_agg_flag
                False,  # final_agg_flag
                True,  # source_query_dq_flag
                False,  # final_query_dq_flag
                SparkExpectationsMiscException  # expected df
        ),
        (
                # test case 19
                # In this test case, action_if_failed consist "ignore" & "fail" for query dq on final_query_dq
                # function set to fail with exceptions
                spark.createDataFrame(
                    [
                        {"meta_query_dq_results": [
                            {"action_if_failed": "fail"},
                        ]},
                    ]
                ),
                'test_dq_stats_table',  # table name
                100,  # input count
                25,  # error count
                0,  # output count
                "query_dq",  # rule type
                False,  # row_dq_flag
                False,  # source_agg_flag
                False,  # final_agg_flag
                False,  # source_query_dq_flag
                True,  # final_query_dq_flag
                SparkExpectationsMiscException  # expected df
        ),
        (
                # test case 20
                # In this test case, action_if_failed consist "ignore" for query dq on final_query_dq
                # function returns empty dataset
                spark.createDataFrame(
                    [
                        {"meta_query_dq_results": [
                            {"action_if_failed": "ignore"},
                        ]},
                    ]
                ),
                'test_dq_stats_table',  # table name
                100,  # input count
                10,  # error count
                0,  # output count
                "query_dq",  # rule type
                False,  # row_dq_flag
                False,  # source_agg_flag
                False,  # final_agg_flag
                False,  # source_query_dq_flag
                True,  # final_query_dq_flag
                spark.createDataFrame(
                    [
                        {"meta_query_dq_results": [{"action_if_failed": "ignore"}]},
                    ]
                ).drop("meta_query_dq_results")  # expected df
        ),
        (
                # test case 20
                # In this test case, action_if_failed consist "ignore" for query dq on final_query_dq
                # function returns empty dataset
                spark.createDataFrame(
                    [
                        {"meta_query_dq_results": [
                            {"action_if_failed": "ignore"},
                            {"action_if_failed": "ignore"},
                            {"action_if_failed": "ignore"},
                            {"action_if_failed": "ignore"},
                            {"action_if_failed": "ignore"},
                            {"action_if_failed": "ignore"}
                        ]},
                    ]
                ),
                'test_dq_stats_table',  # table name
                100,  # input count
                10,  # error count
                0,  # output count
                "query_dq",  # rule type
                False,  # row_dq_flag
                False,  # source_agg_flag
                False,  # final_agg_flag
                False,  # source_query_dq_flag
                True,  # final_query_dq_flag
                spark.createDataFrame(
                    [
                        {"meta_query_dq_results": [{"action_if_failed": "ignore"}]},
                    ]
                ).drop("meta_query_dq_results")  # expected df
        )

    ])
@patch('spark_expectations.utils.actions.SparkExpectationsContext.set_final_agg_dq_status', autospec=True,
       spec_set=True)
@patch('spark_expectations.utils.actions.SparkExpectationsContext.set_source_agg_dq_status', autospec=True,
       spec_set=True)
@patch('spark_expectations.utils.actions.SparkExpectationsContext.set_row_dq_status', autospec=True, spec_set=True)
def test_action_on_dq_rules(_mock_set_row_dq_status, _mock_set_source_agg_dq_status, _mock_set_final_agg_dq_status,
                            input_df, table_name, input_count,
                            error_count, output_count, rule_type, row_dq_flag, source_agg_flag, final_agg_flag,
                            source_query_dq_flag,
                            final_query_dq_flag,
                            expected_output,
                            _fixture_mock_context):
    input_df.show(truncate=False)

    # assert for exception when spark expectations set to fail
    if isinstance(expected_output, type) and issubclass(expected_output, Exception):
        with pytest.raises(expected_output, match=r"error occurred while taking action on given rules .*"):
            SparkExpectationsActions.action_on_rules(_fixture_mock_context,
                                                     input_df,
                                                     input_count,
                                                     error_count,
                                                     output_count,
                                                     rule_type,
                                                     row_dq_flag,
                                                     source_agg_flag,
                                                     final_agg_flag,
                                                     source_query_dq_flag,
                                                     final_query_dq_flag
                                                     )

        # _mock_stats_writer.assert_called_once_with(
        #     SparkExpectationsWriter(_fixture_mock_context.product_id, _fixture_mock_context),
        #     table_name,
        #     input_count,
        #     error_count, output_count, source_agg_result, final_agg_result)
        # if row_dq_flag is True:
        #     _fixture_mock_context.set_row_dq_status.assert_called_once_with("Failed")
        # elif source_agg_flag is True:
        #     _fixture_mock_context.set_source_agg_dq_status("Failed")
        # elif final_agg_flag is True:
        #     _fixture_mock_context.set_final_agg_dq_status("Failed")


    else:
        # assert when all condition passes without action_if_failed "fail"
        df = SparkExpectationsActions.action_on_rules(_fixture_mock_context,
                                                      input_df,
                                                      input_count,
                                                      error_count,
                                                      output_count,
                                                      rule_type,
                                                      row_dq_flag,
                                                      source_agg_flag,
                                                      final_agg_flag
                                                      )
        if row_dq_flag is True:
            # assert for row dq expectations
            assert df.orderBy("col2").collect() == expected_output.orderBy("col2").collect()
        else:
            # assert for agg dq expectations
            assert df.collect() == expected_output.collect()


@pytest.mark.parametrize("expectations, expected_exception", [
    ({"rules": [{}]}, SparkExpectationsMiscException),
    ({}, SparkExpectationsMiscException),
    ({"row_dq_rules": [{
        "rule": "col1_gt_eq_1",
        "expectation": "col1 equals or greater than 1",
        "table_name": "test_table",
        "tag": "col1 gt or eq 1"
    }]}, SparkExpectationsMiscException),
    ({"row_dq_rules": [{}]}, SparkExpectationsMiscException)
])
def test_run_dq_rules_exception(_fixture_df, _fixture_mock_context, expectations, expected_exception):
    # test the exception functionality in run_dq_rules with faulty user input
    with pytest.raises(expected_exception,
                       match=r"error occurred while running expectations .*"):
        SparkExpectationsActions.run_dq_rules(_fixture_mock_context, _fixture_df, expectations, "row_dq")


@pytest.mark.parametrize("input_df, table_name, input_count, error_count, output_count, rule_type, row_dq_flag", [
    (  # test case 1
            spark.createDataFrame(
                [
                    {"col1": 1, "col2": "a", "row_dq_results": [{"action_if_failed": "fail"},
                                                                {"action_if_failed": "ignore"},
                                                                {"action_if_failed": "ignore"}]},
                    {"col1": 2, "col2": "b", "row_dq_results": [
                        {"action_if_failed": "test"}]},
                    {"col1": 3, "col2": "c", "row_dq_results": []}
                ]
            ),
            'test_dq_stats',
            3,
            2,
            0,
            "row_dq",
            True
    ),
    # test case 2
    (
            spark.createDataFrame([(1, "a")], ["dq_rule_1", "dq_rule_2"]),
            'test_dq_stats',
            3,
            2,
            0,
            "row_dq",
            True
    )])
def test_action_on_rules_exception(input_df,
                                   table_name,
                                   input_count,
                                   error_count,
                                   output_count,
                                   rule_type,
                                   row_dq_flag,
                                   _fixture_mock_context):
    # test exception functionality with faulty user input
    with pytest.raises(SparkExpectationsMiscException,
                       match=r"error occurred while taking action on given rules .*"):
        SparkExpectationsActions.action_on_rules(_fixture_mock_context,
                                                 input_df,
                                                 table_name,
                                                 input_count,
                                                 error_count,
                                                 output_count,
                                                 rule_type,
                                                 row_dq_flag)


def test_run_dq_rules_condition_expression_exception(_fixture_df,
                                                     _fixture_query_dq_expected_result,
                                                     _fixture_mock_context):
    # Apply the data quality rules
    _expectations = {"query_dq_rules": [
        {
            "rule_type": "query_dq",
            "rule": "table_row_count_gt_1",
            "expectation": "(select count(*) from query_test_table)>1",
            "action_if_failed": "ignore",
            "table_name": "test_table",
            "tag": "validity",
            "enable_for_target_dq_validation": False,
            "description": "table count should be greater than 1"
        }]}
    _fixture_df.createOrReplaceTempView("query_test_table")

    with pytest.raises(SparkExpectationsMiscException,
                       match=r"error occurred while running expectations .*"):
        SparkExpectationsActions.run_dq_rules(_fixture_mock_context, _fixture_df, _expectations,
                                              "query_dq", False, True)
