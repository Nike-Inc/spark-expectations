# pylint: disable=too-many-lines
import os
from unittest.mock import Mock, patch, MagicMock
import pytest
from pyspark.sql.functions import lit
from spark_expectations.core import get_spark_session
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core.expectations import WrappedDataFrameWriter
from spark_expectations.core.exceptions import (
    SparkExpectationsMiscException
)
from spark_expectations.utils.actions import SparkExpectationsActions
from spark_expectations.sinks.utils.writer import SparkExpectationsWriter
from spark_expectations.utils.regulate_flow import SparkExpectationsRegulateFlow

spark = get_spark_session()


@pytest.fixture(name="_fixture_context")
def fixture_mock_context():
    # fixture for  context
    sparkexpectations_context = SparkExpectationsContext("product1", spark)

    sparkexpectations_context._row_dq_rule_type_name = "row_dq"
    sparkexpectations_context._agg_dq_rule_type_name = "agg_dq"
    sparkexpectations_context._query_dq_rule_type_name = "query_dq"
    sparkexpectations_context.set_dq_stats_table_name("test_dq_stats_table")
    sparkexpectations_context._run_date = "2022-12-27 10:39:44"
    sparkexpectations_context._run_id = "product1_run_test"
    sparkexpectations_context._input_count = 10
    writer = WrappedDataFrameWriter().mode('overwrite').format('delta').build()
    sparkexpectations_context.set_target_and_error_table_writer_config(writer)
    sparkexpectations_context.set_stats_table_writer_config(writer)

    return sparkexpectations_context


@pytest.fixture(name="_fixture_actions")
def fixture_actions():
    # fixture for actions
    return SparkExpectationsActions()


@pytest.fixture(name="_fixture_create_stats_table")
def fixture_create_stats_table():
    # drop if exist dq_spark database and create with test_dq_stats_table
    os.system("rm -rf /tmp/hive/warehouse/dq_spark.db")
    spark.sql("create database if not exists dq_spark")
    spark.sql("use dq_spark")

    spark.sql("drop table if exists test_dq_stats_table")
    os.system("rm -rf /tmp/hive/warehouse/dq_spark.db/test_dq_stats_table")

    spark.sql(
        """
    create table test_dq_stats_table (
    product_id STRING,
    table_name STRING,
    input_count LONG,
    error_count LONG,
    output_count LONG,
    output_percentage FLOAT,
    success_percentage FLOAT,
    error_percentage FLOAT,
    source_agg_dq_results array<map<string, string>>,
    final_agg_dq_results array<map<string, string>>,
    source_query_dq_results array<map<string, string>>,
    final_query_dq_results array<map<string, string>>,
    row_dq_res_summary array<map<string, string>>,
    row_dq_error_threshold array<map<string, string>>,
    dq_status map<string, string>,
    dq_run_time map<string, float>,
    dq_rules map<string, map<string,int>>,
    meta_dq_run_id STRING,
    meta_dq_run_date DATE,
    meta_dq_run_datetime TIMESTAMP
    )
    USING delta
    """
    )

    yield "test_dq_stats_table"

    # drop stats table
    spark.sql("drop table if exists test_dq_stats_table")
    os.system("rm -rf /tmp/hive/warehouse/dq_spark.db/test_dq_stats_table")


@pytest.mark.parametrize("df, "
                         "expectations, "
                         "rule_type, "
                         "row_dq_flag, "
                         "source_agg_dq_flag, "
                         "final_agg_dq_flag, "
                         "source_query_dq_flag, "
                         "final_query_dq_flag, "
                         "input_count, "
                         "error_count, "
                         "output_count,"
                         "write_to_table, "
                         "expected_df, "
                         "agg_or_query_dq_res, "
                         "status ",

                         [
                             (
                                     # Note: where err: refers error table and fnl: final table
                                     # test case 1
                                     # In this test case, the action for failed rows is "ignore",
                                     # so the function should return the input DataFrame with all rows.
                                     # collect stats in the test_stats_table and
                                     # log the error records into the error table.
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a"},
                                             # row doesn't meet expectations(ignore), log into err & fnl
                                             {"col1": 2, "col2": "b"},
                                             # row meets expectations(ignore), log into final table
                                             {"col1": 3, "col2": "c"},
                                             # row meets expectations(ignore), log into final table
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "col1 > 1",
                                             "action_if_failed": "ignore",
                                             "tag": "validity",
                                             "description": "col1 value must be greater than 1",
                                             "enable_error_drop_alert": True,
                                             "error_drop_threshold": "10",

                                         }],
                                         "agg_dq_rules": [{}],
                                     },
                                     "row_dq",
                                     True,  # row_dq_flag
                                     False,  # source_agg_dq_flag
                                     False,  # final_agg_dq_flag
                                     False,  # source_query_dq_flag
                                     False,  # final_query_dq_flag
                                     3,  # input df count
                                     1,  # input df error count
                                     3,  # output df count
                                     True,  # write to table
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a"},
                                             # row doesn't meet expectations(ignore), log into err & fnl
                                             {"col1": 2, "col2": "b"},
                                             # row meets expectations(ignore), log into final table
                                             {"col1": 3, "col2": "c"},
                                             # row meets expectations(ignore), log into final table
                                         ]
                                     ),
                                     None,  # expected agg dq result
                                     # status at different stages for input
                                     {"row_dq_status": "Passed", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"}
                             ),
                             (
                                     # test case 2
                                     # In this  test case, the action for failed rows is "drop",
                                     # so the function should return a filtered DataFrame with only the rows that
                                     # satisfy the rules.
                                     # collect stats in the test_stats_table and
                                     # log the error records into the error table.
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a"},
                                             # row doesn't meet expectations(drop), log into error
                                             {"col1": 2, "col2": "b"},
                                             # row meets expectations(drop), log into final table
                                             {"col1": 3, "col2": "c"},
                                             # row meets expectations(drop), log into final table
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "col1 > 1",
                                             "action_if_failed": "drop",
                                             "tag": "validity",
                                             "description": "col1 value must be greater than 1",
                                             "enable_error_drop_alert": False,
                                             "error_drop_threshold": "5",
                                         }]
                                     },
                                     "row_dq",
                                     True,  # row_dq_flag
                                     False,  # source_agg_dq_flag
                                     False,  # final_agg_dq_flag
                                     False,  # source_query_dq_results
                                     False,  # final_query_dq_results
                                     3,  # input count
                                     1,  # error count
                                     2,  # output count
                                     False,  # write to table
                                     spark.createDataFrame([  # expected_output
                                         # row meets the set expectations
                                         {"col1": 2, "col2": "b"},
                                         # row meets the set expectations
                                         {"col1": 3, "col2": "c"}
                                     ]),
                                     None,  # expected agg dq result
                                     # status at different stages for input
                                     {"row_dq_status": "Passed", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"}
                             ),
                             (
                                     # test case 3
                                     # In this test case, the action for failed rows is "fail",
                                     # so the function should raise an error.
                                     # collect stats in the test_stats_table and log the error records into the error
                                     # table.
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a"},
                                             # row doesn't meet expectations(fail),log into error and raise error
                                             {"col1": 2, "col2": "b"},  # row meet expectations(fail)
                                             {"col1": 3, "col2": "c"},  # row meet expectations(fail)
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "col1 > 1",
                                             "action_if_failed": "fail",
                                             "tag": "validity",
                                             "description": "col1 value must be greater than 1",
                                             "enable_error_drop_alert": False,
                                             "error_drop_threshold": "3",
                                         }],
                                     },
                                     "row_dq",  # type of rule
                                     True,  # row_dq_flag
                                     False,  # source_agg_dq_flag
                                     False,  # final_agg_dq_flag
                                     False,  # source_query_dq_flag
                                     False,  # final_query_dq_flag
                                     3,  # input count
                                     1,  # error count
                                     0,  # output count
                                     True,  # write to table
                                     SparkExpectationsMiscException,  # expected output
                                     None,  # expected agg dq result
                                     # status at different stages for given input
                                     {"row_dq_status": "Failed", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Failed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"}

                             ),

                             (  # test case 4
                                     # In this  test case, the action for failed rows is "ignore" and "drop",
                                     # so the function should return a filtered DataFrame
                                     # with only the rows that satisfy the rules,
                                     # collect stats in the test_stats_table and
                                     # log the error records into the error table
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a"},
                                             # row doesn't meet expectation1(ignore), log err and fnl
                                             {"col1": 2, "col2": "b"},
                                             # row meets both expectations, logged into final table
                                             {"col1": 3, "col2": "c"},
                                             # row meets both expectations, logged into final table
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col1_threshold_1",
                                             "column_name": "col1",
                                             "expectation": "col1 > 1",
                                             "action_if_failed": "ignore",
                                             "tag": "validity",
                                             "description": "col1 value must be greater than 1",
                                             "enable_error_drop_alert": False,
                                             "error_drop_threshold": "1",
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "table_name": "dq_spark.test_table",
                                                 "rule_type": "row_dq",
                                                 "rule": "col2_set",
                                                 "column_name": "col2",
                                                 "expectation": "col2 in ('a', 'b', 'c')",
                                                 "action_if_failed": "drop",
                                                 "tag": "validity",
                                                 "description": "col1 value must be greater than 2"
                                             }],
                                     },
                                     "row_dq",  # type of rule
                                     True,  # row_dq_flag
                                     False,  # source_agg_dq_flag
                                     False,  # final_agg_dq_flag
                                     False,  # source_query_dq_results
                                     False,  # final_query_dq_results
                                     3,  # input count
                                     1,  # error count
                                     3,  # output count
                                     True,  # write to table
                                     spark.createDataFrame([  # expected_output
                                         {"col1": 1, "col2": "a"},
                                         {"col1": 2, "col2": "b"},
                                         {"col1": 3, "col2": "c"}
                                     ]),
                                     None,  # expected agg dq result
                                     # status at different stages for given input
                                     {"row_dq_status": "Passed", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"}
                             ),
                             (
                                     # test case 5
                                     # In this test case, the action for failed rows is "ignore" and "fail"
                                     # Includes row and source_agg data quality checks
                                     # so the function should raise an error. Collect stats in the test_stats_table and
                                     # log the error records into the error table.
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a"},
                                             # row doesnt meet expectation1(ignore),log into err & fnl
                                             {"col1": 2, "col2": "b"},
                                             # row meets both expectations, log into final table
                                             {"col1": 3, "col2": "c"},
                                             # row doesnt meet expectation2(fail),log into err & raise err

                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col1_threshold_1",
                                             "column_name": "col1",
                                             "expectation": "col1 > 1",
                                             "action_if_failed": "ignore",
                                             "tag": "validity",
                                             "description": "col1 value must be greater than 1",
                                             "enable_error_drop_alert": True,
                                             "error_drop_threshold": "5",
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "table_name": "dq_spark.test_table",
                                                 "rule_type": "row_dq",
                                                 "rule": "col2_set",
                                                 "column_name": "col2",
                                                 "expectation": "col2 in ('a', 'b')",
                                                 "action_if_failed": "fail",
                                                 "tag": "validity",
                                                 "description": "col1 value must be greater than 3",
                                                 "enable_error_drop_alert": True,
                                                 "error_drop_threshold": "10",
                                             }],
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "sum(col1) > 10",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "ignore",
                                             "tag": "validity",
                                             "description": "sum of col1 value must be greater than 10"

                                         }]
                                     },
                                     "row_dq",  # type of rule
                                     True,  # row_dq_flag
                                     False,  # source_agg_dq_flag
                                     False,  # final_agg-dq_flag
                                     # source_query_dq_flag
                                     False,
                                     False,  # final_query_dq_flag
                                     3,  # input count
                                     2,  # error count
                                     0,  # output count
                                     False,  # write to table
                                     SparkExpectationsMiscException,  # exception because dq set to fail
                                     None,  # expected agg dq result
                                     # status at different stages for given input
                                     {"row_dq_status": "Failed", "source_agg_dq_status": "Passed",
                                      "final_agg_dq_status": "Skipped", "run_status": "Failed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"}
                             ),
                             (
                                     # test case 6
                                     # In this test case, the action for failed rows is "drop" and "fail",
                                     # Includes source agg and row data quality checks
                                     # so the function should return a filtered DataFrame
                                     # with only the rows that satisfy the rules,
                                     # collect stats in the test_stats_table and
                                     # log the error records into the error table.
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a"},
                                             # row doesnt meet expectation2 (drop), log into error
                                             {"col1": 2, "col2": "b"},
                                             # row doesn't meets expectations1(drop), log into error
                                             {"col1": 3, "col2": "c"},
                                             # row meets all expectations, log into final table
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [
                                             {
                                                 "product_id": "product1",
                                                 "table_name": "dq_spark.test_table",
                                                 "rule_type": "row_dq",
                                                 "rule": "col1_threshold",
                                                 "column_name": "col1",
                                                 "expectation": "col1 > 2",
                                                 "action_if_failed": "drop",
                                                 "tag": "validity",
                                                 "description": "col1 value must be greater than 1",
                                                 "enable_error_drop_alert": False,
                                                 "error_drop_threshold": "5",
                                             },
                                             {
                                                 "product_id": "product1",
                                                 "table_name": "dq_spark.test_table",
                                                 "rule_type": "row_dq",
                                                 "rule": "col2_set",
                                                 "column_name": "col2",
                                                 "expectation": "col2 in ('a', 'b', 'c')",
                                                 "action_if_failed": "fail",
                                                 "tag": "validity",
                                                 "description": "col1 value must be greater than 3",
                                                 "enable_error_drop_alert": True,
                                                 "error_drop_threshold": "5",
                                             }],
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "sum(col1) > 10",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "ignore",
                                             "tag": "validity",
                                             "description": "sum of col1 value must be greater than 10"

                                         }]
                                     },
                                     "row_dq",  # type of rule
                                     True,  # row_dq_flag
                                     False,  # source_agg_dq_flag
                                     False,  # final_agg_dq_flag
                                     # source_query_dq_flag
                                     False,
                                     False,  # final_query_dq_flag
                                     3,  # input count
                                     2,  # error count
                                     1,  # output count
                                     True,  # write to table
                                     spark.createDataFrame(  # expected_output
                                         [
                                             {"col1": 3, "col2": "c"},
                                         ]
                                     ),
                                     None,  # expected agg dq result
                                     # status at different stages for given input
                                     {"row_dq_status": "Passed", "source_agg_dq_status": "Passed",
                                      "final_agg_dq_status": "Skipped", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"}
                             ),

                             (
                                     # test case 7
                                     # In this test case, the action for failed rows is "ignore", "drop" and "fail",
                                     # so the function should return a filtered DataFrame
                                     # with only the rows that satisfy the rules,
                                     # collect stats in the test_stats_table and
                                     # log the error records into the error table.
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             # row doesnt meet expectation2 (drop), log into error
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             # row doesn't meets expectations2(drop), log into error
                                             {"col1": 3, "col2": "c", "col3": 6},
                                             # row meets all expectations, log into final table
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col3_threshold",
                                             "column_name": "col3",
                                             "expectation": "col3 > 1",
                                             "action_if_failed": "ignore",
                                             "tag": "validity",
                                             "description": "col1 value must be greater than 1",
                                             "enable_error_drop_alert": False,
                                             "error_drop_threshold": "1",
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "table_name": "dq_spark.test_table",
                                                 "rule_type": "row_dq",
                                                 "rule": "col1_threshold",
                                                 "column_name": "col1",
                                                 "expectation": "col1 > 2",
                                                 "action_if_failed": "drop",
                                                 "tag": "validity",
                                                 "description": "col1 value must be greater than 1",
                                                 "enable_error_drop_alert": True,
                                                 "error_drop_threshold": "10",
                                             },
                                             {
                                                 "product_id": "product1",
                                                 "table_name": "dq_spark.test_table",
                                                 "rule_type": "row_dq",
                                                 "rule": "col2_set",
                                                 "column_name": "col2",
                                                 "expectation": "col2 in ('a', 'b', 'c')",
                                                 "action_if_failed": "fail",
                                                 "tag": "validity",
                                                 "description": "col1 value must be greater than 3",
                                                 "enable_error_drop_alert": False,
                                                 "error_drop_threshold": "10",
                                             }]
                                     },
                                     "row_dq",  # type of rule
                                     True,  # row_dq_flag
                                     False,  # source_agg_dq_flag
                                     False,  # final_agg_dq_flag
                                     False,  # source_query_dq_results
                                     False,  # final_query_dq_results
                                     3,  # input count
                                     2,  # error count
                                     1,  # output count
                                     True,  # write to table
                                     spark.createDataFrame(  # expected_output
                                         [
                                             {"col1": 3, "col2": "c", "col3": 6},
                                         ]
                                     ),
                                     None,  # expected agg dq result
                                     # status at different stages for given input
                                     {"row_dq_status": "Passed", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Failed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"}
                             ),
                             (
                                     # test case 8
                                     # In this test case, the action for failed agg dq is "ignore" on source data,
                                     # function should return an empty dataframe with no columns,
                                     # returns status of the flow
                                     spark.createDataFrame(
                                         [
                                             # agg expectations meets on the given source data
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             {"col1": 3, "col2": "c", "col3": 6},
                                         ]
                                     ),
                                     {
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "sum_col3_threshold",
                                             "column_name": "col3",
                                             "expectation": "sum(col3) > 1",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "ignore",
                                             "tag": "validity",
                                             "description": "sum of col3 value must be greater than 1"
                                         }
                                         ]},
                                     "agg_dq",  # type of rule
                                     False,  # row_dq_flag
                                     True,  # source_agg_dq_flag
                                     False,  # final_agg_dq_flag
                                     False,  # source_query_dq_flag
                                     False,  # final_query_dq_results
                                     3,  # input count
                                     0,  # error count
                                     0,  # output count
                                     True,  # write to table
                                     # expected df
                                     spark.createDataFrame([{"col1": 1}]).drop("col1"),
                                     None,  # expected agg dq result
                                     # status at different stages for given input
                                     {"row_dq_status": "Skipped", "source_agg_dq_status": "Passed",
                                      "final_agg_dq_status": "Skipped", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"}
                             ),
                             (
                                     # test case 9
                                     # In this test case, the action for failed agg dq is "fail" on source data,
                                     # function raises error due to failed expectations with action "fail",
                                     # log the stats into the stats table
                                     spark.createDataFrame(
                                         [  # agg expectations doesn't meet on source data, raise error
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             {"col1": 3, "col2": "c", "col3": 6},

                                         ]
                                     ),
                                     {
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "sum_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "sum(col1) > 20",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "fail",
                                             "tag": "validity",
                                             "description": "sum of col1 value must be greater than 1"
                                         }
                                         ]},
                                     "agg_dq",  # type of rule
                                     False,  # row_dq_flag
                                     True,  # source_agg_dq_flag
                                     False,  # final_agg_dq_flag
                                     False,  # source_query_dq_flag
                                     False,  # final_query_dq_flag
                                     3,  # input count
                                     0,  # error count
                                     0,  # output count
                                     True,  # write to table
                                     # expected res
                                     SparkExpectationsMiscException,
                                     None,  # expected agg dq result
                                     # status at different stages for given input
                                     {"row_dq_status": "Skipped", "source_agg_dq_status": "Failed",
                                      "final_agg_dq_status": "Skipped", "run_status": "Failed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"}
                             ),
                             (
                                     # test case 10
                                     # In this test case, the action for failed
                                     # agg dq is "ignore" on source dataset,
                                     # function returns final dq result set for failed expectations,
                                     # log the stats into the stats table
                                     spark.createDataFrame(
                                         [
                                             # given dataset doesn't meet source
                                             # agg data quality check for action failed
                                             # 'ignore' and return result set
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             {"col1": 3, "col2": "c", "col3": 6},
                                         ]
                                     ),
                                     {
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "distinct_col2_threshold",
                                             "column_name": "col2",
                                             "expectation": "count(distinct col2) > 4",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "ignore",
                                             "tag": "validity",
                                             "description": "distinct of col2 value must be greater than 4"
                                         }
                                         ]},
                                     "agg_dq",  # type of rule
                                     False,  # row_dq_flag
                                     True,  # source_agg_dq_flag
                                     False,  # final_agg_dq_flag
                                     False,  # source_query_dq_flag
                                     False,  # final_query_dq_flag
                                     3,  # input count
                                     0,  # error count
                                     0,  # output count
                                     True,  # write to table
                                     None,  # expected res
                                     # expected agg dq result
                                     [{"description": "distinct of col2 value must be greater than 4",
                                       "rule": "distinct_col2_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "ignore", "tag": "validity"}],
                                     # status at different stages for given input
                                     {"row_dq_status": "Skipped", "source_agg_dq_status": "Passed",
                                      "final_agg_dq_status": "Skipped", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"}
                             ),
                             # test case 11
                             # In this test case, the action for failed agg dq is "ignore" on final dataset,
                             # there no failed expectations, function returns final dq result set with None,
                             # log the stats into the stats table
                             (
                                     spark.createDataFrame(
                                         [  # data set meets the final_agg data quality checks for action_if_failed
                                             # 'ignore' and returns result set with None
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             {"col1": 3, "col2": "c", "col3": 6},
                                         ]
                                     ),
                                     {
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "min_col3_threshold",
                                             "column_name": "col3",
                                             "expectation": "min(col3) > 1",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "ignore",
                                             "tag": "validity",
                                             "description": "min of col3 value must be greater than 1"
                                         }
                                         ]},
                                     "agg_dq",  # type of rule
                                     False,  # row_dq_flag
                                     False,  # source_agg_dq_flag
                                     True,  # final_agg_dq_flag
                                     # source_query_dq_results
                                     False,
                                     False,  # final_query_dq_flag
                                     3,  # input count
                                     0,  # error count
                                     0,  # output count
                                     True,  # write to table
                                     None,  # expected res df
                                     None,  # expected agg dq result
                                     # status at different stages for given input
                                     {"row_dq_status": "Skipped", "source_agg_dq_status": "Passed",
                                      "final_agg_dq_status": "Passed", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"}
                             ),
                             (
                                     # test case 12
                                     # In this test case, the action for failed agg dq is "fail" on final dataset,
                                     # there are failed expectation for action "fail", function raises err
                                     # log the stats into the stats table
                                     spark.createDataFrame(
                                         [
                                             # given datset doesn't meet the agg dq expectations
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             {"col1": 3, "col2": "c", "col3": 6},
                                         ]
                                     ),
                                     {
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "max_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "max(col1) > 100",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "fail",
                                             "tag": "validity",
                                             "description": "max of col1 value must be greater than 100"
                                         }
                                         ]},
                                     "agg_dq",  # type of rule
                                     False,  # row_dq_flag
                                     False,  # source_agg_dq_flag
                                     True,  # final_agg_dq_flag
                                     False,  # source_query_dq_flag
                                     False,  # final_query_dq_flag
                                     3,  # input count
                                     1,  # error count
                                     2,  # output count
                                     True,  # write to table
                                     SparkExpectationsMiscException,  # expected res
                                     None,  # expected agg dq result
                                     # status at different stages for given input
                                     {"row_dq_status": "Skipped", "source_agg_dq_status": "Passed",
                                      "final_agg_dq_status": "Failed", "run_status": "Failed"}
                             ),
                             (
                                     # test case 13
                                     # In this test case, the action for failed agg dq is "ignore" on final dataset,
                                     # function returns agg_result_set for failed expectations
                                     # log the stats into the stats table
                                     spark.createDataFrame(
                                         [
                                             # given datset doesn't meet agg data quality expectations, returns agg_res
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             {"col1": 3, "col2": "c", "col3": 6},
                                         ]
                                     ),
                                     {
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "avg_col2_threshold",
                                             "column_name": "col2",
                                             "expectation": "avg(col1) > 25",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "ignore",
                                             "tag": "validity",
                                             "description": "average of col1 value must be greater than 25"
                                         }
                                         ]},
                                     "agg_dq",  # type of rule
                                     False,  # row_dq_flag
                                     False,  # source_agg_dq_flag
                                     True,  # final_agg_dq_flag
                                     False,  # source_query_dq_flag
                                     False,  # final_query_dq_flag
                                     3,  # input count
                                     1,  # error count
                                     3,  # output count
                                     True,  # write to table
                                     None,  # expected res
                                     # expected agg dq result
                                     [{"description": "average of col1 value must be greater than 25",
                                       "rule": "avg_col2_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "ignore", "tag": "validity"}],
                                     # status at different stages for given input
                                     {"row_dq_status": "Skipped", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Passed", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"}
                             ),

                             (
                                     # test case 14
                                     # In this test case, the action for failed
                                     # agg dq is "ignore" on source dataset,
                                     # function returns agg_result_set for failed expectations
                                     # log the stats into the stats table
                                     spark.createDataFrame(
                                         [
                                             # given datset doesn't meet all agg data quality expectations
                                             # for source dataset, return agg_res set
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             {"col1": 3, "col2": "c", "col3": 6},
                                         ]
                                     ),
                                     {
                                         # expectations
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "avg_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "avg(col1) > 25",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "ignore",
                                             "tag": "validity",
                                             "description": "average of col1 value must be greater than 25"
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "table_name": "dq_spark.test_table",
                                                 "rule_type": "agg_dq",
                                                 "rule": "min_col3_threshold",
                                                 "column_name": "col3",
                                                 "expectation": "min(col3) > 15",
                                                 "enable_for_source_dq_validation": True,
                                                 "enable_for_target_dq_validation": True,
                                                 "action_if_failed": "ignore",
                                                 "tag": "accuracy",
                                                 "description": "min of col3 value must be greater than 15"
                                             },
                                             {
                                                 "product_id": "product1",
                                                 "table_name": "dq_spark.test_table",
                                                 "rule_type": "agg_dq",
                                                 "rule": "count_col2_threshold",
                                                 "column_name": "col2",
                                                 "expectation": "count(distinct col2) > 5",
                                                 "enable_for_source_dq_validation": True,
                                                 "enable_for_target_dq_validation": True,
                                                 "action_if_failed": "ignore",
                                                 "tag": "validity",
                                                 "description": "distinct count of col2 value must be greater than 5"
                                             }
                                         ]},
                                     "agg_dq",  # type of rule
                                     False,  # row_dq_flag
                                     True,  # source_agg_dq_flag
                                     False,  # final_agg_dq_flag
                                     False,  # source_query_dq_flag
                                     False,  # final_query_dq_flag
                                     3,  # input count
                                     1,  # error count
                                     3,  # output count
                                     True,  # write to table
                                     None,  # expected res
                                     # expected agg dq result
                                     [{"description": "average of col1 value must be greater than 25",
                                       "rule": "avg_col1_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "ignore", "tag": "validity"},
                                      {"description": "min of col3 value must be greater than 15",
                                       "rule": "min_col3_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "ignore", "tag": "accuracy"},
                                      {"description": "distinct count of col2 value must be greater than 5",
                                       "rule": "count_col2_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "ignore", "tag": "validity"}
                                      ],
                                     # status at different stages for given input
                                     {"row_dq_status": "Skipped", "source_agg_dq_status": "Passed",
                                      "final_agg_dq_status": "Skipped", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"}
                             ),
                             (
                                     # test case 15
                                     # In this test case, the action for failed agg dq is "ignore" on final dataset,
                                     # function returns agg_result_set for failed expectations
                                     # log the stats into the stats table
                                     spark.createDataFrame(
                                         [
                                             # given datset doesn't meet all agg data quality expectations
                                             # for source dataset, function returns agg_res set
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             {"col1": 3, "col2": "c", "col3": 6},
                                         ]
                                     ),
                                     {
                                         # expectations
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "avg_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "avg(col1) > 25",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "ignore",
                                             "tag": "validity",
                                             "description": "average of col1 value must be greater than 25"
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "table_name": "dq_spark.test_table",
                                                 "rule_type": "agg_dq",
                                                 "rule": "min_col3_threshold",
                                                 "column_name": "col3",
                                                 "expectation": "min(col3) > 15",
                                                 "enable_for_source_dq_validation": True,
                                                 "enable_for_target_dq_validation": True,
                                                 "action_if_failed": "ignore",
                                                 "tag": "validity",
                                                 "description": "min of col3 value must be greater than 15"
                                             },
                                             {
                                                 "product_id": "product1",
                                                 "table_name": "dq_spark.test_table",
                                                 "rule_type": "agg_dq",
                                                 "rule": "count_col2_threshold",
                                                 "column_name": "col2",
                                                 "expectation": "count(distinct col2) > 5",
                                                 "enable_for_source_dq_validation": True,
                                                 "enable_for_target_dq_validation": True,
                                                 "action_if_failed": "ignore",
                                                 "tag": "accuracy",
                                                 "description": "distinct count of col2 value must be greater than 5"
                                             }
                                         ]},
                                     "agg_dq",  # type of rule
                                     False,  # row_dq_flag
                                     False,  # source_agg_dq_flag
                                     True,  # final_agg_dq_flag
                                     # source_query_dq_flag
                                     False,
                                     False,  # final_query_dq_flag
                                     3,  # input count
                                     1,  # error count
                                     3,  # output count
                                     True,  # write to table
                                     None,  # expected res
                                     # expected agg dq result
                                     [{"description": "average of col1 value must be greater than 25",
                                       "rule": "avg_col1_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "ignore", "tag": "validity"},
                                      {"description": "min of col3 value must be greater than 15",
                                       "rule": "min_col3_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "ignore", "tag": "validity"},
                                      {"description": "distinct count of col2 value must be greater than 5",
                                       "rule": "count_col2_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "ignore", "tag": "accuracy"}
                                      ],
                                     # status at different stages for given input
                                     {"row_dq_status": "Skipped", "source_agg_dq_status": "Passed",
                                      "final_agg_dq_status": "Passed", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"}
                             ),
                             (
                                     # test case 16
                                     # In this test case, the action for failed
                                     # agg dq is "ignore" , "fail" on source dataset,
                                     # given dataset doesn't meet expectation 1 & 2, function raises err
                                     # log the stats into the stats table
                                     spark.createDataFrame(
                                         [
                                             # given dataset doesn't meet the expectation 1 & 2
                                             # which consist action_if_failed "ignore" & "fail"
                                             # so, function raises error
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             {"col1": 3, "col2": "c", "col3": 6},
                                         ]
                                     ),
                                     {
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "avg_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "avg(col1) > 25",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "fail",
                                             "tag": "validity",
                                             "description": "average of col1 value must be greater than 25"
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "table_name": "dq_spark.test_table",
                                                 "rule_type": "agg_dq",
                                                 "rule": "min_col3_threshold",
                                                 "column_name": "col3",
                                                 "expectation": "min(col3) > 15",
                                                 "enable_for_source_dq_validation": True,
                                                 "enable_for_target_dq_validation": True,
                                                 "action_if_failed": "ignore",
                                                 "tag": "validity",
                                                 "description": "min of col3 value must be greater than 15"
                                             },
                                             {
                                                 "product_id": "product1",
                                                 "table_name": "dq_spark.test_table",
                                                 "rule_type": "agg_dq",
                                                 "rule": "count_col2_threshold",
                                                 "column_name": "col2",
                                                 "expectation": "count(distinct col2) > 2",
                                                 "enable_for_source_dq_validation": True,
                                                 "enable_for_target_dq_validation": True,
                                                 "action_if_failed": "fail",
                                                 "tag": "validity",
                                                 "description": "distinct count of col2 value must be greater than 2"
                                             }
                                         ]},
                                     "agg_dq",  # type of rule
                                     False,  # row_dq_flag
                                     True,  # source_agg_dq_flag
                                     False,  # final_agg_dq_flag
                                     False,  # source_query_dq_flag
                                     False,  # final_query_dq_flag
                                     3,  # input count
                                     1,  # error count
                                     3,  # output count
                                     True,  # write to table
                                     SparkExpectationsMiscException,  # expected res
                                     # expected agg dq result
                                     [{"description": "average of col1 value must be greater than 25",
                                       "rule": "avg_col1_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "ignore", "tag": "validity"},
                                      {"description": "min of col3 value must be greater than 15",
                                       "rule": "min_col3_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "ignore", "tag": "validity"}
                                      ],
                                     # status at different stages for given input
                                     {"row_dq_status": "Skipped", "source_agg_dq_status": "Failed",
                                      "final_agg_dq_status": "Skipped", "run_status": "Failed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"}
                             ),
                             (
                                     # test case 17
                                     # In this test case, the action for failed
                                     # agg dq is "ignore" , "fail" on source dataset,
                                     # given datset meets all expectations, function returns agg_res with None
                                     # log the stats into the stats table
                                     spark.createDataFrame(
                                         [
                                             # given datset meets all the expectations
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             {"col1": 3, "col2": "c", "col3": 6},
                                         ]
                                     ),
                                     {
                                         # expectations
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "avg_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "avg(col1) < 25",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "fail",
                                             "tag": "validity",
                                             "description": "average of col1 value must be less than 25"
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "table_name": "dq_spark.test_table",
                                                 "rule_type": "agg_dq",
                                                 "rule": "min_col3_threshold",
                                                 "column_name": "col3",
                                                 "expectation": "min(col3) < 15",
                                                 "enable_for_source_dq_validation": True,
                                                 "enable_for_target_dq_validation": True,
                                                 "action_if_failed": "ignore",
                                                 "tag": "validity",
                                                 "description": "min of col3 value must be less than 15"
                                             },
                                             {
                                                 "product_id": "product1",
                                                 "table_name": "dq_spark.test_table",
                                                 "rule_type": "agg_dq",
                                                 "rule": "count_col2_threshold",
                                                 "column_name": "col2",
                                                 "expectation": "count(distinct col2) > 2",
                                                 "enable_for_source_dq_validation": True,
                                                 "enable_for_target_dq_validation": True,
                                                 "action_if_failed": "fail",
                                                 "tag": "validity",
                                                 "description": "distinct count of col2 value must be greater than 2"
                                             }
                                         ]},
                                     "agg_dq",  # type of rule
                                     False,  # row_dq_flag
                                     False,  # source_agg_dq_flag
                                     True,  # final_agg_dq_flag
                                     False,  # source_query_dq_flag
                                     False,  # final_query_dq_flag
                                     3,  # input count
                                     1,  # error count
                                     3,  # output count
                                     True,  # write to table
                                     None,  # expected res
                                     None,  # expected agg dq result
                                     # status at different stages for given input
                                     {"row_dq_status": "Skipped", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Passed", "run_status": "Passed"}
                             ),
                             (
                                     # test case 18
                                     # In this test case, the action for failed agg dq is "fail" on final dataset,
                                     # function raises error, due to dataset doesn't meet expectation1
                                     # log the stats into the stats table
                                     spark.createDataFrame(
                                         [
                                             # given dataset meets expectation2(fail) and
                                             # doesn't meet expectations1(fail),
                                             # expectations1 set to fail, raise error
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             {"col1": 3, "col2": "c", "col3": 6},

                                         ]
                                     ),
                                     {
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "avg_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "avg(col1) > 25",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "fail",
                                             "tag": "validity",
                                             "description": "average of col1 value must be greater than 25"
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "table_name": "dq_spark.test_table",
                                                 "rule_type": "agg_dq",
                                                 "rule": "count_col2_threshold",
                                                 "column_name": "col2",
                                                 "expectation": "count(distinct col2) > 2",
                                                 "enable_for_source_dq_validation": True,
                                                 "enable_for_target_dq_validation": True,
                                                 "action_if_failed": "fail",
                                                 "tag": "validity",
                                                 "description": "distinct count of col2 value must be greater than 2"
                                             }
                                         ]},
                                     "agg_dq",  # type of rule
                                     False,  # row_dq_flag
                                     False,  # source_agg_dq_flag
                                     True,  # final_agg_dq_flag
                                     False,  # source_query_dq_flag
                                     False,  # final_query_dq_flag
                                     3,  # input count
                                     1,  # error count
                                     3,  # output count
                                     True,  # write to table
                                     SparkExpectationsMiscException,  # expected res
                                     # expected agg dq result
                                     [{"description": "average of col1 value must be greater than 25",
                                       "rule": "avg_col1_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "fail", "tag": "validity"},
                                      ],
                                     # status at different stages for given input
                                     {"row_dq_status": "Skipped", "source_agg_dq_status": "Failed",
                                      "final_agg_dq_status": "Skipped", "run_status": "Failed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"}
                             ),
                             (
                                     # test case 19
                                     # In this test case, the action for failed query dq is "ignore" on source dataset,
                                     # function captures  error, due to dataset doesn't meet expectation1
                                     # log the stats into the stats table
                                     spark.createDataFrame(
                                         [
                                             # given dataset meets expectation2(ignore) and
                                             # doesn't meet expectations1(=ignore),
                                             # expectations1 set to fail, captures error
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             {"col1": 3, "col2": "c", "col3": 6},

                                         ]
                                     ),
                                     {
                                         "query_dq_rules": [
                                             {
                                                 "product_id": "product1",
                                                 "table_name": "dq_spark.test_table",
                                                 "rule_type": "agg_dq",
                                                 "rule": "count_col2_threshold",
                                                 "column_name": "col2",
                                                 "expectation": "(select count(distinct col2) from test_table) > 2",
                                                 "enable_for_source_dq_validation": True,
                                                 "enable_for_target_dq_validation": True,
                                                 "action_if_failed": "ignore",
                                                 "tag": "validity",
                                                 "description": "distinct count of col2 value must be greater than 2"
                                             }
                                         ]},
                                     "query_dq",  # type of rule
                                     False,  # row_dq_flag
                                     False,  # source_agg_dq_flag
                                     False,  # final_agg_dq_flag
                                     True,  # source_query_dq_flag
                                     False,  # final_query_dq_flag
                                     3,  # input count
                                     1,  # error count
                                     3,  # output count
                                     True,  # write to table
                                     None,  # expected res
                                     # expected agg / query dq result
                                     None,
                                     # status at different stages for given input
                                     {"row_dq_status": "Skipped", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Passed",
                                      "source_query_dq_status": "Passed", "final_query_dq_status": "Skipped"}
                             ),

                             (  # test case 20
                                     # In this test case, the action for failed query dq is "fail" on source dataset,
                                     # function raises error, due to dataset doesn't meet expectation1
                                     # log the stats into the stats table
                                     spark.createDataFrame(
                                         [
                                             # given dataset meets expectation2(ignore) and
                                             # doesn't meet expectations1(fail),
                                             # expectations1 set to fail, raise error
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             {"col1": 3, "col2": "c", "col3": 6},

                                         ]
                                     ),
                                     {
                                         "query_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "query_dq",
                                             "rule": "avg_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "(select sum(col1) from test_table) > 25",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "fail",
                                             "tag": "validity",
                                             "description": "sum of col1 value must be greater than 25"
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "table_name": "dq_spark.test_table",
                                                 "rule_type": "agg_dq",
                                                 "rule": "count_col2_threshold",
                                                 "column_name": "col2",
                                                 "expectation": "(select count(distinct col2) from test_table) > 2",
                                                 "enable_for_source_dq_validation": True,
                                                 "enable_for_target_dq_validation": True,
                                                 "action_if_failed": "ignore",
                                                 "tag": "validity",
                                                 "description": "distinct count of col2 value must be greater than 2"
                                             }
                                         ]},
                                     "query_dq",  # type of rule
                                     False,  # row_dq_flag
                                     False,  # source_agg_dq_flag
                                     False,  # final_agg_dq_flag
                                     True,  # source_query_dq_flag
                                     False,  # final_query_dq_flag
                                     3,  # input count
                                     1,  # error count
                                     3,  # output count
                                     True,  # write to table
                                     SparkExpectationsMiscException,  # expected res
                                     # expected agg / query dq result
                                     [{"description": "sum of col1 value must be greater than 25",
                                       "rule": "avg_col1_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "fail", "tag": "validity"},
                                      ],
                                     # status at different stages for given input
                                     {"row_dq_status": "Skipped", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Failed",
                                      "source_query_dq_status": "Failed", "final_query_dq_status": "Skipped"}
                             ),

                             (  # test case 21
                                     # In this test case, the action for failed query dq is "ignore" on final dataset,
                                     # function captures error, due to dataset doesn't meet expectation1
                                     # log the stats into the stats table
                                     spark.createDataFrame(
                                         [
                                             # doesn't meet expectations1(ignore),
                                             # expectations1 set to ignore, captures error
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             {"col1": 3, "col2": "c", "col3": 6},

                                         ]
                                     ),
                                     {
                                         "query_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "query_dq",
                                             "rule": "avg_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "(select avg(col3) from test_table) > 50",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "ignore",
                                             "tag": "validity",
                                             "description": "avg of col3 value must be greater than 50"
                                         }
                                         ]},
                                     "query_dq",  # type of rule
                                     False,  # row_dq_flag
                                     False,  # source_agg_dq_flag
                                     False,  # final_agg_dq_flag
                                     False,  # source_query_dq_flag
                                     True,  # final_query_dq_flag
                                     3,  # input count
                                     1,  # error count
                                     3,  # output count
                                     True,  # write to table
                                     None,  # expected res
                                     # expected agg / query dq result
                                     [{"description": "avg of col3 value must be greater than 50",
                                       "rule": "avg_col1_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "ignore", "tag": "validity"},
                                      ],
                                     # status at different stages for given input
                                     {"row_dq_status": "Skipped", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Passed"}
                             ),
                             (  # test case 22
                                     # In this test case, the action for failed query dq is "fail" on final dataset,
                                     # function captures error, due to dataset doesn't meet expectation1
                                     # log the stats into the stats table
                                     spark.createDataFrame(
                                         [
                                             # given dataset meets expectation2(fail) and
                                             # doesn't meet expectations1(fail),
                                             # expectations1 set to fail, raise error
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             {"col1": 3, "col2": "c", "col3": 6},

                                         ]
                                     ),
                                     {
                                         "query_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "query_dq",
                                             "rule": "avg_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "(select avg(col3) from test_table) > 50",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "ignore",
                                             "tag": "validity",
                                             "description": "avg of col3 value must be greater than 50"
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "table_name": "dq_spark.test_table",
                                                 "rule_type": "agg_dq",
                                                 "rule": "count_col2_threshold",
                                                 "column_name": "col2",
                                                 "expectation": "(select sum(distinct col1) from test_table) > 2",
                                                 "enable_for_source_dq_validation": True,
                                                 "enable_for_target_dq_validation": True,
                                                 "action_if_failed": "ignore",
                                                 "tag": "validity",
                                                 "description": "sum of col1 value must be greater than 2"
                                             }
                                         ],
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "avg_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "avg(col1) > 25",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "fail",
                                             "tag": "validity",
                                             "description": "average of col1 value must be greater than 25"
                                         }]},
                                     "query_dq",  # type of rule
                                     False,  # row_dq_flag
                                     False,  # source_agg_dq_flag
                                     False,  # final_agg_dq_flag
                                     False,  # source_query_dq_flag
                                     True,  # final_query_dq_flag
                                     3,  # input count
                                     1,  # error count
                                     3,  # output count
                                     True,  # write to table
                                     None,  # expected res
                                     # expected agg / query dq result
                                     [{"description": "avg of col3 value must be greater than 50",
                                       "rule": "avg_col1_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "ignore", "tag": "validity"},
                                      ],
                                     # status at different stages for given input
                                     {"row_dq_status": "Skipped", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Passed"}
                             ),
                             (  # test case 23
                                     # In this test case, the action for failed query dq is "fail" on final dataset,
                                     # function captures error, due to dataset doesn't meet expectation1
                                     # log the stats into the stats table
                                     spark.createDataFrame(
                                         [
                                             # given dataset meets expectation2(fail) and
                                             # doesn't meet expectations1(fail),
                                             # expectations1 set to fail, raise error
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             {"col1": 3, "col2": "c", "col3": 6},

                                         ]
                                     ),
                                     {
                                         "query_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "query_dq",
                                             "rule": "avg_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "(select avg(col3) from test_table) > 50",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "fail",
                                             "tag": "validity",
                                             "description": "avg of col3 value must be greater than 50"
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "table_name": "dq_spark.test_table",
                                                 "rule_type": "agg_dq",
                                                 "rule": "count_col2_threshold",
                                                 "column_name": "col2",
                                                 "expectation": "(select sum(distinct col1) from test_table) > 2",
                                                 "enable_for_source_dq_validation": True,
                                                 "enable_for_target_dq_validation": True,
                                                 "action_if_failed": "fail",
                                                 "tag": "validity",
                                                 "description": "sum of col1 value must be greater than 2"
                                             }
                                         ],
                                         "agg_dq_rules": [{}],
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col1_threshold_1",
                                             "column_name": "col1",
                                             "expectation": "col1 > 1",
                                             "action_if_failed": "ignore",
                                             "tag": "validity",
                                             "description": "col1 value must be greater than 1"
                                         }]},
                                     "query_dq",  # type of rule
                                     False,  # row_dq_flag
                                     False,  # source_agg_dq_flag
                                     False,  # final_agg_dq_flag
                                     False,  # source_query_dq_flag
                                     True,  # final_query_dq_flag
                                     3,  # input count
                                     1,  # error count
                                     3,  # output count
                                     True,  # write to table
                                     SparkExpectationsMiscException,  # expected res
                                     # expected agg / query dq result
                                     [{"description": "avg of col3 value must be greater than 50",
                                       "rule": "avg_col1_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "fail", "tag": "validity"},
                                      ],
                                     # status at different stages for given input
                                     {"row_dq_status": "Skipped", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Failed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Failed"}
                             )
                         ])
@patch('spark_expectations.utils.regulate_flow.SparkExpectationsNotify', autospec=True,
       spec_set=True)
def test_execute_dq_process(_mock_notify,
                            df,
                            expectations,
                            rule_type,
                            row_dq_flag,
                            source_agg_dq_flag,
                            final_agg_dq_flag,
                            source_query_dq_flag,
                            final_query_dq_flag,
                            input_count,
                            error_count,
                            output_count,
                            write_to_table,
                            expected_df,
                            agg_or_query_dq_res,
                            status,
                            _fixture_actions,
                            _fixture_context,
                            _fixture_create_stats_table):
    spark.conf.set("spark.sql.session.timeZone", "Etc/UTC")
    df.createOrReplaceTempView("test_table")
    _fixture_context._dq_expectations = expectations
    writer = SparkExpectationsWriter(_fixture_context)
    regulate_flow = SparkExpectationsRegulateFlow("product1")

    func_process = regulate_flow.execute_dq_process(
        _fixture_context,
        _fixture_actions,
        writer,
        _mock_notify,
        expectations,
        "dq_spark.test_final_table",
        input_count
    )

    # assert if expected output raises certain exception for failure
    if isinstance(expected_df, type) and issubclass(expected_df, Exception):
        with pytest.raises(expected_df,
                           match=r"error occurred while executing func_process error "
                                 r"occurred while taking action on given rules "
                                 r"Job failed, as there is a data quality issue .*"):
            (_df, _agg_dq_res, _error_count, _status) = func_process(df,
                                                                     rule_type,
                                                                     row_dq_flag,
                                                                     source_agg_dq_flag,
                                                                     final_agg_dq_flag,
                                                                     source_query_dq_flag,
                                                                     final_query_dq_flag,
                                                                     error_count,
                                                                     output_count)
            # compare with stats table
            stats_table = spark.table("test_dq_stats_table")
            assert stats_table.count() == 1
            row = stats_table.first()
            assert row.product_id == "product1"
            assert row.table_name == "dq_spark.test_final_table"
            assert row.input_count == input_count
            assert row.error_count == error_count
            assert row.output_count == output_count
            assert row.status.get("source_agg") == status.get("source_agg_dq_status")
            assert row.status.get("row_dq") == status.get("row_dq_status")
            assert row.status.get("final_agg") == status.get("final_agg_dq_status")
            assert row.status.get("run_status") == status.get("run_status")

    # assert expectations result
    else:
        (df, _agg_dq_res, _error_count, _status) = func_process(df,
                                                                rule_type,
                                                                row_dq_flag,
                                                                source_agg_dq_flag,
                                                                final_agg_dq_flag,
                                                                source_query_dq_flag,
                                                                final_query_dq_flag,
                                                                error_count,
                                                                output_count)

        if rule_type == "row_dq":
            expected_df = expected_df.withColumn("meta_dq_run_id", lit("product1_run_test")) \
                .withColumn("meta_dq_run_date", lit("2022-12-27 10:39:44"))
            assert df.orderBy("col2").collect() == expected_df.orderBy("col2").collect()
            assert _error_count == spark.table("dq_spark.test_final_table_error").count()
            assert _status == status.get("row_dq_status")
            assert _agg_dq_res == agg_or_query_dq_res
            assert output_count == expected_df.count()
            # if write_to_table is True:
            #     assert output_count == spark.table("dq_spark.test_final_table").count()

        elif (rule_type == "agg_dq"):
            # assert dq result for source_agg & final_agg
            assert _agg_dq_res == agg_or_query_dq_res
            assert _status == (
                status.get("source_agg_dq_status") if source_agg_dq_flag is True else status.get("final_agg_dq_status"))
        elif (rule_type == "agg_dq"):
            assert _agg_dq_res == agg_or_query_dq_res
            assert _status == (
                status.get("source_query_dq_status") if source_query_dq_flag is True else status.get(
                    "final_query_dq_status"))


@pytest.mark.parametrize("df, "
                         "expectations, "
                         "rule_type, "
                         "row_dq_flag, "
                         "source_agg_dq_flag, "
                         "final_agg_dq_flag, "
                         "source_query_dq_flag, "
                         "final_query_dq_flag, "
                         "input_count, "
                         "error_count, "
                         "output_count,"
                         "write_to_table ",
                         [
                             (
                                     #  faulty user input provided to test exception handling
                                     #  functionality in the module
                                     spark.createDataFrame(
                                         [  # dataset
                                             {"col1": 1, "col2": "a"},
                                             {"col1": 2, "col2": "b"},
                                             {"col1": 3, "col2": "c"},
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "col1 > 1",
                                             "action_if_failed": "ignore",
                                             "tag": "validity",
                                             "description": "col1 value must be greater than 1"
                                         }],
                                         "agg_dq_rules": [{}],
                                     },
                                     "row_dq",  # type of rule
                                     True,  # row_dq_flag
                                     False,  # source_agg_dq_flag
                                     False,  # final_agg_dq_flag
                                     False,  # source_query_dq_results
                                     False,  # final_query_dq_results
                                     3,  # input count
                                     1,  # error count
                                     3,  # output count
                                     True,  # write to table
                             ),
                             (
                                     #  faulty user input provided to test
                                     #  exception handling functionality in the module
                                     spark.createDataFrame(
                                         [
                                             # dataset
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             {"col1": 3, "col2": "c", "col3": 6},
                                         ]
                                     ),
                                     {
                                         # expectations
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "max_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "max(col1) > 100",
                                             "action_if_failed": "fail",
                                             "tag": "validity",
                                             "description": "max of col1 value must be greater than 100"
                                         }
                                         ]},
                                     "agg_dq",  # type of rule
                                     False,  # row_dq_flag
                                     False,  # source_agg_dq_flag
                                     True,  # final_agg_dq_flag
                                     False,  # source_query_dq_results
                                     False,  # final_query_dq_results
                                     3,  # input count
                                     1,  # error count
                                     2,  # output count
                                     True,  # write to table
                             ),
                             (
                                     #  faulty user input provided to test with registering table for query_dq
                                     #  exception handling functionality in the module
                                     spark.createDataFrame(
                                         [
                                             # dataset
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             {"col1": 3, "col2": "c", "col3": 6},
                                         ]
                                     ),
                                     {
                                         # expectations
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "max_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "max(col1) > 100",
                                             "action_if_failed": "fail",
                                             "tag": "validity",
                                             "description": "max of col1 value must be greater than 100"
                                         }
                                         ]},
                                     "query_dq",  # type of rule
                                     False,  # row_dq_flag
                                     False,  # source_agg_dq_flag
                                     False,  # final_agg_dq_flag
                                     True,  # source_query_dq_results
                                     False,  # final_query_dq_results
                                     3,  # input count
                                     1,  # error count
                                     2,  # output count
                                     True,  # write to table
                             )

                         ])
def test_execute_dq_process_exception(df,
                                      expectations,
                                      rule_type,
                                      row_dq_flag,
                                      source_agg_dq_flag,
                                      final_agg_dq_flag,
                                      source_query_dq_flag,
                                      final_query_dq_flag,
                                      input_count,
                                      error_count,
                                      output_count,
                                      write_to_table):
    # faulty user input provided to test exception handling functionality in the module
    # assert for the exception
    with pytest.raises(SparkExpectationsMiscException,
                       match=r"error occurred while executing func_process .*"):
        mock_contextt = Mock(spec=SparkExpectationsContext)
        mock_contextt.spark = spark
        actions = SparkExpectationsActions()
        writer = SparkExpectationsWriter(mock_contextt)
        regulate_flow = SparkExpectationsRegulateFlow("product1")
        func_process = regulate_flow.execute_dq_process(
            mock_contextt,
            actions,
            writer,
            expectations,
            "dq_spark.test_final_table",
            input_count
        )

        (_df, _agg_dq_res, _error_count, _status) = func_process(df,
                                                                 rule_type,
                                                                 row_dq_flag,
                                                                 source_agg_dq_flag,
                                                                 final_agg_dq_flag,
                                                                 source_query_dq_flag,
                                                                 final_query_dq_flag,
                                                                 error_count,
                                                                 output_count)
