# pylint: disable=too-many-lines
import os
from unittest.mock import Mock
from unittest.mock import patch
import pytest
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, to_timestamp, col
from pyspark.sql.types import StringType, IntegerType, StructField, StructType
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.utils.reader import SparkExpectationsReader
from spark_expectations.sinks.utils.writer import SparkExpectationsWriter
from spark_expectations.core.expectations import SparkExpectations, WrappedDataFrameWriter
from spark_expectations.config.user_config import Constants as user_config
from spark_expectations.core import get_spark_session
from spark_expectations.core.exceptions import (
    SparkExpectationsMiscException
)
from spark_expectations.notifications.push.spark_expectations_notify import SparkExpectationsNotify
from spark_expectations.sinks.utils.collect_statistics import SparkExpectationsCollectStatistics

spark = get_spark_session()


@pytest.fixture(name="_fixture_local_kafka_topic")
def fixture_setup_local_kafka_topic():
    current_dir = os.path.dirname(os.path.abspath(__file__))

    if os.getenv('UNIT_TESTING_ENV') != "spark_expectations_unit_testing_on_github_actions":

        # remove if docker conatiner is running
        os.system(f"sh {current_dir}/../../spark_expectations/examples/docker_scripts/docker_kafka_stop_script.sh")

        # start docker container and create the topic
        os.system(f"sh {current_dir}/../../spark_expectations/examples/docker_scripts/docker_kafka_start_script.sh")

        yield "docker container started"

        # remove docker container
        os.system(f"sh {current_dir}/../../spark_expectations/examples/docker_scripts/docker_kafka_stop_script.sh")

    else:
        yield "A Kafka server has been launched within a Docker container for the purpose of conducting tests in " \
              "a Jenkins environment"


@pytest.fixture(name="_fixture_df")
def fixture_df():
    # create a sample input raw dataframe for spark expectations
    return spark.createDataFrame(
        [
            {"row_id": 0, "col1": 1, "col2": "a"},
            {"row_id": 1, "col1": 2, "col2": "b"},
            {"row_id": 2, "col1": 3, "col2": "c"},
        ]
    )


@pytest.fixture(name="_fixture_dq_rules")
def fixture_dq_rules():
    # create a sample dq_rules map for above input
    return {"rules": {"num_dq_rules": 1, "num_row_dq_rules": 1},
            "query_dq_rules": {"num_final_query_dq_rules": 0, "num_source_query_dq_rules": 0,
                               "num_query_dq_rules": 0},
            "agg_dq_rules": {"num_source_agg_dq_rules": 0, "num_agg_dq_rules": 0,
                             "num_final_agg_dq_rules": 0}}


@pytest.fixture(name="_fixture_expectations")
def fixture_expectations():
    # create a sample input expectations to run on raw dataframe
    return {  # expectations rules
        "row_dq_rules": [{
            "product_id": "product1",
            "target_table_name": "dq_spark.test_table",
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
        "target_table_name": "dq_spark.test_final_table"

    }


@pytest.fixture(name="_fixture_create_database")
def fixture_create_database():
    # drop and create dq_spark if exists
    os.system("rm -rf /tmp/hive/warehouse/dq_spark.db")
    spark.sql("create database if not exists dq_spark")
    spark.sql("use dq_spark")

    yield "dq_spark"

    # drop dq_spark if exists
    os.system("rm -rf /tmp/hive/warehouse/dq_spark.db")


@pytest.fixture(name="_fixture_context")
def fixture_context():
    _context: SparkExpectationsContext = SparkExpectationsContext("product_id")
    _context.set_table_name("dq_spark.test_final_table")
    _context.set_dq_stats_table_name("dq_spark.test_dq_stats_table")
    _context.set_final_table_name("dq_spark.test_final_table")
    _context.set_error_table_name("dq_spark.test_final_table_error")
    _context._run_date = "2022-12-27 10:39:44"
    _context._env = "local"
    _context.set_input_count(100)
    _context.set_output_count(100)
    _context.set_error_count(0)
    _context._run_id = "product1_run_test"

    return _context


@pytest.fixture(name="_fixture_spark_expectations")
def fixture_spark_expectations(_fixture_context):
    # create a spark expectations class object
    spark_expectations = SparkExpectations("product1")

    def _error_threshold_exceeds(expectations):
        pass

    spark_expectations._context = _fixture_context
    spark_expectations.reader = SparkExpectationsReader("product1", _fixture_context)
    spark_expectations._writer = SparkExpectationsWriter("product1", _fixture_context)
    spark_expectations._notification = SparkExpectationsNotify("product1", _fixture_context)
    spark_expectations._notification.notify_rules_exceeds_threshold = _error_threshold_exceeds
    spark_expectations._statistics_decorator = SparkExpectationsCollectStatistics("product1",
                                                                                  spark_expectations._context,
                                                                                  spark_expectations._writer)
    return spark_expectations


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

    spark.sql("drop table if exists test_dq_stats_table")
    os.system("rm -rf /tmp/hive/warehouse/dq_spark.db/test_dq_stats_table")

    # remove database
    os.system("rm -rf /tmp/hive/warehouse/dq_spark.db")


@pytest.mark.parametrize("input_df, "
                         "expectations, "
                         "write_to_table, "
                         "write_to_temp_table, "
                         "row_dq, agg_dq, "
                         "source_agg_dq, "
                         "final_agg_dq, "
                         "query_dq, "
                         "source_query_dq, "
                         "final_query_dq, "
                         "expected_output, "
                         "input_count, "
                         "error_count, "
                         "output_count, "
                         "source_agg_dq_res, "
                         "final_agg_dq_res, "
                         "source_query_dq_res, "
                         "final_query_dq_res, "
                         "dq_rules, "
                         "status",
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
                                             "target_table_name": "dq_spark.test_table",
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
                                         "target_table_name": "dq_spark.test_final_table"

                                     },
                                     True,  # write to table
                                     True,  # write to temp table
                                     True,  # row_dq
                                     False,  # agg_dq
                                     False,  # source_agg_dq
                                     False,  # final_agg_dq
                                     False,  # query_dq
                                     False,  # source_query_dq
                                     False,  # final_query_dq
                                     # expected res
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a"},
                                             {"col1": 2, "col2": "b"},
                                             {"col1": 3, "col2": "c"},
                                         ]
                                     ),
                                     3,  # input count
                                     1,  # error count
                                     3,  # output count
                                     None,  # source_agg_dq_res
                                     None,  # final_agg_dq_res
                                     None,  # source_query_dq_res
                                     None,  # final_query_dq_res
                                     {"rules": {"num_dq_rules": 1, "num_row_dq_rules": 1},
                                      "query_dq_rules": {"num_final_query_dq_rules": 0, "num_source_query_dq_rules": 0,
                                                         "num_query_dq_rules": 0},
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 0, "num_agg_dq_rules": 0,
                                                       "num_final_agg_dq_rules": 0}},  # dq_rules
                                     # status at different stages for given input
                                     {"row_dq_status": "Passed", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"},
                             ),

                             (
                                     # test case 2
                                     # In this test case, the action for failed rows is "drop",
                                     # collect stats in the test_stats_table and
                                     # log the error records into the error table.
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             # row  meets expectations(drop), log into err & fnl
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             # row doesn't meets expectations(drop), log into final table
                                             {"col1": 3, "col2": "c", 'col3': 6},
                                             # row meets expectations(drop), log into final table
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col2_set",
                                             "column_name": "col2",
                                             "expectation": "col2 in  ('a', 'c')",
                                             "action_if_failed": "drop",
                                             "tag": "strict",
                                             "description": "col2 value must be in ('a', 'b')",
                                             "enable_error_drop_alert": True,
                                             "error_drop_threshold": "5",
                                         }],
                                         "agg_dq_rules": [{}],
                                         "target_table_name": "dq_spark.test_final_table"

                                     },
                                     True,  # write to table
                                     True,  # write to temp table
                                     True,  # row_dq
                                     False,  # agg_dq
                                     False,  # source_agg_dq
                                     False,  # final_agg_dq
                                     False,  # query_dq
                                     False,  # source_query_dq
                                     False,  # final_query_dq
                                     # expected res
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             # row doesn't meet expectations(ignore), log into err & fnl
                                             {"col1": 3, "col2": "c", 'col3': 6},
                                             # row meets expectations(ignore), log into final table
                                         ]
                                     ),
                                     3,  # input count
                                     1,  # error count
                                     2,  # output count
                                     None,  # source_agg_dq_res
                                     None,  # final_agg_dq_res
                                     None,  # source_query_dq_res
                                     None,  # final_query_dq_res
                                     {"rules": {"num_dq_rules": 1, "num_row_dq_rules": 1},
                                      "query_dq_rules": {"num_final_query_dq_rules": 0, "num_source_query_dq_rules": 0,
                                                         "num_query_dq_rules": 0},
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 0, "num_agg_dq_rules": 0,
                                                       "num_final_agg_dq_rules": 0}},  # dq_rules
                                     # status at different stages for given input
                                     {"row_dq_status": "Passed", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"},
                             ),
                             (
                                     # test case 3
                                     # In this test case, the action for failed rows is "fail",
                                     # spark expectations expected to fail
                                     # collect stats in the test_stats_table and
                                     # log the error records into the error table.
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             # row doesn't meet expectations(fail), log into err & fnl
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             # row meets doesn't expectations(fail), log into final table
                                             {"col1": 3, "col2": "c", 'col3': 6},
                                             # row meets doesn't expectations(fail), log into final table
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col3_threshold",
                                             "column_name": "col3",
                                             "expectation": "col3 > 6",
                                             "action_if_failed": "fail",
                                             "tag": "strict",
                                             "description": "col3 value must be greater than 6",
                                             "enable_error_drop_alert": True,
                                             "error_drop_threshold": "15",
                                         }],
                                         "agg_dq_rules": [{}],
                                         "target_table_name": "dq_spark.test_final_table"

                                     },
                                     True,  # write to table
                                     True,  # write to temp table
                                     True,  # row_dq
                                     False,  # agg_dq
                                     False,  # source_agg_dq
                                     False,  # final_agg_dq_dq
                                     False,  # query_dq
                                     False,  # source_query_dq
                                     False,  # final_query_dq
                                     SparkExpectationsMiscException,  # expected res
                                     3,  # input count
                                     3,  # error count
                                     0,  # output count
                                     None,  # source_agg_dq_res
                                     None,  # final_agg_dq_res
                                     None,  # source_query_dq_res
                                     None,  # final_query_dq_res
                                     {"rules": {"num_dq_rules": 1, "num_row_dq_rules": 1},
                                      "query_dq_rules": {"num_final_query_dq_rules": 0, "num_source_query_dq_rules": 0,
                                                         "num_query_dq_rules": 0},
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 0, "num_agg_dq_rules": 0,
                                                       "num_final_agg_dq_rules": 0}},  # dq_rules
                                     # status at different stages for given input
                                     {"row_dq_status": "Failed", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Failed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"},
                             ),
                             (  # test case 4
                                     # In this test case, the action for failed rows is "ignore" & "drop",
                                     # collect stats in the test_stats_table and
                                     # log the error records into the error table.
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             # row doesn't meet expectations1(ignore) 2(drop), log into err & fnl
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             # row meets expectations1(ignore), log into final table
                                             {"col1": 3, "col2": "c", 'col3': 6},
                                             # row doesnt'meets expectations1(ignore), log into final table
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col3_threshold",
                                             "column_name": "col3",
                                             "expectation": "col3 > 6",
                                             "action_if_failed": "ignore",
                                             "tag": "strict",
                                             "description": "col3 value must be greater than 6",
                                             "enable_error_drop_alert": False,
                                             "error_drop_threshold": "10",
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "target_table_name": "dq_spark.test_table",
                                                 "rule_type": "row_dq",
                                                 "rule": "col1_add_col3_threshold",
                                                 "column_name": "col1",
                                                 "expectation": "(col1+col3) > 6",
                                                 "action_if_failed": "drop",
                                                 "tag": "strict",
                                                 "description": "col1_add_col3 value must be greater than 6",
                                                 "enable_error_drop_alert": False,
                                                 "error_drop_threshold": "15",
                                             }
                                         ],
                                         "agg_dq_rules": [{}],
                                         "target_table_name": "dq_spark.test_final_table"

                                     },
                                     True,  # write to table
                                     True,  # write to temp table
                                     True,  # row_dq
                                     False,  # agg_dq
                                     False,  # source_agg_dq
                                     False,  # final_agg_dq
                                     False,  # query_dq
                                     False,  # source_query_dq
                                     False,  # final_query_dq
                                     # expected res
                                     spark.createDataFrame(
                                         [
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             {"col1": 3, "col2": "c", 'col3': 6},
                                         ]
                                     ),
                                     3,  # input count
                                     3,  # error count
                                     2,  # output count
                                     None,  # source_agg_dq_res
                                     None,  # final_agg_dq_res
                                     None,  # source_query_dq_res
                                     None,  # final_query_dq_res
                                     {"rules": {"num_dq_rules": 2, "num_row_dq_rules": 2},
                                      "query_dq_rules": {"num_final_query_dq_rules": 0, "num_source_query_dq_rules": 0,
                                                         "num_query_dq_rules": 0},
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 0, "num_agg_dq_rules": 0,
                                                       "num_final_agg_dq_rules": 0}},  # dq_rules
                                     # status at different stages for given input
                                     {"row_dq_status": "Passed", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"},
                             ),
                             (
                                     # test case 5
                                     # In this test case, the action for failed rows is "ignore" & "fail",
                                     # collect stats in the test_stats_table and
                                     # log the error records into the error table.
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             # row doesn't meet expectations1(ignore), log into err & fnl
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             # row meets expectations1(ignore), log into final table
                                             {"col1": 3, "col2": "c", 'col3': 6},
                                             # row meets expectations1(ignore), log into final table
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col3_threshold",
                                             "column_name": "col3",
                                             "expectation": "col3 > 6",
                                             "action_if_failed": "ignore",
                                             "tag": "strict",
                                             "description": "col3 value must be greater than 6",
                                             "enable_error_drop_alert": True,
                                             "error_drop_threshold": "20",
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "target_table_name": "dq_spark.test_table",
                                                 "rule_type": "row_dq",
                                                 "rule": "col3_minus_col1_threshold",
                                                 "column_name": "col1",
                                                 "expectation": "(col3-col1) > 1",
                                                 "action_if_failed": "fail",
                                                 "tag": "strict",
                                                 "description": "col3_minus_col1 value must be greater than 1",
                                                 "enable_error_drop_alert": True,
                                                 "error_drop_threshold": "5",
                                             }
                                         ],
                                         "agg_dq_rules": [{}],
                                         "target_table_name": "dq_spark.test_final_table"

                                     },
                                     True,  # write to table
                                     True,  # write to temp table
                                     True,  # row_dq
                                     False,  # agg_dq
                                     False,  # source_agg_dq
                                     False,  # final_agg_dq
                                     False,  # query_dq
                                     False,  # source_query_dq
                                     False,  # final_query_dq
                                     # expected res
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             {"col1": 3, "col2": "c", 'col3': 6},
                                         ]
                                     ),
                                     3,  # input count
                                     3,  # error count
                                     3,  # output count
                                     None,  # source_agg_dq_res
                                     None,  # final_agg_dq_res
                                     None,  # source_query_dq_res
                                     None,  # final_query_dq_res
                                     {"rules": {"num_dq_rules": 2, "num_row_dq_rules": 2},
                                      "query_dq_rules": {"num_final_query_dq_rules": 0, "num_source_query_dq_rules": 0,
                                                         "num_query_dq_rules": 0},
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 0, "num_agg_dq_rules": 0,
                                                       "num_final_agg_dq_rules": 0}},  # dq_rules
                                     # status at different stages for given input
                                     {"row_dq_status": "Passed", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"},
                             ),
                             (  # Test case 6
                                     # In this test case, the action for failed rows is "drop" & "fail",
                                     # collect stats in the test_stats_table and
                                     # log the error records into the error table.
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             # row doesn't meet expectations1(drop) & 2(drop), log into err & fnl
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             # row meets expectations1(drop) & 2(fail), log into final table
                                             {"col1": 3, "col2": "c", 'col3': 6},
                                             # row meets expectations1(drop), & 2(fail) log into final table
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col3_threshold",
                                             "column_name": "col3",
                                             "expectation": "col3 > 6",
                                             "action_if_failed": "drop",
                                             "tag": "strict",
                                             "description": "col3 value must be greater than 6",
                                             "enable_error_drop_alert": True,
                                             "error_drop_threshold": "25",
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "target_table_name": "dq_spark.test_table",
                                                 "rule_type": "row_dq",
                                                 "rule": "col3_minus_col1_threshold",
                                                 "column_name": "col1",
                                                 "expectation": "(col3-col1) = 1",
                                                 "action_if_failed": "fail",
                                                 "tag": "strict",
                                                 "description": "col3_minus_col1 value must be equals to 1",
                                                 "enable_error_drop_alert": False,
                                                 "error_drop_threshold": "25",
                                             }
                                         ],
                                         "agg_dq_rules": [{}],
                                         "target_table_name": "dq_spark.test_final_table"

                                     },
                                     True,  # write to table
                                     True,  # write to temp table
                                     True,  # row_dq
                                     False,  # agg_dq
                                     False,  # source_dq
                                     False,  # final_dq
                                     False,  # query_dq
                                     False,  # source_query_dq
                                     False,  # final_query_dq
                                     SparkExpectationsMiscException,  # expected res
                                     3,  # input count
                                     3,  # error count
                                     0,  # output count
                                     None,  # source_agg_dq_res
                                     None,  # final_agg_dq_res
                                     None,  # source_query_dq_res
                                     None,  # final_query_dq_res
                                     {"rules": {"num_dq_rules": 2, "num_row_dq_rules": 2},
                                      "query_dq_rules": {"num_final_query_dq_rules": 0, "num_source_query_dq_rules": 0,
                                                         "num_query_dq_rules": 0},
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 0, "num_agg_dq_rules": 0,
                                                       "num_final_agg_dq_rules": 0}},  # dq_rules
                                     # status at different stages for given input
                                     {"row_dq_status": "Failed", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Failed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"},
                             ),
                             (  # Test case 6
                                     # In this test case, the action for failed rows is "drop" & "fail",
                                     # collect stats in the test_stats_table and
                                     # log the error records into the error table
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             # row doesn't meet expectations1(drop) & meets 2(fail), log into err & fnl
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             # row meets expectations1(drop) & meets 2(fail), log into final table
                                             {"col1": 3, "col2": "c", 'col3': 6},
                                             # row meets expectations1(drop) & meets 2(fail), log into final table
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col3_threshold",
                                             "column_name": "col3",
                                             "expectation": "col3 > 6",
                                             "action_if_failed": "drop",
                                             "tag": "strict",
                                             "description": "col3 value must be greater than 6",
                                             "enable_error_drop_alert": True,
                                             "error_drop_threshold": "10",
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "target_table_name": "dq_spark.test_table",
                                                 "rule_type": "row_dq",
                                                 "rule": "col3_mul_col1_threshold",
                                                 "column_name": "col1",
                                                 "expectation": "(col3*col1) > 1",
                                                 "action_if_failed": "fail",
                                                 "tag": "strict",
                                                 "description": "col3_mul_col1 value must be equals to 1",
                                                 "enable_error_drop_alert": True,
                                                 "error_drop_threshold": "10",
                                             }
                                         ],
                                         "agg_dq_rules": [{}],
                                         "target_table_name": "dq_spark.test_final_table"

                                     },
                                     True,  # write to table
                                     True,  # write to temp table
                                     True,  # row_dq
                                     False,  # agg_dq
                                     False,  # source_dq
                                     False,  # final_dq
                                     False,  # query_dq
                                     False,  # source_query_dq
                                     False,  # final_query_dq
                                     # expected res
                                     spark.createDataFrame([], schema=StructType([
                                         StructField("col1", IntegerType()),
                                         StructField("col2", StringType()),
                                         StructField("col3", IntegerType())
                                     ])),
                                     3,  # input count
                                     3,  # error count
                                     0,  # output count
                                     None,  # source_agg_dq_res
                                     None,  # final_agg_dq_res
                                     None,  # source_query_dq_res
                                     None,  # final_query_dq_res
                                     {"rules": {"num_dq_rules": 2, "num_row_dq_rules": 2},
                                      "query_dq_rules": {"num_final_query_dq_rules": 0, "num_source_query_dq_rules": 0,
                                                         "num_query_dq_rules": 0},
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 0, "num_agg_dq_rules": 0,
                                                       "num_final_agg_dq_rules": 0}},  # dq_rules
                                     # status at different stages for given input
                                     {"row_dq_status": "Passed", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"},
                             ),
                             (
                                     # Test case 7
                                     # In this test case, the action for failed rows is "ignore", "drop" & "fail",
                                     # collect stats in the test_stats_table and
                                     # log the error records into the error table

                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             # row doesn't meet the expectation1(ignore) & expectation2(drop)
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             # row doesn't meet the expectation2(drop)
                                             {"col1": 3, "col2": "c", 'col3': 6},
                                             # row meets all the expectations
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "col1 > 1",
                                             "action_if_failed": "ignore",
                                             "tag": "strict",
                                             "description": "col1 value must be greater than 1",
                                             "enable_error_drop_alert": False,
                                             "error_drop_threshold": "0",
                                         }, {
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col3_threshold",
                                             "column_name": "col3",
                                             "expectation": "col3 > 5",
                                             "action_if_failed": "drop",
                                             "tag": "strict",
                                             "description": "col3 value must be greater than 5",
                                             "enable_error_drop_alert": True,
                                             "error_drop_threshold": "10",
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "target_table_name": "dq_spark.test_table",
                                                 "rule_type": "row_dq",
                                                 "rule": "col3_mul_col1_threshold",
                                                 "column_name": "col1",
                                                 "expectation": "(col3*col1) > 1",
                                                 "action_if_failed": "fail",
                                                 "tag": "strict",
                                                 "description": "col3_mul_col1 value must be equals to 1",
                                                 "enable_error_drop_alert": False,
                                                 "error_drop_threshold": "20",
                                             }
                                         ],
                                         "agg_dq_rules": [{}],
                                         "target_table_name": "dq_spark.test_final_table"

                                     },
                                     True,  # write to table
                                     True,  # write_to_temp_table
                                     True,  # row_dq
                                     False,  # agg_dq
                                     False,  # source_dq
                                     False,  # final_agg_dq
                                     False,  # query_dq
                                     False,  # source_query_dq
                                     False,  # final_query_dq
                                     spark.createDataFrame(  # expected output
                                         [
                                             {"col1": 3, "col2": "c", 'col3': 6},
                                         ]
                                     ),
                                     3,  # input count
                                     2,  # error count
                                     1,  # output count
                                     None,  # source_agg_result
                                     None,  # final_agg_result
                                     None,  # source_query_dq_res
                                     None,  # final_query_dq_res
                                     {"rules": {"num_dq_rules": 3, "num_row_dq_rules": 3},
                                      "query_dq_rules": {"num_final_query_dq_rules": 0, "num_source_query_dq_rules": 0,
                                                         "num_query_dq_rules": 0},
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 0, "num_agg_dq_rules": 0,
                                                       "num_final_agg_dq_rules": 0}},  # dq_rules
                                     {"row_dq_status": "Passed", "source_agg_dq_status": "Skipped",  # status
                                      "final_agg_dq_status": "Skipped", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"},
                             ),
                             (
                                     # Test case 8
                                     # In this test case, dq run set for source_agg_dq
                                     # collect stats in the test_stats_table
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             {"col1": 3, "col2": "c", 'col3': 6},
                                         ]
                                     ),
                                     {  # expectations rules
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "sum_col3_threshold",
                                             "column_name": "col3",
                                             "expectation": "sum(col3) > 20",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "ignore",
                                             "tag": "strict",
                                             "description": "sum col3 value must be greater than 20",
                                             "enable_error_drop_alert": True,
                                             "error_drop_threshold": "10",
                                         }],
                                         "row_dq_rules": [{}],
                                         "target_table_name": "dq_spark.test_final_table"

                                     },
                                     True,  # write to table
                                     True,  # write to temp table
                                     False,  # row_dq
                                     True,  # agg_dq
                                     True,  # source_agg_dq
                                     False,  # final_agg_dq
                                     False,  # query_dq
                                     False,  # source_query_dq
                                     False,  # final_query_dq
                                     None,  # expected result
                                     3,  # input count
                                     0,  # error count
                                     0,  # output count
                                     [{"description": "sum col3 value must be greater than 20",  # source_ag_result
                                       "rule": "sum_col3_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "ignore", "tag": "strict"}],
                                     None,  # final_agg_result
                                     None,  # source_query_dq_res
                                     None,  # final_query_dq_res
                                     {"rules": {"num_dq_rules": 1, "num_row_dq_rules": 0},
                                      "query_dq_rules": {"num_final_query_dq_rules": 0, "num_source_query_dq_rules": 0,
                                                         "num_query_dq_rules": 0},
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 1, "num_agg_dq_rules": 1,
                                                       "num_final_agg_dq_rules": 1}},  # dq_rules
                                     {"row_dq_status": "Skipped", "source_agg_dq_status": "Passed",  # status
                                      "final_agg_dq_status": "Skipped", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"},
                             ),
                             (
                                     # Test case 9
                                     # In this test case, dq run set for source_agg_dq with action_if_failed fail
                                     # collect stats in the test_stats_table
                                     spark.createDataFrame(
                                         [
                                             # avg of col3 is not more than 25
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             {"col1": 3, "col2": "c", 'col3': 6},
                                         ]
                                     ),
                                     {  # expectations rules
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "avg_col3_threshold",
                                             "column_name": "col3",
                                             "expectation": "avg(col3) > 25",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "fail",
                                             "tag": "strict",
                                             "description": "avg col3 value must be greater than 25",
                                         }],
                                         "row_dq_rules": [{}],
                                         "target_table_name": "dq_spark.test_final_table"

                                     },
                                     True,  # write to table
                                     True,  # write to temp table
                                     False,  # row_dq
                                     True,  # agg_dq
                                     True,  # source_agg_dq
                                     False,  # final_agg_dq
                                     False,  # query_dq
                                     False,  # source_query_dq
                                     False,  # final_query_dq
                                     SparkExpectationsMiscException,  # excepted result
                                     3,  # input count
                                     0,  # error count
                                     0,  # output count
                                     [{"description": "avg col3 value must be greater than 25",  # source_agg_result
                                       "rule": "avg_col3_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "fail", "tag": "strict"}],
                                     None,  # final_agg_result
                                     None,  # source_query_dq_res
                                     None,  # final_query_dq_res
                                     {"rules": {"num_dq_rules": 1, "num_row_dq_rules": 0},
                                      "query_dq_rules": {"num_final_query_dq_rules": 0, "num_source_query_dq_rules": 0,
                                                         "num_query_dq_rules": 0},
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 1, "num_agg_dq_rules": 1,
                                                       "num_final_agg_dq_rules": 1}},  # dq_rules
                                     {"row_dq_status": "Skipped", "source_agg_dq_status": "Failed",  # status
                                      "final_agg_dq_status": "Skipped", "run_status": "Failed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"},
                             ),
                             (
                                     # Test case 10
                                     # In this test case, dq run set for final_agg_dq with action_if_failed ignore
                                     # collect stats in the test_stats_table
                                     spark.createDataFrame(
                                         [
                                             # minimum of col1 must be greater than 10
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             {"col1": 3, "col2": "c", 'col3': 6},
                                         ]
                                     ),
                                     {  # expectations rules
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "min_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "min(col1) > 10",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "ignore",
                                             "tag": "strict",
                                             "description": "min col1 value must be greater than 10",
                                         }],
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col2_set",
                                             "column_name": "col2",
                                             "expectation": "col2 in  ('a', 'c')",
                                             "action_if_failed": "drop",
                                             "tag": "strict",
                                             "description": "col2 value must be in ('a', 'b')",
                                             "enable_error_drop_alert": False,
                                             "error_drop_threshold": "0",
                                         }],
                                         "target_table_name": "dq_spark.test_final_table"

                                     },
                                     True,  # write to table
                                     True,  # write to temp table
                                     True,  # row_dq
                                     True,  # agg_dq
                                     False,  # source_agg_dq
                                     True,  # final_agg_dq
                                     False,  # query_dq
                                     False,  # source_query_dq
                                     False,  # final_query_dq
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 3, "col2": "c", 'col3': 6},

                                         ]
                                     ),  # expected result but row_dq set to false
                                     3,  # input count
                                     1,  # error count
                                     2,  # output count
                                     None,  # source_agg-result
                                     [{"description": "min col1 value must be greater than 10",
                                       "rule": "min_col1_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "ignore", "tag": "strict"}],
                                     # final_agg_result
                                     None,  # source_query_dq_res
                                     None,  # final_query_dq_res
                                     {"rules": {"num_dq_rules": 2, "num_row_dq_rules": 1},
                                      "query_dq_rules": {"num_final_query_dq_rules": 0, "num_source_query_dq_rules": 0,
                                                         "num_query_dq_rules": 0},
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 1, "num_agg_dq_rules": 1,
                                                       "num_final_agg_dq_rules": 1}},  # dq_rules
                                     {"row_dq_status": "Passed", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Passed", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"},
                             ),
                             (
                                     # Test case 11
                                     # In this test case, dq run set for row_dq & final_agg_dq
                                     # with action_if_failed drop(row), fail(agg)
                                     # collect stats in the test_stats_table & error into error_table

                                     spark.createDataFrame(
                                         # standard deviation of col3 must be greater than 10
                                         [
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             # row meet expectation
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             # row doesn't meet expectation
                                             {"col1": 3, "col2": "c", 'col3': 6},
                                             # row meets expectations

                                         ]
                                     ),
                                     {  # expectations rules
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "std_col3_threshold",
                                             "column_name": "col3",
                                             "expectation": "stddev(col3) > 10",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "fail",
                                             "tag": "strict",
                                             "description": "std col3 value must be greater than 10",
                                         }],
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col2_set",
                                             "column_name": "col2",
                                             "expectation": "col2 in  ('a', 'c')",
                                             "action_if_failed": "drop",
                                             "tag": "strict",
                                             "description": "col2 value must be in ('a', 'b')",
                                             "enable_error_drop_alert": True,
                                             "error_drop_threshold": "15",
                                         }],
                                         "target_table_name": "dq_spark.test_final_table"
                                     },
                                     True,  # write to table
                                     True,  # write temp table
                                     True,  # row_dq
                                     True,  # agg_dq
                                     False,  # source_dq_dq
                                     True,  # final_agg_dq
                                     False,  # query_dq
                                     False,  # source_query_dq
                                     False,  # final_query_dq
                                     SparkExpectationsMiscException,  # expected result
                                     3,  # input count
                                     1,  # error count
                                     2,  # output count
                                     None,  # source_agg_result
                                     [{"description": "std col3 value must be greater than 10",
                                       "rule": "std_col3_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "fail", "tag": "strict"}],
                                     # final_agg_result
                                     None,  # source_query_dq_res
                                     None,  # final_query_dq_res
                                     {"rules": {"num_dq_rules": 2, "num_row_dq_rules": 1},
                                      "query_dq_rules": {"num_final_query_dq_rules": 0, "num_source_query_dq_rules": 0,
                                                         "num_query_dq_rules": 0},
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 1, "num_agg_dq_rules": 1,
                                                       "num_final_agg_dq_rules": 1}},  # dq_rules
                                     {"row_dq_status": "Passed", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Failed", "run_status": "Failed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"},
                                     # status
                             ),

                             (
                                     # Test case 12
                                     # In this test case, dq run set for row_dq
                                     # with action_if_failed drop
                                     # collect stats in the test_stats_table & error into error_table
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a"},
                                             # row doesn't meet the expectations
                                             {"col1": 2, "col2": "b"},
                                             # row meets the expectation
                                             {"col1": 3, "col2": "c"},
                                             # row meets the expectations
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "col1 > 1",
                                             "action_if_failed": "drop",
                                             "tag": "validity",
                                             "description": "col1 value must be greater than 1",
                                             "enable_error_drop_alert": False,
                                             "error_drop_threshold": "0",
                                         }],
                                         "target_table_name": "dq_spark.test_final_table"
                                     },
                                     True,  # write to table
                                     True,  # write to temp table
                                     True,  # row_dq
                                     False,  # agg_dq
                                     False,  # source_agg_dq
                                     False,  # final_agg_dq
                                     False,  # query_dq
                                     False,  # source_query_dq
                                     False,  # final_query_dq
                                     spark.createDataFrame([  # expected_output
                                         {"col1": 2, "col2": "b"},
                                         {"col1": 3, "col2": "c"}
                                     ]),  # expected result
                                     3,  # input count
                                     1,  # error count
                                     2,  # output count
                                     None,  # source_agg_result
                                     None,  # final_agg_result
                                     None,  # source_query_dq_res
                                     None,  # final_query_dq_res
                                     {"rules": {"num_dq_rules": 1, "num_row_dq_rules": 1},
                                      "query_dq_rules": {"num_final_query_dq_rules": 0, "num_source_query_dq_rules": 0,
                                                         "num_query_dq_rules": 0},
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 0, "num_agg_dq_rules": 0,
                                                       "num_final_agg_dq_rules": 0}},  # dq_rules
                                     {"row_dq_status": "Passed", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"}  # status
                             ),
                             (
                                     # Test case 13
                                     # In this test case, dq run set for row_dq  & source_agg_dq
                                     # with action_if_failed (ignore, drop) and ignore(agg_dq)
                                     # collect stats in the test_stats_table & error into error_table

                                     spark.createDataFrame(
                                         [
                                             # count of distinct element in col2 must be greater than 2
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             # row doesn't meet expectation1(ignore)
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             # row meets all row_dq expectations
                                             {"col1": 3, "col2": "c", "col3": 6},
                                             # row meets all row_dq expectations
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col1_threshold_1",
                                             "column_name": "col1",
                                             "expectation": "col1 > 1",
                                             "action_if_failed": "ignore",
                                             "tag": "validity",
                                             "description": "col1 value must be greater than 1",
                                             "enable_error_drop_alert": True,
                                             "error_drop_threshold": "10",
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "target_table_name": "dq_spark.test_table",
                                                 "rule_type": "row_dq",
                                                 "rule": "col2_set",
                                                 "column_name": "col2",
                                                 "expectation": "col2 in ('a', 'b', 'c')",
                                                 "action_if_failed": "drop",
                                                 "tag": "validity",
                                                 "description": "col1 value must be greater than 2",
                                                 "enable_error_drop_alert": True,
                                                 "error_drop_threshold": "10",
                                             }],
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "distinct_col2_threshold",
                                             "column_name": "col2",
                                             "expectation": "count(distinct col2) > 4",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "ignore",
                                             "tag": "validity",
                                             "description": "distinct of col2 value must be greater than 4",
                                         }],
                                         "target_table_name": "dq_spark.test_final_table"

                                     },
                                     True,  # write to table
                                     True,  # write to temp table
                                     True,  # row_dq
                                     True,  # agg_dq
                                     True,  # source_agg_dq
                                     False,  # final_agg_dq
                                     False,  # query_dq
                                     False,  # source_query_dq
                                     False,  # final_query_dq
                                     spark.createDataFrame([  # expected_output
                                         {"col1": 1, "col2": "a", "col3": 4},
                                         {"col1": 2, "col2": "b", "col3": 5},
                                         {"col1": 3, "col2": "c", "col3": 6}
                                     ]),
                                     3,  # input count
                                     1,  # error count
                                     3,  # output count
                                     [{"description": "distinct of col2 value must be greater than 4",
                                       "rule": "distinct_col2_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "ignore", "tag": "validity"}],
                                     # source_agg_result
                                     None,  # final_agg_result
                                     None,  # source_query_dq_res
                                     None,  # final_query_dq_res
                                     {"rules": {"num_dq_rules": 3, "num_row_dq_rules": 2},
                                      "query_dq_rules": {"num_final_query_dq_rules": 0, "num_source_query_dq_rules": 0,
                                                         "num_query_dq_rules": 0},
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 1, "num_agg_dq_rules": 1,
                                                       "num_final_agg_dq_rules": 1}},  # dq_rules
                                     {"row_dq_status": "Passed", "source_agg_dq_status": "Passed",
                                      "final_agg_dq_status": "Skipped", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"}  # status

                             ),

                             (
                                     # Test case 14
                                     # In this test case, dq run set for row_dq, source_agg_dq & final_agg_dq
                                     # with action_if_failed r(ignore, drop) for row_dq  and (ignore) for agg_dq
                                     # collect stats in the test_stats_table & error into error_table
                                     spark.createDataFrame(
                                         [
                                             # avg of col1 must be greater than 4(ignore) for agg_dq
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             # row doesn't meet row_dq expectation1(ignore)
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             # row doesn't meet row_dq expectation2(drop)
                                             {"col1": 3, "col2": "c", "col3": 6},
                                             # row meets all row-dq expectations
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col3_threshold_4",
                                             "column_name": "col3",
                                             "expectation": "col3 > 4",
                                             "action_if_failed": "drop",
                                             "tag": "validity",
                                             "description": "col3 value must be greater than 4",
                                             "enable_error_drop_alert": True,
                                             "error_drop_threshold": "20",
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "target_table_name": "dq_spark.test_table",
                                                 "rule_type": "row_dq",
                                                 "rule": "col2_set",
                                                 "column_name": "col2",
                                                 "expectation": "col2 in ('a', 'b')",
                                                 "action_if_failed": "ignore",
                                                 "tag": "validity",
                                                 "description": "col2 value must be in (a, b)",
                                                 "enable_error_drop_alert": True,
                                                 "error_drop_threshold": "2",
                                             }],
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "avg_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "avg(col1) > 4",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "ignore",
                                             "tag": "accuracy",
                                             "description": "avg of col1 value must be greater than 4",
                                         }],
                                         "target_table_name": "dq_spark.test_final_table"

                                     },
                                     True,  # write to table
                                     True,  # write to temp table
                                     True,  # row_dq
                                     True,  # agg_dq
                                     True,  # source_agg_dq
                                     True,  # final_agg_dq
                                     False,  # query_dq
                                     False,  # source_query_dq
                                     False,  # final_query_dq
                                     spark.createDataFrame([  # expected_output
                                         {"col1": 2, "col2": "b", "col3": 5},
                                         {"col1": 3, "col2": "c", "col3": 6}
                                     ]),  # expected result
                                     3,  # input count
                                     2,  # error count
                                     2,  # output count
                                     [{"description": "avg of col1 value must be greater than 4",
                                       "rule": "avg_col1_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "ignore", "tag": "accuracy"}],
                                     # source_agg_dq
                                     [{"description": "avg of col1 value must be greater than 4",
                                       "rule": "avg_col1_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "ignore", "tag": "accuracy"}],
                                     # final_agg_dq
                                     None,  # source_query_dq_res
                                     None,  # final_query_dq_res
                                     {"rules": {"num_dq_rules": 3, "num_row_dq_rules": 2},
                                      "query_dq_rules": {"num_final_query_dq_rules": 0, "num_source_query_dq_rules": 0,
                                                         "num_query_dq_rules": 0},
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 1, "num_agg_dq_rules": 1,
                                                       "num_final_agg_dq_rules": 1}},  # dq_rules
                                     {"row_dq_status": "Passed", "source_agg_dq_status": "Passed",
                                      "final_agg_dq_status": "Passed", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"}  # status

                             ),
                             (
                                     # Test case 15
                                     # In this test case, dq run set for row_dq, source_agg_dq & final_agg_dq
                                     # with action_if_failed (ignore, drop) for row_dq  and (ignore, fail) for agg_dq
                                     # collect stats in the test_stats_table & error into error_table
                                     spark.createDataFrame(
                                         [
                                             # average of col1 must be greater than 4(ignore)
                                             # standard deviation of col1 must be greater than 0(fail)
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             # row doesn't meet row_dq expectation1(drop) and expectation2(ignore)
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             # row meets all row_dq_expectations
                                             {"col1": 3, "col2": "c", "col3": 6},
                                             # row meets all row_dq_expectations
                                             {"col1": 2, "col2": "d", "col3": 7}
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col3_and_col1_threshold_4",
                                             "column_name": "col3, col1",
                                             "expectation": "((col3 * col1) - col3) > 5",
                                             "action_if_failed": "drop",
                                             "tag": "validity",
                                             "description": "col3 and col1 operation value must be greater than 3",
                                             "enable_error_drop_alert": True,
                                             "error_drop_threshold": "25",
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "target_table_name": "dq_spark.test_table",
                                                 "rule_type": "row_dq",
                                                 "rule": "col2_set",
                                                 "column_name": "col2",
                                                 "expectation": "col2 in ('b', 'c')",
                                                 "action_if_failed": "ignore",
                                                 "tag": "validity",
                                                 "description": "col2 value must be in (b, c)",
                                                 "enable_error_drop_alert": True,
                                                 "error_drop_threshold": "30",
                                             }],
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "avg_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "avg(col1) > 4",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "ignore",
                                             "tag": "validity",
                                             "description": "avg of col1 value must be greater than 4",
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "target_table_name": "dq_spark.test_table",
                                                 "rule_type": "agg_dq",
                                                 "rule": "stddev_col3_threshold",
                                                 "column_name": "col3",
                                                 "expectation": "stddev(col3) > 1",
                                                 "enable_for_source_dq_validation": True,
                                                 "enable_for_target_dq_validation": False,
                                                 "action_if_failed": "fail",
                                                 "tag": "validity",
                                                 "description": "stddev of col3 value must be greater than one"
                                             },
                                             {
                                                 "product_id": "product1",
                                                 "target_table_name": "dq_spark.test_table",
                                                 "rule_type": "agg_dq",
                                                 "rule": "stddev_col3_threshold",
                                                 "column_name": "col3",
                                                 "expectation": "stddev(col3) < 1",
                                                 "enable_for_source_dq_validation": False,
                                                 "enable_for_target_dq_validation": True,
                                                 "action_if_failed": "fail",
                                                 "tag": "validity",
                                                 "description": "avg of col3 value must be greater than 0",
                                             }
                                         ],
                                         "target_table_name": "dq_spark.test_final_table"

                                     },
                                     True,  # write to table
                                     True,  # write to temp table
                                     True,  # row_dq
                                     True,  # agg_dq
                                     True,  # source_agg_dq
                                     True,  # final_agg_dq
                                     False,  # query_dq
                                     False,  # source_query_dq
                                     False,  # final_query_dq
                                     spark.createDataFrame([  # expected_output
                                         {"col1": 3, "col2": "c", "col3": 6},
                                         {"col1": 2, "col2": "d", "col3": 7}
                                     ]),  # expected result
                                     4,  # input count
                                     3,  # error count
                                     2,  # output count
                                     [{"description": "avg of col1 value must be greater than 4",
                                       "rule": "avg_col1_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "ignore", "tag": "validity"}],
                                     # source_agg_result
                                     [{"description": "avg of col1 value must be greater than 4",
                                       "rule": "avg_col1_threshold",
                                       "rule_type": "agg_dq", "action_if_failed": "ignore", "tag": "validity"}],
                                     # final_agg_result
                                     None,  # source_query_dq_res
                                     None,  # final_query_dq_res
                                     {"rules": {"num_dq_rules": 5, "num_row_dq_rules": 2},
                                      "query_dq_rules": {"num_final_query_dq_rules": 0, "num_source_query_dq_rules": 0,
                                                         "num_query_dq_rules": 0},  # dq_rules
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 2, "num_agg_dq_rules": 3,
                                                       "num_final_agg_dq_rules": 2}},
                                     {"row_dq_status": "Passed", "source_agg_dq_status": "Passed",
                                      "final_agg_dq_status": "Passed", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"}  # status

                             ),
                             (
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             # row doesn't meet expectations(fail),log into error and raise error
                                             {"col1": 2, "col2": "b", "col3": 5},  # row meet expectations(fail)
                                             {"col1": 3, "col2": "c", "col3": 6},  # row meet expectations(fail)
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [],
                                         "agg_dq_rules": [],
                                         "target_table_name": "dq_spark.test_final_table"

                                     },
                                     True,
                                     True,
                                     True,
                                     True,
                                     True,
                                     True,
                                     False,  # query_dq
                                     False,  # source_query_dq
                                     False,  # final_query_dq
                                     SparkExpectationsMiscException,
                                     3,  # input count
                                     0,  # error count
                                     0,
                                     None,
                                     None,
                                     None,  # source_query_dq_res
                                     None,  # final_query_dq_res
                                     {"rules": {"num_dq_rules": 0, "num_row_dq_rules": 0},
                                      "query_dq_rules": {"num_final_query_dq_rules": 0, "num_source_query_dq_rules": 0,
                                                         "num_query_dq_rules": 0},
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 0, "num_agg_dq_rules": 0,
                                                       "num_final_agg_dq_rules": 0}},  # dq rules
                                     {"row_dq_status": "Skipped", "source_agg_dq_status": "Failed",
                                      "final_agg_dq_status": "Skipped", "run_status": "Failed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Skipped"}
                             ),
                             (
                                     # Test case 16
                                     # In this test case, dq run set for query_dq source_query_dq
                                     # with action_if_failed (ignore) for query_dq
                                     # collect stats in the test_stats_table & error into error_table
                                     spark.createDataFrame(
                                         [
                                             # sum of col1 must be greater than 10(ignore)
                                             # standard deviation of col3 must be greater than 0(ignore)
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             # row doesn't meet row_dq expectation1(drop)
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             # row meets all row_dq_expectations
                                             {"col1": 3, "col2": "c", "col3": 6},
                                             # row meets all row_dq_expectations
                                         ]
                                     ),
                                     {  # expectations rules
                                         "query_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "query_dq",
                                             "rule": "sum_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "(select sum(col1) from test_table) > 10",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "ignore",
                                             "tag": "validity",
                                             "description": "sum of col1 value must be greater than 10"
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "target_table_name": "dq_spark.test_table",
                                                 "rule_type": "query_dq",
                                                 "rule": "stddev_col3_threshold",
                                                 "column_name": "col3",
                                                 "expectation": "(select stddev(col3) from test_table) > 0",
                                                 "enable_for_source_dq_validation": True,
                                                 "enable_for_target_dq_validation": True,
                                                 "action_if_failed": "ignore",
                                                 "tag": "validity",
                                                 "description": "stddev of col3 value must be greater than 0"
                                             }
                                         ],
                                         "target_table_name": "dq_spark.test_final_table"

                                     },
                                     False,  # write to table
                                     False,  # write to temp table
                                     False,  # row_dq
                                     False,  # agg_dq
                                     False,  # source_agg_dq
                                     False,  # final_agg_dq
                                     True,  # query_dq
                                     True,  # source_query_dq
                                     False,  # final_query_dq
                                     None,  # expected result
                                     3,  # input count
                                     0,  # error count
                                     0,  # output count
                                     None,  # source_agg_result
                                     None,  # final_agg_result
                                     # final_agg_result
                                     [{"description": "sum of col1 value must be greater than 10",
                                       "rule": "sum_col1_threshold",
                                       "rule_type": "query_dq", "action_if_failed": "ignore", "tag": "validity"}],
                                     # source_query_dq_res
                                     None,  # final_query_dq_res
                                     {"rules": {"num_dq_rules": 2, "num_row_dq_rules": 0},
                                      "query_dq_rules": {"num_final_query_dq_rules": 2, "num_source_query_dq_rules": 2,
                                                         "num_query_dq_rules": 2},  # dq_rules
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 0, "num_agg_dq_rules": 0,
                                                       "num_final_agg_dq_rules": 0}},
                                     {"row_dq_status": "Skipped", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Passed",
                                      "source_query_dq_status": "Passed", "final_query_dq_status": "Skipped"}  # status

                             ),

                             (
                                     # Test case 17
                                     # In this test case, dq run set for query_dq final_query_dq
                                     # with action_if_failed (ignore) for query_dq
                                     # collect stats in the test_stats_table & error into error_table
                                     spark.createDataFrame(
                                         [
                                             # max of col1 must be greater than 10(ignore)
                                             # min of col3 must be greater than 0(ignore)
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             # row doesn't meet row_dq expectation1(drop) and expectation2(ignore)
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             # row meets all row_dq_expectations
                                             {"col1": 3, "col2": "c", "col3": 6},
                                             # row meets all row_dq_expectations
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col3_and_col1_threshold_4",
                                             "column_name": "col3, col1",
                                             "expectation": "((col3 * col1) - col3) > 5",
                                             "action_if_failed": "drop",
                                             "tag": "validity",
                                             "description": "col3 and col1 operation value must be greater than 3",
                                             "enable_error_drop_alert": True,
                                             "error_drop_threshold": "20",
                                         }],
                                         "query_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "query_dq",
                                             "rule": "max_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "(select max(col1) from target_test_table) > 10",
                                             "enable_for_source_dq_validation": False,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "ignore",
                                             "tag": "strict",
                                             "description": "max of col1 value must be greater than 10",

                                         },
                                             {
                                                 "product_id": "product1",
                                                 "target_table_name": "dq_spark.test_table",
                                                 "rule_type": "query_dq",
                                                 "rule": "min_col3_threshold",
                                                 "column_name": "col3",
                                                 "expectation": "(select min(col3) from target_test_table) > 0",
                                                 "enable_for_source_dq_validation": False,
                                                 "enable_for_target_dq_validation": True,
                                                 "action_if_failed": "ignore",
                                                 "tag": "validity",
                                                 "description": "min of col3 value must be greater than 0"
                                             }
                                         ],
                                         "target_table_name": "dq_spark.test_final_table"

                                     },
                                     True,  # write to table
                                     False,  # write to temp table
                                     True,  # row_dq
                                     False,  # agg_dq
                                     False,  # source_agg_dq
                                     False,  # final_agg_dq
                                     True,  # query_dq
                                     False,  # source_query_dq
                                     True,  # final_query_dq
                                     spark.createDataFrame(
                                         [
                                             {"col1": 3, "col2": "c", "col3": 6},
                                         ]),  # expected result
                                     3,  # input count
                                     2,  # error count
                                     1,  # output count
                                     None,  # source_agg_result
                                     None,  # final_agg_result
                                     # final_agg_result
                                     None,  # source_query_dq_res
                                     [{"description": "max of col1 value must be greater than 10",
                                       "rule": "max_col1_threshold",
                                       "rule_type": "query_dq", "action_if_failed": "ignore", "tag": "strict"}],
                                     # final_query_dq_res
                                     {"rules": {"num_dq_rules": 3, "num_row_dq_rules": 1},
                                      "query_dq_rules": {"num_final_query_dq_rules": 2, "num_source_query_dq_rules": 0,
                                                         "num_query_dq_rules": 2},  # dq_rules
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 0, "num_agg_dq_rules": 0,
                                                       "num_final_agg_dq_rules": 0}},
                                     {"row_dq_status": "Passed", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Passed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Passed"}  # status

                             ),
                             (
                                     # Test case 18
                                     # In this test case, dq run set for query_dq source_query_dq(ignore, fail)
                                     # with action_if_failed (fail) for query_dq
                                     # collect stats in the test_stats_table, error into error_table & raises the error
                                     spark.createDataFrame(
                                         [
                                             # min of col1 must be greater than 10(fail)
                                             # standard deviation of col3 must be greater than 0(ignore)
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             # row doesn't meet row_dq expectation1(drop)
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             # row meets all row_dq_expectations
                                             {"col1": 3, "col2": "c", "col3": 6},
                                             # row meets all row_dq_expectations
                                         ]
                                     ),
                                     {  # expectations rules
                                         "query_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "query_dq",
                                             "rule": "min_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "(select min(col1) from test_table) > 10",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": False,
                                             "action_if_failed": "fail",
                                             "tag": "validity",
                                             "description": "min of col1 value must be greater than 10"
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "target_table_name": "dq_spark.test_table",
                                                 "rule_type": "query_dq",
                                                 "rule": "stddev_col3_threshold",
                                                 "column_name": "col3",
                                                 "expectation": "(select stddev(col3) from test_table) > 0",
                                                 "enable_for_source_dq_validation": True,
                                                 "enable_for_target_dq_validation": False,
                                                 "action_if_failed": "ignore",
                                                 "tag": "validity",
                                                 "description": "stddev of col3 value must be greater than 0"
                                             }
                                         ],
                                         "target_table_name": "dq_spark.test_final_table"

                                     },
                                     False,  # write to table
                                     False,  # write to temp table
                                     False,  # row_dq
                                     False,  # agg_dq
                                     False,  # source_agg_dq
                                     False,  # final_agg_dq
                                     True,  # query_dq
                                     True,  # source_query_dq
                                     False,  # final_query_dq
                                     SparkExpectationsMiscException,  # expected result
                                     3,  # input count
                                     0,  # error count
                                     0,  # output count
                                     None,  # source_agg_result
                                     None,  # final_agg_result
                                     # final_agg_result
                                     [{"description": "min of col1 value must be greater than 10",
                                       "rule": "min_col1_threshold",
                                       "rule_type": "query_dq", "action_if_failed": "fail", "tag": "validity"}],
                                     # source_query_dq_res
                                     None,  # final_query_dq_res
                                     {"rules": {"num_dq_rules": 2, "num_row_dq_rules": 0},
                                      "query_dq_rules": {"num_final_query_dq_rules": 2, "num_source_query_dq_rules": 2,
                                                         "num_query_dq_rules": 2},  # dq_rules
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 0, "num_agg_dq_rules": 0,
                                                       "num_final_agg_dq_rules": 0}},
                                     {"row_dq_status": "Skipped", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Failed",
                                      "source_query_dq_status": "Failed", "final_query_dq_status": "Skipped"}  # status

                             ),

                             (
                                     # Test case 19
                                     # In this test case, dq run set for query_dq final_query_dq(ignore, fail)
                                     # with action_if_failed (ignore, fail) for query_dq
                                     # collect stats in the test_stats_table, error into error_table & raises error
                                     spark.createDataFrame(
                                         [
                                             # max of col1 must be greater than 10(ignore)
                                             # min of col3 must be greater than 0(ignore)
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             # row doesn't meet row_dq expectation1(drop) and expectation2(ignore)
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             # row meets all row_dq_expectations
                                             {"col1": 3, "col2": "c", "col3": 6},
                                             # row meets all row_dq_expectations
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col3_and_col1_threshold_4",
                                             "column_name": "col3, col1",
                                             "expectation": "((col3 * col1) - col3) > 5",
                                             "action_if_failed": "drop",
                                             "tag": "validity",
                                             "description": "col3 and col1 operation value must be greater than 3",
                                             "enable_error_drop_alert": True,
                                             "error_drop_threshold": "25",
                                         }],
                                         "query_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "query_dq",
                                             "rule": "max_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "(select max(col1) from target_test_table) > 10",
                                             "enable_for_source_dq_validation": False,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "fail",
                                             "tag": "strict",
                                             "description": "max of col1 value must be greater than 10"
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "target_table_name": "dq_spark.test_table",
                                                 "rule_type": "query_dq",
                                                 "rule": "min_col3_threshold",
                                                 "column_name": "col3",
                                                 "expectation": "(select min(col3) from target_test_table) > 0",
                                                 "enable_for_source_dq_validation": False,
                                                 "enable_for_target_dq_validation": True,
                                                 "action_if_failed": "ignore",
                                                 "tag": "validity",
                                                 "description": "min of col3 value must be greater than 0"
                                             }
                                         ],
                                         "target_table_name": "dq_spark.test_final_table"

                                     },
                                     True,  # write to table
                                     False,  # write to temp table
                                     True,  # row_dq
                                     False,  # agg_dq
                                     False,  # source_agg_dq
                                     False,  # final_agg_dq
                                     True,  # query_dq
                                     False,  # source_query_dq
                                     True,  # final_query_dq
                                     SparkExpectationsMiscException,  # expected result
                                     3,  # input count
                                     2,  # error count
                                     1,  # output count
                                     None,  # source_agg_result
                                     None,  # final_agg_result
                                     # final_agg_result
                                     None,  # source_query_dq_res
                                     [{"description": "max of col1 value must be greater than 10",
                                       "rule": "max_col1_threshold",
                                       "rule_type": "query_dq", "action_if_failed": "fail", "tag": "strict"}],
                                     # final_query_dq_res
                                     {"rules": {"num_dq_rules": 3, "num_row_dq_rules": 1},
                                      "query_dq_rules": {"num_final_query_dq_rules": 2, "num_source_query_dq_rules": 0,
                                                         "num_query_dq_rules": 2},  # dq_rules
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 0, "num_agg_dq_rules": 0,
                                                       "num_final_agg_dq_rules": 0}},
                                     {"row_dq_status": "Passed", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Failed",
                                      "source_query_dq_status": "Skipped", "final_query_dq_status": "Failed"}  # status

                             ),

                             (
                                     # Test case 19
                                     # In this test case, dq run set for query_dq source_query_dq &
                                     # final_query_dq(ignore, fail)
                                     # with action_if_failed (ignore, fail) for query_dq
                                     # collect stats in the test_stats_table, error into error_table
                                     spark.createDataFrame(
                                         [
                                             # min of col1 must be greater than 10(ignore) - source_query_dq
                                             # max of col1 must be greater than 100(ignore) - final_query_dq
                                             # min of col3 must be greater than 0(fail) - final_query_dq
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             # row meets all row_dq_expectations(drop)
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             # ow doesn't meet row_dq expectation1(drop)
                                             {"col1": 3, "col2": "c", "col3": 6},
                                             # row meets all row_dq_expectations
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col3_mod_2",
                                             "column_name": "col3",
                                             "expectation": "(col3 % 2) = 0",
                                             "action_if_failed": "drop",
                                             "tag": "validity",
                                             "description": "col3 mod must equals to 0",
                                             "enable_error_drop_alert": True,
                                             "error_drop_threshold": "40",
                                         }],
                                         "query_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "query_dq",
                                             "rule": "min_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "(select min(col1) from test_table) > 10",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": False,
                                             "action_if_failed": "ignore",
                                             "tag": "strict",
                                             "description": "min of col1 value must be greater than 10"
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "target_table_name": "dq_spark.test_table",
                                                 "rule_type": "query_dq",
                                                 "rule": "max_col1_threshold",
                                                 "column_name": "col1",
                                                 "expectation": "(select max(col1) from target_test_table) > 100",
                                                 "enable_for_source_dq_validation": False,
                                                 "enable_for_target_dq_validation": True,
                                                 "action_if_failed": "ignore",
                                                 "tag": "strict",
                                                 "description": "max of col1 value must be greater than 100"
                                             },
                                             {
                                                 "product_id": "product1",
                                                 "target_table_name": "dq_spark.test_table",
                                                 "rule_type": "query_dq",
                                                 "rule": "min_col3_threshold",
                                                 "column_name": "col3",
                                                 "expectation": "(select min(col3) from target_test_table) > 0",
                                                 "enable_for_source_dq_validation": False,
                                                 "enable_for_target_dq_validation": True,
                                                 "action_if_failed": "fail",
                                                 "tag": "validity",
                                                 "description": "min of col3 value must be greater than 0"
                                             }
                                         ],
                                         "target_table_name": "dq_spark.test_final_table"

                                     },
                                     True,  # write to table
                                     False,  # write to temp table
                                     True,  # row_dq
                                     False,  # agg_dq
                                     False,  # source_agg_dq
                                     False,  # final_agg_dq
                                     True,  # query_dq
                                     True,  # source_query_dq
                                     True,  # final_query_dq
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 3, "col2": "c", "col3": 6},
                                         ]
                                     ),  # expected result
                                     3,  # input count
                                     1,  # error count
                                     2,  # output count
                                     None,  # source_agg_result
                                     None,  # final_agg_result
                                     # final_agg_result
                                     [{"description": "min of col1 value must be greater than 10",
                                       "rule": "min_col1_threshold",
                                       "rule_type": "query_dq", "action_if_failed": "ignore", "tag": "strict"}],
                                     # source_query_dq_res
                                     [{"description": "max of col1 value must be greater than 100",
                                       "rule": "max_col1_threshold",
                                       "rule_type": "query_dq", "action_if_failed": "ignore", "tag": "strict"}],
                                     # final_query_dq_res
                                     {"rules": {"num_dq_rules": 4, "num_row_dq_rules": 1},
                                      "query_dq_rules": {"num_final_query_dq_rules": 2, "num_source_query_dq_rules": 1,
                                                         "num_query_dq_rules": 3},  # dq_rules
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 0, "num_agg_dq_rules": 0,
                                                       "num_final_agg_dq_rules": 0}},
                                     {"row_dq_status": "Passed", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Passed",
                                      "source_query_dq_status": "Passed", "final_query_dq_status": "Passed"}  # status

                             ),

                             (
                                     # Test case 20
                                     # In this test case, dq run set for query_dq source_query_dq &
                                     # final_query_dq(ignore, fail)
                                     # with action_if_failed (ignore, fail) for query_dq
                                     # collect stats in the test_stats_table, error into error_table & raise the error
                                     spark.createDataFrame(
                                         [
                                             # min of col1 must be greater than 10(ignore) - source_query_dq
                                             # max of col1 must be greater than 100(fail) - final_query_dq
                                             # min of col3 must be greater than 0(fail) - final_query_dq
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             # row meets all row_dq_expectations(drop)
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             # ow doesn't meet row_dq expectation1(drop)
                                             {"col1": 3, "col2": "c", "col3": 6},
                                             # row meets all row_dq_expectations
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col3_mod_2",
                                             "column_name": "col3",
                                             "expectation": "(col3 % 2) = 0",
                                             "action_if_failed": "drop",
                                             "tag": "validity",
                                             "description": "col3 mod must equals to 0",
                                             "enable_error_drop_alert": False,
                                             "error_drop_threshold": "10",
                                         }],
                                         "query_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "query_dq",
                                             "rule": "min_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "(select min(col1) from test_table) > 10",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": False,
                                             "action_if_failed": "ignore",
                                             "tag": "strict",
                                             "description": "min of col1 value must be greater than 10"
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "target_table_name": "dq_spark.test_table",
                                                 "rule_type": "query_dq",
                                                 "rule": "max_col1_threshold",
                                                 "column_name": "col1",
                                                 "expectation": "(select max(col1) from target_test_table) > 100",
                                                 "enable_for_source_dq_validation": False,
                                                 "enable_for_target_dq_validation": True,
                                                 "action_if_failed": "fail",
                                                 "tag": "strict",
                                                 "description": "max of col1 value must be greater than 100"
                                             },
                                             {
                                                 "product_id": "product1",
                                                 "target_table_name": "dq_spark.test_table",
                                                 "rule_type": "query_dq",
                                                 "rule": "min_col3_threshold",
                                                 "column_name": "col3",
                                                 "expectation": "(select min(col3) from target_test_table) > 0",
                                                 "enable_for_source_dq_validation": False,
                                                 "enable_for_target_dq_validation": True,
                                                 "action_if_failed": "ignore",
                                                 "tag": "validity",
                                                 "description": "min of col3 value must be greater than 0"
                                             }
                                         ],
                                         "target_table_name": "dq_spark.test_final_table"

                                     },
                                     True,  # write to table
                                     False,  # write to temp table
                                     True,  # row_dq
                                     False,  # agg_dq
                                     False,  # source_agg_dq
                                     False,  # final_agg_dq
                                     True,  # query_dq
                                     True,  # source_query_dq
                                     True,  # final_query_dq
                                     SparkExpectationsMiscException,  # expected result
                                     3,  # input count
                                     1,  # error count
                                     2,  # output count
                                     None,  # source_agg_result
                                     None,  # final_agg_result
                                     # final_agg_result
                                     [{"description": "min of col1 value must be greater than 10",
                                       "rule": "min_col1_threshold",
                                       "rule_type": "query_dq", "action_if_failed": "ignore", "tag": "strict"}],
                                     # source_query_dq_res
                                     [{"description": "max of col1 value must be greater than 100",
                                       "rule": "max_col1_threshold",
                                       "rule_type": "query_dq", "action_if_failed": "fail", "tag": "strict"}],
                                     # final_query_dq_res
                                     {"rules": {"num_dq_rules": 4, "num_row_dq_rules": 1},
                                      "query_dq_rules": {"num_final_query_dq_rules": 2, "num_source_query_dq_rules": 1,
                                                         "num_query_dq_rules": 3},  # dq_rules
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 0, "num_agg_dq_rules": 0,
                                                       "num_final_agg_dq_rules": 0}},
                                     {"row_dq_status": "Passed", "source_agg_dq_status": "Skipped",
                                      "final_agg_dq_status": "Skipped", "run_status": "Failed",
                                      "source_query_dq_status": "Passed", "final_query_dq_status": "Failed"}  # status

                             ),
                             (
                                     # Test case 20
                                     # In this test case, dq run set for query_dq source_query_dq &
                                     # final_query_dq(ignore, fail)
                                     # with action_if_failed (ignore, fail) for query_dq
                                     # collect stats in the test_stats_table, error into error_table & raise the error
                                     spark.createDataFrame(
                                         [
                                             # min of col1 must be greater than 10(ignore) - source_query_dq
                                             # max of col1 must be greater than 100(fail) - final_query_dq
                                             # min of col3 must be greater than 0(fail) - final_query_dq
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             # row meets all row_dq_expectations(drop)
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             # ow doesn't meet row_dq expectation1(drop)
                                             {"col1": 3, "col2": "c", "col3": 6},
                                             # row meets all row_dq_expectations
                                         ]
                                     ),
                                     {  # expectations rules
                                         "agg_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "agg_dq",
                                             "rule": "col3_max_value",
                                             "column_name": "col3",
                                             "expectation": "max(col3) > 1",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "fail",
                                             "tag": "validity",
                                             "description": "col3 mod must equals to 0"
                                         }],
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col3_mod_2",
                                             "column_name": "col3",
                                             "expectation": "(col3 % 2) = 0",
                                             "action_if_failed": "drop",
                                             "tag": "validity",
                                             "description": "col3 mod must equals to 0",
                                             "enable_error_drop_alert": False,
                                             "error_drop_threshold": "100",
                                         }],
                                         "query_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "query_dq",
                                             "rule": "count_col1_threshold",
                                             "column_name": "col1",
                                             "expectation": "(select count(col1) from test_table) > 3",
                                             "enable_for_source_dq_validation": True,
                                             "enable_for_target_dq_validation": False,
                                             "action_if_failed": "ignore",
                                             "tag": "strict",
                                             "description": "count of col1 value must be greater than 3"
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "target_table_name": "dq_spark.test_table",
                                                 "rule_type": "query_dq",
                                                 "rule": "col3_positive_threshold",
                                                 "column_name": "col1",
                                                 "expectation": "(select count(case when col3>0 then 1 else 0 end) from target_test_table) > 10",
                                                 "enable_for_source_dq_validation": False,
                                                 "enable_for_target_dq_validation": True,
                                                 "action_if_failed": "ignore",
                                                 "tag": "strict",
                                                 "description": "count of col3 positive value must be greater than 10"
                                             }
                                         ],
                                         "target_table_name": "dq_spark.test_final_table"

                                     },
                                     True,  # write to table
                                     False,  # write to temp table
                                     True,  # row_dq
                                     True,  # agg_dq
                                     True,  # source_agg_dq
                                     True,  # final_agg_dq
                                     True,  # query_dq
                                     True,  # source_query_dq
                                     True,  # final_query_dq
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             {"col1": 3, "col2": "c", "col3": 6},
                                         ]
                                     ),  # expected result
                                     3,  # input count
                                     1,  # error count
                                     2,  # output count
                                     None,  # source_agg_result
                                     None,  # final_agg_result
                                     # final_agg_result
                                     [{"description": "count of col1 value must be greater than 3",
                                       "rule": "count_col1_threshold",
                                       "rule_type": "query_dq", "action_if_failed": "ignore", "tag": "strict"}],
                                     # source_query_dq_res
                                     [{"description": "count of col3 positive value must be greater than 10",
                                       "rule": "col3_positive_threshold",
                                       "rule_type": "query_dq", "action_if_failed": "ignore", "tag": "strict"}],
                                     # final_query_dq_res
                                     {"rules": {"num_dq_rules": 4, "num_row_dq_rules": 1},
                                      "query_dq_rules": {"num_final_query_dq_rules": 1, "num_source_query_dq_rules": 1,
                                                         "num_query_dq_rules": 2},  # dq_rules
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 1, "num_agg_dq_rules": 1,
                                                       "num_final_agg_dq_rules": 1}},
                                     {"row_dq_status": "Passed", "source_agg_dq_status": "Passed",
                                      "final_agg_dq_status": "Passed", "run_status": "Passed",
                                      "source_query_dq_status": "Passed", "final_query_dq_status": "Passed"}  # status
                             )
                         ])
def test_with_expectations(input_df,
                           expectations,
                           write_to_table,
                           write_to_temp_table,
                           row_dq,
                           agg_dq,
                           source_agg_dq,
                           final_agg_dq,
                           query_dq,
                           source_query_dq,
                           final_query_dq,
                           expected_output,
                           input_count,
                           error_count,
                           output_count,
                           source_agg_dq_res,
                           final_agg_dq_res,
                           source_query_dq_res,
                           final_query_dq_res,
                           dq_rules,
                           status,
                           _fixture_spark_expectations,
                           _fixture_context,
                           _fixture_create_stats_table,
                           _fixture_local_kafka_topic):
    spark.conf.set("spark.sql.session.timeZone", "Etc/UTC")
    spark_conf = {"spark.sql.session.timeZone": "Etc/UTC"}
    options = {'mode': 'overwrite', "format": "delta"}
    options_error_table = {'mode': 'overwrite', "format": "delta"}

    input_df.createOrReplaceTempView("test_table")

    _fixture_context._num_row_dq_rules = (dq_rules.get("rules").get("num_row_dq_rules"))
    _fixture_context._num_dq_rules = (dq_rules.get("rules").get("num_dq_rules"))
    _fixture_context._num_agg_dq_rules = (dq_rules.get("agg_dq_rules"))
    _fixture_context._num_query_dq_rules = (dq_rules.get("query_dq_rules"))

    # Decorate the mock function with required args
    @_fixture_spark_expectations.with_expectations(
        expectations,
        write_to_table,
        write_to_temp_table,
        row_dq,
        user_conf={**spark_conf, **{user_config.se_notifications_on_fail: False}},
        options=options,
        options_error_table=options_error_table,
    )
    def get_dataset() -> DataFrame:
        return input_df

    input_df.show(truncate=False)

    if isinstance(expected_output, type) and issubclass(expected_output, Exception):
        with pytest.raises(expected_output, match=r"error occurred while processing spark expectations .*"):
            get_dataset()  # decorated_func()

        if status.get("final_agg_dq_status") == 'Failed' or status.get("final_query_dq_status") == 'Failed':
            try:
                spark.table("dq_spark.test_final_table")
                assert False
            except Exception as e:
                assert True

    else:
        get_dataset()  # decorated_func()

        if row_dq is True and write_to_table is True:
            expected_output_df = expected_output.withColumn("run_id", lit("product1_run_test")) \
                .withColumn("run_date", to_timestamp(lit("2022-12-27 10:39:44")))

            error_table = spark.table("dq_spark.test_final_table_error")
            result_df = spark.table("dq_spark.test_final_table")
            result_df.show(truncate=False)

            assert result_df.orderBy("col2").collect() == expected_output_df.orderBy("col2").collect()
            assert error_table.count() == error_count

    stats_table = spark.table("test_dq_stats_table")
    row = stats_table.first()
    assert stats_table.count() == 1
    assert row.product_id == "product1"
    assert row.table_name == "dq_spark.test_final_table"
    assert row.input_count == input_count
    assert row.error_count == error_count
    assert row.output_count == output_count
    assert row.source_agg_dq_results == source_agg_dq_res
    assert row.final_agg_dq_results == final_agg_dq_res
    assert row.source_query_dq_results == source_query_dq_res
    assert row.final_query_dq_results == final_query_dq_res
    assert row.dq_status.get("source_agg_dq") == status.get("source_agg_dq_status")
    assert row.dq_status.get("row_dq") == status.get("row_dq_status")
    assert row.dq_status.get("final_agg_dq") == status.get("final_agg_dq_status")
    assert row.dq_status.get("source_query_dq") == status.get("source_query_dq_status")
    assert row.dq_status.get("final_query_dq") == status.get("final_query_dq_status")
    assert row.dq_status.get("run_status") == status.get("run_status")

    assert spark.read.format("kafka").option(
        "kafka.bootstrap.servers", "localhost:9092"
    ).option("subscribe", "dq-sparkexpectations-stats").option(
        "startingOffsets", "earliest"
    ).option(
        "endingOffsets", "latest"
    ).load().orderBy(col('timestamp').desc()).limit(1).selectExpr(
        "cast(value as string) as value").collect() == stats_table.selectExpr("to_json(struct(*)) AS value").collect()

    spark.sql("drop table if exists test_final_table_error")
    os.system("rm -rf /tmp/hive/warehouse/dq_spark.db/test_final_table_error")


# @pytest.mark.parametrize("write_to_table", [(True), (False)])
# def test_with_table_write_expectations(
#         write_to_table,
#         _fixture_create_database,
#         _fixture_df,
#         _fixture_expectations,
#         _fixture_context,
#         _fixture_dq_rules,
#         _fixture_spark_expectations,
#         _fixture_create_stats_table,
#         _fixture_local_kafka_topic):
#     _fixture_context._num_row_dq_rules = (_fixture_dq_rules.get("rules").get("num_row_dq_rules"))
#     _fixture_context._num_dq_rules = (_fixture_dq_rules.get("rules").get("num_dq_rules"))
#     _fixture_context._num_agg_dq_rules = (_fixture_dq_rules.get("agg_dq_rules"))
#     _fixture_context._num_query_dq_rules = (_fixture_dq_rules.get("query_dq_rules"))
#
#     # Create a mock object with a return value
#     mock_func = Mock(return_value=_fixture_df)
#
#     # Decorate the mock function with required args
#     decorated_func = _fixture_spark_expectations.with_expectations(
#         _fixture_expectations,
#         write_to_table,
#         agg_dq=None,
#         query_dq=None,
#         spark_conf={UserConfig.se_notifications_on_fail: False},
#         options={'mode': 'overwrite', "format": "delta"},
#         options_error_table={'mode': 'overwrite', "format": "delta"}
#     )(mock_func)
#
#     decorated_func()
#
#     if write_to_table is True:
#         assert "test_final_table" in [obj.name for obj in spark.catalog.listTables()]
#         spark.sql("drop table if exists dq_spark.test_final_table")
#     else:
#         assert "test_final_table" not in [obj.name for obj in spark.catalog.listTables()]


@patch("spark_expectations.core.expectations.SparkExpectationsWriter.write_error_stats")
def test_with_expectations_patch(_write_error_stats,
                                 _fixture_create_database,
                                 _fixture_spark_expectations,
                                 _fixture_context,
                                 _fixture_dq_rules,
                                 _fixture_df,
                                 _fixture_expectations):
    _fixture_context._num_row_dq_rules = (_fixture_dq_rules.get("rules").get("num_row_dq_rules"))
    _fixture_context._num_dq_rules = (_fixture_dq_rules.get("rules").get("num_dq_rules"))
    _fixture_context._num_agg_dq_rules = (_fixture_dq_rules.get("agg_dq_rules"))
    _fixture_context._num_query_dq_rules = (_fixture_dq_rules.get("query_dq_rules"))

    decorated_func = _fixture_spark_expectations.with_expectations(
        _fixture_expectations,
        True,
        agg_dq=None,
        query_dq=None,
        user_conf={user_config.se_notifications_on_fail: False},
        options={'mode': 'overwrite', "format": "delta"},
        options_error_table={'mode': 'overwrite', "format": "delta"}
    )(Mock(return_value=_fixture_df))

    decorated_func()

    _write_error_stats.assert_called_once_with()


def test_with_expectations_dataframe_not_returned_exception(_fixture_create_database,
                                                            _fixture_spark_expectations,
                                                            _fixture_df,
                                                            _fixture_expectations,
                                                            _fixture_local_kafka_topic):
    partial_func = _fixture_spark_expectations.with_expectations(
        _fixture_expectations,
        user_conf={user_config.se_notifications_on_fail: False},
    )

    with pytest.raises(SparkExpectationsMiscException,
                       match=r"error occurred while processing spark expectations error occurred while"
                             r" processing spark "
                             r"expectations due to given dataframe is not type of dataframe"):
        # Create a mock object with a rdd return value
        mock_func = Mock(return_value=_fixture_df.rdd)

        # Decorate the mock function with required args
        decorated_func = partial_func(mock_func)
        decorated_func()


def test_with_expectations_exception(_fixture_create_database,
                                     _fixture_spark_expectations,
                                     _fixture_df,
                                     _fixture_expectations,
                                     _fixture_create_stats_table,
                                     _fixture_local_kafka_topic):
    partial_func = _fixture_spark_expectations.with_expectations(
        _fixture_expectations,
        user_conf={user_config.se_notifications_on_fail: False}
    )

    with pytest.raises(SparkExpectationsMiscException,
                       match=r"error occurred while processing spark expectations .*"):
        # Create a mock object with a list return value
        mock_func = Mock(return_value=["apple", "banana", "pineapple", "orange"])

        # Decorate the mock function with required args
        decorated_func = partial_func(mock_func)
        decorated_func()


@pytest.mark.parametrize("input_df, "
                         "expectations, "
                         "write_to_table, "
                         "write_to_temp_table, "
                         "row_dq, agg_dq, "
                         "source_agg_dq, "
                         "final_agg_dq,"
                         "dq_rules ",
                         [
                             (
                                     # In this test case, the action for failed rows is "ignore" & "drop",
                                     # collect stats in the test_stats_table and
                                     # log the error records into the error table.
                                     spark.createDataFrame(
                                         [
                                             {"col1": 1, "col2": "a", "col3": 4},
                                             # row doesn't meet expectations1(ignore) 2(drop), log into err & fnl
                                             {"col1": 2, "col2": "b", "col3": 5},
                                             # row meets expectations1(ignore), log into final table
                                             {"col1": 3, "col2": "c", 'col3': 6},
                                             # row doesnt'meets expectations1(ignore), log into final table
                                         ]
                                     ),
                                     {  # expectations rules
                                         "row_dq_rules": [{
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "row_dq",
                                             "rule": "col3_threshold",
                                             "column_name": "col3",
                                             "expectation": "col3 > 6",
                                             "action_if_failed": "ignore",
                                             "tag": "strict",
                                             "description": "col3 value must be greater than 6",
                                             "enable_error_drop_alert": True,
                                             "error_drop_threshold": "25",
                                         },
                                             {
                                                 "product_id": "product1",
                                                 "target_table_name": "dq_spark.test_table",
                                                 "rule_type": "row_dq",
                                                 "rule": "col1_add_col3_threshold",
                                                 "column_name": "col1",
                                                 "expectation": "(col1+col3) > 6",
                                                 "action_if_failed": "drop",
                                                 "tag": "strict",
                                                 "description": "col1_add_col3 value must be greater than 6",
                                                 "enable_error_drop_alert": True,
                                                 "error_drop_threshold": "50",
                                             }
                                         ],
                                         "agg_dq_rules": [{}],
                                         "target_table_name": "dq_spark.test_final_table"

                                     },
                                     True,  # write to table
                                     True,  # write to temp table
                                     True,  # row_dq
                                     False,  # agg_dq
                                     False,  # source_agg_dq
                                     False,  # final_agg_dq_res
                                     {"rules": {"num_dq_rules": 0, "num_row_dq_rules": 0},
                                      "query_dq_rules": {"num_final_query_dq_rules": 0, "num_source_query_dq_rules": 0,
                                                         "num_query_dq_rules": 0},
                                      "agg_dq_rules": {"num_source_agg_dq_rules": 0, "num_agg_dq_rules": 0,
                                                       "num_final_agg_dq_rules": 0}}  # dq_rules
                             )
                         ])
@patch('spark_expectations.core.expectations.SparkExpectationsNotify', autospec=True,
       spec_set=True)
@patch('spark_expectations.notifications.push.spark_expectations_notify._notification_hook', autospec=True,
       spec_set=True)
def test_error_threshold_breach(_mock_notification_hook, _mock_spark_expectations_notify, input_df,
                                expectations,
                                write_to_table,
                                write_to_temp_table,
                                row_dq,
                                agg_dq,
                                source_agg_dq,
                                final_agg_dq,
                                dq_rules,
                                _fixture_spark_expectations,
                                _fixture_context,
                                _fixture_create_stats_table,
                                _fixture_local_kafka_topic):
    spark.conf.set("spark.sql.session.timeZone", "Etc/UTC")
    spark_conf = {"spark.sql.session.timeZone": "Etc/UTC"}
    options = {'mode': 'overwrite', "format": "delta"}
    options_error_table = {'mode': 'overwrite', "format": "delta"}

    # set neccessary parameters in context class or object
    _fixture_context._num_row_dq_rules = (dq_rules.get("rules").get("num_row_dq_rules"))
    _fixture_context._num_dq_rules = (dq_rules.get("rules").get("num_dq_rules"))
    _fixture_context._num_agg_dq_rules = (dq_rules.get("agg_dq_rules"))
    _fixture_context._num_query_dq_rules = (dq_rules.get("query_dq_rules"))

    # Decorate the mock function with required args
    @_fixture_spark_expectations.with_expectations(
        expectations,
        write_to_table,
        write_to_temp_table,
        row_dq,
        agg_dq={
            user_config.se_agg_dq: agg_dq,
            user_config.se_source_agg_dq: source_agg_dq,
            user_config.se_final_agg_dq: final_agg_dq,
        },
        query_dq=None,
        user_conf={
            user_config.se_notifications_on_fail: False,
            user_config.se_notifications_on_error_drop_exceeds_threshold_breach: True,
            user_config.se_notifications_on_error_drop_threshold: 10,
        },
        options=options,
        options_error_table=options_error_table
    )
    def get_dataset() -> DataFrame:
        return input_df

    get_dataset()

    _mock_notification_hook.send_notification.assert_called_once()
    # _mock_spark_expectations_notify("product_id",
    # _fixture_context).notify_on_exceeds_of_error_threshold.assert_called_once()


@pytest.mark.parametrize("input_df, "
                         "expectations, "
                         "write_to_table, "
                         "write_to_temp_table, "
                         "row_dq, agg_dq, "
                         "source_agg_dq, "
                         "final_agg_dq, "
                         "query_dq, "
                         "source_query_dq, "
                         "final_query_dq, "
                         "expected_output, "
                         "input_count, "
                         "error_count, "
                         "output_count ",
                         [(
                                 # In this test case, dq run set for query_dq source_query_dq &
                                 # final_query_dq(ignore, fail)
                                 # with action_if_failed (ignore, fail) for query_dq
                                 # collect stats in the test_stats_table, error into error_table & raise the error
                                 spark.createDataFrame(
                                     [
                                         # min of col1 must be greater than 10(ignore) - source_query_dq
                                         # max of col1 must be greater than 100(fail) - final_query_dq
                                         # min of col3 must be greater than 0(fail) - final_query_dq
                                         {"col1": 1, "col2": "a", "col3": 4},
                                         # row meets all row_dq_expectations(drop)
                                         {"col1": 2, "col2": "b", "col3": 5},
                                         # ow doesn't meet row_dq expectation1(drop)
                                         {"col1": 3, "col2": "c", "col3": 6},
                                         # row meets all row_dq_expectations
                                     ]
                                 ),
                                 {  # expectations rules
                                     "row_dq_rules": [{
                                         "product_id": "product1",
                                         "target_table_name": "dq_spark.test_table",
                                         "rule_type": "row_dq",
                                         "rule": "col3_mod_2",
                                         "column_name": "col3",
                                         "expectation": "(col3 % 2) = 0",
                                         "action_if_failed": "drop",
                                         "tag": "validity",
                                         "description": "col3 mod must equals to 0",
                                         "enable_error_drop_alert": False,
                                         "error_drop_threshold": "10",
                                     }],
                                     "query_dq_rules": [
                                         {
                                             "product_id": "product1",
                                             "target_table_name": "dq_spark.test_table",
                                             "rule_type": "query_dq",
                                             "rule": "col3_positive_threshold",
                                             "column_name": "col1",
                                             "expectation": "(select count(case when col3>0 then 1 else 0 end) from target_test_table) > 10",
                                             "enable_for_source_dq_validation": False,
                                             "enable_for_target_dq_validation": True,
                                             "action_if_failed": "ignore",
                                             "tag": "strict",
                                             "description": "count of col3 positive value must be greater than 10"
                                         }
                                     ],
                                     "target_table_name": "dq_spark.test_final_table"

                                 },
                                 True,  # write to table
                                 False,  # write to temp table
                                 True,  # row_dq
                                 False,  # agg_dq
                                 False,  # source_agg_dq
                                 False,  # final_agg_dq
                                 True,  # query_dq
                                 False,  # source_query_dq
                                 True,  # final_query_dq
                                 spark.createDataFrame(
                                     [
                                         {"col1": 1, "col2": "a", "col3": 4},
                                         {"col1": 3, "col2": "c", "col3": 6},
                                     ]
                                 ),  # expected result
                                 3,  # input count
                                 1,  # error count
                                 2,  # output count
                         )])
def test_target_table_view_exception(input_df,
                                     expectations,
                                     write_to_table,
                                     write_to_temp_table,
                                     row_dq,
                                     agg_dq,
                                     source_agg_dq,
                                     final_agg_dq,
                                     query_dq,
                                     source_query_dq,
                                     final_query_dq,
                                     expected_output,
                                     input_count,
                                     error_count,
                                     output_count,
                                     _fixture_spark_expectations,
                                     _fixture_context,
                                     _fixture_create_stats_table,
                                     _fixture_local_kafka_topic):
    spark.conf.set("spark.sql.session.timeZone", "Etc/UTC")
    spark_conf = {"spark.sql.session.timeZone": "Etc/UTC"}
    options = {'mode': 'overwrite', "format": "delta"}
    options_error_table = {'mode': 'overwrite', "format": "delta"}

    input_df.createOrReplaceTempView("test_table")

    # Decorate the mock function with required args
    @_fixture_spark_expectations.with_expectations(
        expectations,
        write_to_table,
        write_to_temp_table,
        row_dq,
        agg_dq={
            user_config.se_agg_dq: agg_dq,
            user_config.se_source_agg_dq: source_agg_dq,
            user_config.se_final_agg_dq: final_agg_dq,
        },
        query_dq={
            user_config.se_query_dq: query_dq,
            user_config.se_source_query_dq: source_query_dq,
            user_config.se_final_query_dq: final_query_dq,
            user_config.se_target_table_view: ""
        },
        user_conf={**spark_conf, **{user_config.se_notifications_on_fail: False}},
        options=options,
        options_error_table=options_error_table,
    )
    def get_dataset() -> DataFrame:
        return input_df

    input_df.show(truncate=False)

    with pytest.raises(SparkExpectationsMiscException, match=r"error occurred while processing spark expectations .*"):
        get_dataset()  # decorated_func()


# [UnitTests for WrappedDataFrameWriter class]

@pytest.fixture
def fresh_writer():
    # Reset the class attributes before each test
    WrappedDataFrameWriter._mode = None
    WrappedDataFrameWriter._format = None
    WrappedDataFrameWriter._partition_by = []
    WrappedDataFrameWriter._options = {}
    WrappedDataFrameWriter._bucket_by = {}
    return WrappedDataFrameWriter


def test_mode(fresh_writer):
    fresh_writer.mode("overwrite")
    assert fresh_writer._mode == "overwrite"


def test_format(fresh_writer):
    fresh_writer.format("parquet")
    assert fresh_writer._format == "parquet"


def test_partitionBy(fresh_writer):
    fresh_writer.partitionBy("date", "region")
    assert fresh_writer._partition_by == ["date", "region"]


def test_option(fresh_writer):
    fresh_writer.option("compression", "gzip")
    assert fresh_writer._options == {"compression": "gzip"}


def test_options(fresh_writer):
    fresh_writer.options(path="/path/to/output", inferSchema="true")
    assert fresh_writer._options == {"path": "/path/to/output", "inferSchema": "true"}


def test_bucketBy(fresh_writer):
    fresh_writer.bucketBy(4, "country", "city")
    assert fresh_writer._bucket_by == {"num_buckets": 4, "columns": ("country", "city")}


def test_build(fresh_writer):
    writer = fresh_writer.mode("overwrite") \
            .format("parquet") \
            .partitionBy("date", "region") \
            .option("compression", "gzip") \
            .options(path="/path/to/output", inferSchema="true") \
            .bucketBy(4, "country", "city") \
            .sortBy("col1", "col2")
    expected_config = {
        "mode": "overwrite",
        "format": "parquet",
        "partitionBy": ["date", "region"],
        "options": {"compression": "gzip", "path": "/path/to/output", "inferSchema": "true"},
        "bucketBy": {"num_buckets": 4, "columns": ("country", "city")},
        "sortBy": ["col1", "col2"],
    }
    assert writer.build() == expected_config


def test_build_some_values(fresh_writer):
    writer = fresh_writer.mode("append").format("iceberg")

    expected_config = {
        "mode": "append",
        "format": "iceberg",
        "partitionBy": [],
        "options": {},
        "bucketBy": {},
        "sortBy": []
    }
    assert writer.build() == expected_config
