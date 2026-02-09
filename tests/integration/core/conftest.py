# pylint: disable=too-many-lines
"""
Conftest file containing test parameters and fixtures for test_expectations.py
"""
import os
import pytest
from pyspark.sql.types import StringType, IntegerType, StructField, StructType

from spark_expectations.core import get_spark_session
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core.expectations import SparkExpectations, WrappedDataFrameWriter
from spark_expectations.core.exceptions import SparkExpectationsMiscException

# Initialize spark session for test parameter creation
spark = get_spark_session()


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture(name="_fixture_local_kafka_topic", scope="session", autouse=True)
def fixture_setup_local_kafka_topic():
    current_dir = os.path.dirname(os.path.abspath(__file__))

    if os.getenv("UNIT_TESTING_ENV") != "spark_expectations_unit_testing_on_github_actions":
        # remove if docker container is running
        os.system(f"sh {current_dir}/../../../containers/kafka/scripts/docker_kafka_stop_script.sh")

        # start docker container and create the topic
        os.system(f"sh {current_dir}/../../../containers/kafka/scripts/docker_kafka_start_script.sh")

        yield "docker container started"

        # remove docker container
        os.system(f"sh {current_dir}/../../../containers/kafka/scripts/docker_kafka_stop_script.sh")

    else:
        yield (
            "A Kafka server has been launched within a Docker container for the purpose of conducting tests in "
            "a Jenkins environment"
        )


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
    return {
        "rules": {"num_dq_rules": 1, "num_row_dq_rules": 1},
        "query_dq_rules": {
            "num_final_query_dq_rules": 0,
            "num_source_query_dq_rules": 0,
            "num_query_dq_rules": 0,
        },
        "agg_dq_rules": {
            "num_source_agg_dq_rules": 0,
            "num_agg_dq_rules": 0,
            "num_final_agg_dq_rules": 0,
        },
    }


@pytest.fixture(name="_fixture_rules_df")
def fixture_rules_df():
    rules_dict = {
        "product_id": "product1",
        "table_name": "dq_spark.test_table",
        "rule_type": "row_dq",
        "rule": "col1_threshold",
        "column_name": "col1",
        "expectation": "col1 > 1",
        "action_if_failed": "ignore",
        "tag": "validity",
        "description": "col1 value must be greater than 1",
        "enable_for_source_dq_validation": True,
        "enable_for_target_dq_validation": True,
        "is_active": True,
        "enable_error_drop_alert": True,
        "error_drop_threshold": "10",
        "priority": "medium",
    }
    return spark.createDataFrame([rules_dict])


@pytest.fixture(name="_fixture_create_database")
def fixture_create_database():
    # drop and create dq_spark if exists
    spark.sql("DROP DATABASE IF EXISTS dq_spark CASCADE")
    os.system("rm -rf /tmp/hive/warehouse/dq_spark.db")
    spark.sql("create database if not exists dq_spark")
    spark.sql("use dq_spark")

    yield "dq_spark"

    # drop dq_spark if exists
    spark.sql("DROP DATABASE IF EXISTS dq_spark CASCADE")
    os.system("rm -rf /tmp/hive/warehouse/dq_spark.db")


@pytest.fixture(name="_fixture_context")
def fixture_context():
    _context: SparkExpectationsContext = SparkExpectationsContext("product_id", spark)
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
def fixture_spark_expectations(_fixture_rules_df):
    # create a spark expectations class object
    writer = WrappedDataFrameWriter().mode("append").format("delta")
    spark_expectations = SparkExpectations(
        product_id="product1",
        rules_df=_fixture_rules_df,
        stats_table="dq_spark.test_dq_stats_table",
        stats_table_writer=writer,
        target_and_error_table_writer=writer,
        debugger=False,
    )

    return spark_expectations


# Parameter names for test_with_expectations
TEST_WITH_EXPECTATIONS_PARAM_NAMES = (
    "input_df, "
    "expectations, "
    "write_to_table, "
    "write_to_temp_table, "
    "expected_output, "
    "input_count, "
    "error_count, "
    "output_count, "
    "source_agg_dq_res, "
    "final_agg_dq_res, "
    "source_query_dq_res, "
    "final_query_dq_res, "
    "dq_rules, "
    "status"
)


# Test case data for test_with_expectations
TEST_WITH_EXPECTATIONS_PARAMS = [
    (
        # Note: where err: refers error table and fnl: final table
        # test case 0
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
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col1_threshold",
                "column_name": "col1",
                "expectation": "col1 > 1",
                "action_if_failed": "ignore",
                "tag": "validity",
                "description": "col1 value must be greater than 1",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": True,
                "is_active": True,
                "enable_error_drop_alert": True,
                "error_drop_threshold": "10",
                "priority": "medium",
            }
        ],
        True,  # write to table
        True,  # write to temp table
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
        {
            "rules": {"num_dq_rules": 1, "num_row_dq_rules": 1},
            "query_dq_rules": {
                "num_final_query_dq_rules": 0,
                "num_source_query_dq_rules": 0,
                "num_query_dq_rules": 0,
            },
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 0,
                "num_agg_dq_rules": 0,
                "num_final_agg_dq_rules": 0,
            },
        },  # dq_rules
        # status at different stages for given input
        {
            "row_dq_status": "Passed",
            "source_agg_dq_status": "Skipped",
            "final_agg_dq_status": "Skipped",
            "run_status": "Passed",
            "source_query_dq_status": "Skipped",
            "final_query_dq_status": "Skipped",
        },
    ),
    (
        # test case 1
        # In this test case, the action for failed rows is "drop",
        # collect stats in the test_stats_table and
        # log the error records into the error table.
        spark.createDataFrame(
            [
                {"col1": 1, "col2": "a", "col3": 4},
                # row  meets expectations(drop), log into err & fnl
                {"col1": 2, "col2": "b", "col3": 5},
                # row doesn't meets expectations(drop), log into final table
                {"col1": 3, "col2": "c", "col3": 6},
                # row meets expectations(drop), log into final table
            ]
        ),
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col2_set",
                "column_name": "col2",
                "expectation": "col2 in  ('a', 'c')",
                "action_if_failed": "drop",
                "tag": "strict",
                "description": "col2 value must be in ('a', 'b')",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": True,
                "error_drop_threshold": "5",
                "priority": "medium",
            }
        ],
        True,  # write to table
        True,  # write to temp table
        # expected res
        spark.createDataFrame(
            [
                {"col1": 1, "col2": "a", "col3": 4},
                # row doesn't meet expectations(ignore), log into err & fnl
                {"col1": 3, "col2": "c", "col3": 6},
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
        {
            "rules": {"num_dq_rules": 1, "num_row_dq_rules": 1},
            "query_dq_rules": {
                "num_final_query_dq_rules": 0,
                "num_source_query_dq_rules": 0,
                "num_query_dq_rules": 0,
            },
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 0,
                "num_agg_dq_rules": 0,
                "num_final_agg_dq_rules": 0,
            },
        },  # dq_rules
        # status at different stages for given input
        {
            "row_dq_status": "Passed",
            "source_agg_dq_status": "Skipped",
            "final_agg_dq_status": "Skipped",
            "run_status": "Passed",
            "source_query_dq_status": "Skipped",
            "final_query_dq_status": "Skipped",
        },
    ),
    (
        # test case 2
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
                {"col1": 3, "col2": "c", "col3": 6},
                # row meets doesn't expectations(fail), log into final table
            ]
        ),
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col3_threshold",
                "column_name": "col3",
                "expectation": "col3 > 6",
                "action_if_failed": "fail",
                "tag": "strict",
                "description": "col3 value must be greater than 6",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": True,
                "error_drop_threshold": "15",
                "priority": "medium",
            }
        ],
        True,  # write to table
        True,  # write to temp table
        SparkExpectationsMiscException,  # expected res
        3,  # input count
        3,  # error count
        0,  # output count
        None,  # source_agg_dq_res
        None,  # final_agg_dq_res
        None,  # source_query_dq_res
        None,  # final_query_dq_res
        {
            "rules": {"num_dq_rules": 1, "num_row_dq_rules": 1},
            "query_dq_rules": {
                "num_final_query_dq_rules": 0,
                "num_source_query_dq_rules": 0,
                "num_query_dq_rules": 0,
            },
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 0,
                "num_agg_dq_rules": 0,
                "num_final_agg_dq_rules": 0,
            },
        },  # dq_rules
        # status at different stages for given input
        {
            "row_dq_status": "Failed",
            "source_agg_dq_status": "Skipped",
            "final_agg_dq_status": "Skipped",
            "run_status": "Failed",
            "source_query_dq_status": "Skipped",
            "final_query_dq_status": "Skipped",
        },
    ),
    (  # test case 3
        # In this test case, the action for failed rows is "ignore" & "drop",
        # collect stats in the test_stats_table and
        # log the error records into the error table.
        spark.createDataFrame(
            [
                {"col1": 1, "col2": "a", "col3": 4},
                # row doesn't meet expectations1(ignore) 2(drop), log into err & fnl
                {"col1": 2, "col2": "b", "col3": 5},
                # row meets expectations1(ignore), log into final table
                {"col1": 3, "col2": "c", "col3": 6},
                # row doesnt'meets expectations1(ignore), log into final table
            ]
        ),
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col3_threshold",
                "column_name": "col3",
                "expectation": "col3 > 6",
                "action_if_failed": "ignore",
                "tag": "strict",
                "description": "col3 value must be greater than 6",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "10",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col1_add_col3_threshold",
                "column_name": "col1",
                "expectation": "(col1+col3) > 6",
                "action_if_failed": "drop",
                "tag": "strict",
                "description": "col1_add_col3 value must be greater than 6",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "15",
                "priority": "medium",
            },
        ],
        True,  # write to table
        True,  # write to temp table
        # expected res
        spark.createDataFrame(
            [
                {"col1": 2, "col2": "b", "col3": 5},
                {"col1": 3, "col2": "c", "col3": 6},
            ]
        ),
        3,  # input count
        3,  # error count
        2,  # output count
        None,  # source_agg_dq_res
        None,  # final_agg_dq_res
        None,  # source_query_dq_res
        None,  # final_query_dq_res
        {
            "rules": {"num_dq_rules": 2, "num_row_dq_rules": 2},
            "query_dq_rules": {
                "num_final_query_dq_rules": 0,
                "num_source_query_dq_rules": 0,
                "num_query_dq_rules": 0,
            },
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 0,
                "num_agg_dq_rules": 0,
                "num_final_agg_dq_rules": 0,
            },
        },  # dq_rules
        # status at different stages for given input
        {
            "row_dq_status": "Passed",
            "source_agg_dq_status": "Skipped",
            "final_agg_dq_status": "Skipped",
            "run_status": "Passed",
            "source_query_dq_status": "Skipped",
            "final_query_dq_status": "Skipped",
        },
    ),
    (
        # test case 4
        # In this test case, the action for failed rows is "ignore" & "fail",
        # collect stats in the test_stats_table and
        # log the error records into the error table.
        spark.createDataFrame(
            [
                {"col1": 1, "col2": "a", "col3": 4},
                # row doesn't meet expectations1(ignore), log into err & fnl
                {"col1": 2, "col2": "b", "col3": 5},
                # row meets expectations1(ignore), log into final table
                {"col1": 3, "col2": "c", "col3": 6},
                # row meets expectations1(ignore), log into final table
            ]
        ),
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col3_threshold",
                "column_name": "col3",
                "expectation": "col3 > 6",
                "action_if_failed": "ignore",
                "tag": "strict",
                "description": "col3 value must be greater than 6",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": True,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col3_minus_col1_threshold",
                "column_name": "col1",
                "expectation": "(col3-col1) > 1",
                "action_if_failed": "fail",
                "tag": "strict",
                "description": "col3_minus_col1 value must be greater than 1",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": True,
                "error_drop_threshold": "5",
                "priority": "medium",
            },
        ],
        True,  # write to table
        True,  # write to temp table
        # expected res
        spark.createDataFrame(
            [
                {"col1": 1, "col2": "a", "col3": 4},
                {"col1": 2, "col2": "b", "col3": 5},
                {"col1": 3, "col2": "c", "col3": 6},
            ]
        ),
        3,  # input count
        3,  # error count
        3,  # output count
        None,  # source_agg_dq_res
        None,  # final_agg_dq_res
        None,  # source_query_dq_res
        None,  # final_query_dq_res
        {
            "rules": {"num_dq_rules": 2, "num_row_dq_rules": 2},
            "query_dq_rules": {
                "num_final_query_dq_rules": 0,
                "num_source_query_dq_rules": 0,
                "num_query_dq_rules": 0,
            },
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 0,
                "num_agg_dq_rules": 0,
                "num_final_agg_dq_rules": 0,
            },
        },  # dq_rules
        # status at different stages for given input
        {
            "row_dq_status": "Passed",
            "source_agg_dq_status": "Skipped",
            "final_agg_dq_status": "Skipped",
            "run_status": "Passed",
            "source_query_dq_status": "Skipped",
            "final_query_dq_status": "Skipped",
        },
    ),
    (
        # Test case 5
        # In this test case, the action for failed rows is "drop" & "fail",
        # collect stats in the test_stats_table and
        # log the error records into the error table.
        spark.createDataFrame(
            [
                {"col1": 1, "col2": "a", "col3": 4},
                # row doesn't meet expectations1(drop) & 2(drop), log into err & fnl
                {"col1": 2, "col2": "b", "col3": 5},
                # row meets expectations1(drop) & 2(fail), log into final table
                {"col1": 3, "col2": "c", "col3": 6},
                # row meets expectations1(drop), & 2(fail) log into final table
            ]
        ),
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col3_threshold",
                "column_name": "col3",
                "expectation": "col3 > 6",
                "action_if_failed": "drop",
                "tag": "strict",
                "description": "col3 value must be greater than 6",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": True,
                "error_drop_threshold": "25",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col3_minus_col1_threshold",
                "column_name": "col1",
                "expectation": "(col3-col1) = 1",
                "action_if_failed": "fail",
                "tag": "strict",
                "description": "col3_minus_col1 value must be equals to 1",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "25",
                "priority": "medium",
            },
        ],
        True,  # write to table
        True,  # write to temp table
        SparkExpectationsMiscException,  # expected res
        3,  # input count
        3,  # error count
        0,  # output count
        None,  # source_agg_dq_res
        None,  # final_agg_dq_res
        None,  # source_query_dq_res
        None,  # final_query_dq_res
        {
            "rules": {"num_dq_rules": 2, "num_row_dq_rules": 2},
            "query_dq_rules": {
                "num_final_query_dq_rules": 0,
                "num_source_query_dq_rules": 0,
                "num_query_dq_rules": 0,
            },
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 0,
                "num_agg_dq_rules": 0,
                "num_final_agg_dq_rules": 0,
            },
        },  # dq_rules
        # status at different stages for given input
        {
            "row_dq_status": "Failed",
            "source_agg_dq_status": "Skipped",
            "final_agg_dq_status": "Skipped",
            "run_status": "Failed",
            "source_query_dq_status": "Skipped",
            "final_query_dq_status": "Skipped",
        },
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
                {"col1": 3, "col2": "c", "col3": 6},
                # row meets expectations1(drop) & meets 2(fail), log into final table
            ]
        ),
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col3_threshold",
                "column_name": "col3",
                "expectation": "col3 > 6",
                "action_if_failed": "drop",
                "tag": "strict",
                "description": "col3 value must be greater than 6",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": True,
                "error_drop_threshold": "10",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col3_mul_col1_threshold",
                "column_name": "col1",
                "expectation": "(col3*col1) > 1",
                "action_if_failed": "fail",
                "tag": "strict",
                "description": "col3_mul_col1 value must be equals to 1",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": True,
                "error_drop_threshold": "10",
                "priority": "medium",
            },
        ],
        True,  # write to table
        True,  # write to temp table
        # expected res
        spark.createDataFrame(
            [],
            schema=StructType(
                [
                    StructField("col1", IntegerType()),
                    StructField("col2", StringType()),
                    StructField("col3", IntegerType()),
                ]
            ),
        ),
        3,  # input count
        3,  # error count
        0,  # output count
        None,  # source_agg_dq_res
        None,  # final_agg_dq_res
        None,  # source_query_dq_res
        None,  # final_query_dq_res
        {
            "rules": {"num_dq_rules": 2, "num_row_dq_rules": 2},
            "query_dq_rules": {
                "num_final_query_dq_rules": 0,
                "num_source_query_dq_rules": 0,
                "num_query_dq_rules": 0,
            },
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 0,
                "num_agg_dq_rules": 0,
                "num_final_agg_dq_rules": 0,
            },
        },  # dq_rules
        # status at different stages for given input
        {
            "row_dq_status": "Passed",
            "source_agg_dq_status": "Skipped",
            "final_agg_dq_status": "Skipped",
            "run_status": "Passed",
            "source_query_dq_status": "Skipped",
            "final_query_dq_status": "Skipped",
        },
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
                {"col1": 3, "col2": "c", "col3": 6},
                # row meets all the expectations
            ]
        ),
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col1_threshold",
                "column_name": "col1",
                "expectation": "col1 > 1",
                "action_if_failed": "ignore",
                "tag": "strict",
                "description": "col1 value must be greater than 1",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "0",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col3_threshold",
                "column_name": "col3",
                "expectation": "col3 > 5",
                "action_if_failed": "drop",
                "tag": "strict",
                "description": "col3 value must be greater than 5",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": True,
                "error_drop_threshold": "10",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col3_mul_col1_threshold",
                "column_name": "col1",
                "expectation": "(col3*col1) > 1",
                "action_if_failed": "fail",
                "tag": "strict",
                "description": "col3_mul_col1 value must be equals to 1",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
        ],
        True,  # write to table
        True,  # write_to_temp_table
        spark.createDataFrame(  # expected output
            [
                {"col1": 3, "col2": "c", "col3": 6},
            ]
        ),
        3,  # input count
        2,  # error count
        1,  # output count
        None,  # source_agg_result
        None,  # final_agg_result
        None,  # source_query_dq_res
        None,  # final_query_dq_res
        {
            "rules": {"num_dq_rules": 3, "num_row_dq_rules": 3},
            "query_dq_rules": {
                "num_final_query_dq_rules": 0,
                "num_source_query_dq_rules": 0,
                "num_query_dq_rules": 0,
            },
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 0,
                "num_agg_dq_rules": 0,
                "num_final_agg_dq_rules": 0,
            },
        },  # dq_rules
        {
            "row_dq_status": "Passed",
            "source_agg_dq_status": "Skipped",  # status
            "final_agg_dq_status": "Skipped",
            "run_status": "Passed",
            "source_query_dq_status": "Skipped",
            "final_query_dq_status": "Skipped",
        },
    ),
    (
        # Test case 8
        # In this test case, dq run set for source_agg_dq
        # collect stats in the test_stats_table
        spark.createDataFrame(
            [
                {"col1": 1, "col2": "a", "col3": 4},
                {"col1": 2, "col2": "b", "col3": 5},
                {"col1": 3, "col2": "c", "col3": 6},
            ]
        ),
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "agg_dq",
                "rule": "sum_col3_threshold",
                "column_name": "col3",
                "expectation": "sum(col3) > 20",
                "action_if_failed": "ignore",
                "tag": "strict",
                "description": "sum col3 value must be greater than 20",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": True,
                "error_drop_threshold": "10",
                "priority": "medium",
            }
        ],
        True,  # write to table
        True,  # write to temp table
        spark.createDataFrame(
            [
                {"col1": 1, "col2": "a", "col3": 4},
                {"col1": 2, "col2": "b", "col3": 5},
                {"col1": 3, "col2": "c", "col3": 6},
            ]
        ),  # expected result
        3,  # input count
        0,  # error count
        0,  # output count
        [
            {
                "action_if_failed": "ignore",
                "column_name": "col3",
                "description": "sum col3 value must be greater than 20",  # source_ag_result
                "priority": "medium",
                "rule": "sum_col3_threshold",
                "rule_type": "agg_dq",
                "status": "fail",
                "tag": "strict",
                "id_hash": "15ed445152a4fc3c047a2d138eb173b6",
                "expectation_hash": "b029f67e8ea05c560a5c27cba445ec86",
            }
        ],
        None,  # final_agg_result
        None,  # source_query_dq_res
        None,  # final_query_dq_res
        {
            "rules": {"num_dq_rules": 1, "num_row_dq_rules": 0},
            "query_dq_rules": {
                "num_final_query_dq_rules": 0,
                "num_source_query_dq_rules": 0,
                "num_query_dq_rules": 0,
            },
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 1,
                "num_agg_dq_rules": 1,
                "num_final_agg_dq_rules": 1,
            },
        },  # dq_rules
        {
            "row_dq_status": "Skipped",
            "source_agg_dq_status": "Passed",  # status
            "final_agg_dq_status": "Skipped",
            "run_status": "Passed",
            "source_query_dq_status": "Skipped",
            "final_query_dq_status": "Skipped",
        },
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
                {"col1": 3, "col2": "c", "col3": 6},
            ]
        ),
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "agg_dq",
                "rule": "avg_col3_threshold",
                "column_name": "col3",
                "expectation": "avg(col3) > 25",
                "action_if_failed": "fail",
                "tag": "strict",
                "description": "avg col3 value must be greater than 25",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            }
        ],
        True,  # write to table
        True,  # write to temp table
        SparkExpectationsMiscException,  # excepted result
        3,  # input count
        0,  # error count
        0,  # output count
        [
            {
                "action_if_failed": "fail",
                "column_name": "col3",
                "description": "avg col3 value must be greater than 25",  # source_agg_result
                "priority": "medium",
                "rule": "avg_col3_threshold",
                "rule_type": "agg_dq",
                "status": "fail",
                "tag": "strict",
                "id_hash": "ef2a8e02bcad08db61e377ec0226478f",
                "expectation_hash": "797f30f2cd9cde1dafdb3f95e21c3a53",
            }
        ],
        None,  # final_agg_result
        None,  # source_query_dq_res
        None,  # final_query_dq_res
        {
            "rules": {"num_dq_rules": 1, "num_row_dq_rules": 0},
            "query_dq_rules": {
                "num_final_query_dq_rules": 0,
                "num_source_query_dq_rules": 0,
                "num_query_dq_rules": 0,
            },
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 1,
                "num_agg_dq_rules": 1,
                "num_final_agg_dq_rules": 1,
            },
        },  # dq_rules
        {
            "row_dq_status": "Skipped",
            "source_agg_dq_status": "Failed",  # status
            "final_agg_dq_status": "Skipped",
            "run_status": "Failed",
            "source_query_dq_status": "Skipped",
            "final_query_dq_status": "Skipped",
        },
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
                {"col1": 3, "col2": "c", "col3": 6},
            ]
        ),
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "agg_dq",
                "rule": "min_col1_threshold",
                "column_name": "col1",
                "expectation": "min(col1) > 10",
                "action_if_failed": "ignore",
                "tag": "strict",
                "description": "min col1 value must be greater than 10",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": True,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col2_set",
                "column_name": "col2",
                "expectation": "col2 in  ('a', 'c')",
                "action_if_failed": "drop",
                "tag": "strict",
                "description": "col2 value must be in ('a', 'b')",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "0",
                "priority": "medium",
            },
        ],
        True,  # write to table
        True,  # write to temp table
        spark.createDataFrame(
            [
                {"col1": 1, "col2": "a", "col3": 4},
                {"col1": 3, "col2": "c", "col3": 6},
            ]
        ),  # expected result but row_dq set to false
        3,  # input count
        1,  # error count
        2,  # output count
        None,  # source_agg-result
        [
            {
                "action_if_failed": "ignore",
                "column_name": "col1",
                "description": "min col1 value must be greater than 10",
                "priority": "medium",
                "rule": "min_col1_threshold",
                "rule_type": "agg_dq",
                "status": "fail",
                "tag": "strict",
                "id_hash": "2bcb7bf50101435566025d1c259eb19f",
                "expectation_hash": "5576a7fcbf3fcf9c733d1df0296e736a",
            }
        ],
        # final_agg_result
        None,  # source_query_dq_res
        None,  # final_query_dq_res
        {
            "rules": {"num_dq_rules": 2, "num_row_dq_rules": 1},
            "query_dq_rules": {
                "num_final_query_dq_rules": 0,
                "num_source_query_dq_rules": 0,
                "num_query_dq_rules": 0,
            },
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 1,
                "num_agg_dq_rules": 1,
                "num_final_agg_dq_rules": 1,
            },
        },  # dq_rules
        {
            "row_dq_status": "Passed",
            "source_agg_dq_status": "Skipped",
            "final_agg_dq_status": "Passed",
            "run_status": "Passed",
            "source_query_dq_status": "Skipped",
            "final_query_dq_status": "Skipped",
        },
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
                {"col1": 3, "col2": "c", "col3": 6},
                # row meets expectations
            ]
        ),
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "agg_dq",
                "rule": "std_col3_threshold",
                "column_name": "col3",
                "expectation": "stddev(col3) > 10",
                "action_if_failed": "fail",
                "tag": "strict",
                "description": "std col3 value must be greater than 10",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": True,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col2_set",
                "column_name": "col2",
                "expectation": "col2 in  ('a', 'c')",
                "action_if_failed": "drop",
                "tag": "strict",
                "description": "col2 value must be in ('a', 'b')",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": True,
                "error_drop_threshold": "15",
                "priority": "medium",
            },
        ],
        True,  # write to table
        True,  # write temp table
        SparkExpectationsMiscException,  # expected result
        3,  # input count
        1,  # error count
        2,  # output count
        None,  # source_agg_result
        [
            {
                "action_if_failed": "fail",
                "column_name": "col3",
                "description": "std col3 value must be greater than 10",
                "priority": "medium",
                "rule": "std_col3_threshold",
                "rule_type": "agg_dq",
                "status": "fail",
                "tag": "strict",
                "id_hash": "a062f66eee3e1ba1d031fd79b61f5187",
                "expectation_hash": "7ca5577ef52631573b62144d528fff03",
            }
        ],
        # final_agg_result
        None,  # source_query_dq_res
        None,  # final_query_dq_res
        {
            "rules": {"num_dq_rules": 2, "num_row_dq_rules": 1},
            "query_dq_rules": {
                "num_final_query_dq_rules": 0,
                "num_source_query_dq_rules": 0,
                "num_query_dq_rules": 0,
            },
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 1,
                "num_agg_dq_rules": 1,
                "num_final_agg_dq_rules": 1,
            },
        },  # dq_rules
        {
            "row_dq_status": "Passed",
            "source_agg_dq_status": "Skipped",
            "final_agg_dq_status": "Failed",
            "run_status": "Failed",
            "source_query_dq_status": "Skipped",
            "final_query_dq_status": "Skipped",
        },
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
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col1_threshold",
                "column_name": "col1",
                "expectation": "col1 > 1",
                "action_if_failed": "drop",
                "tag": "validity",
                "description": "col1 value must be greater than 1",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "0",
                "priority": "medium",
            }
        ],
        True,  # write to table
        True,  # write to temp table
        spark.createDataFrame(
            [{"col1": 2, "col2": "b"}, {"col1": 3, "col2": "c"}]  # expected_output
        ),  # expected result
        3,  # input count
        1,  # error count
        2,  # output count
        None,  # source_agg_result
        None,  # final_agg_result
        None,  # source_query_dq_res
        None,  # final_query_dq_res
        {
            "rules": {"num_dq_rules": 1, "num_row_dq_rules": 1},
            "query_dq_rules": {
                "num_final_query_dq_rules": 0,
                "num_source_query_dq_rules": 0,
                "num_query_dq_rules": 0,
            },
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 0,
                "num_agg_dq_rules": 0,
                "num_final_agg_dq_rules": 0,
            },
        },  # dq_rules
        {
            "row_dq_status": "Passed",
            "source_agg_dq_status": "Skipped",
            "final_agg_dq_status": "Skipped",
            "run_status": "Passed",
            "source_query_dq_status": "Skipped",
            "final_query_dq_status": "Skipped",
        },  # status
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
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col1_threshold_1",
                "column_name": "col1",
                "expectation": "col1 > 1",
                "action_if_failed": "ignore",
                "tag": "validity",
                "description": "col1 value must be greater than 1",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": True,
                "error_drop_threshold": "10",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col2_set",
                "column_name": "col2",
                "expectation": "col2 in ('a', 'b', 'c')",
                "action_if_failed": "drop",
                "tag": "validity",
                "description": "col1 value must be greater than 2",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": True,
                "error_drop_threshold": "10",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "agg_dq",
                "rule": "distinct_col2_threshold",
                "column_name": "col2",
                "expectation": "count(distinct col2) > 4",
                "action_if_failed": "ignore",
                "tag": "validity",
                "description": "distinct of col2 value must be greater than 4",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
        ],
        True,  # write to table
        True,  # write to temp table
        spark.createDataFrame(
            [  # expected_output
                {"col1": 1, "col2": "a", "col3": 4},
                {"col1": 2, "col2": "b", "col3": 5},
                {"col1": 3, "col2": "c", "col3": 6},
            ]
        ),
        3,  # input count
        1,  # error count
        3,  # output count
        [
            {
                "action_if_failed": "ignore",
                "column_name": "col2",
                "description": "distinct of col2 value must be greater than 4",
                "priority": "medium",
                "rule": "distinct_col2_threshold",
                "rule_type": "agg_dq",
                "status": "fail",
                "tag": "validity",
                "id_hash": "77034ce515558ddc3974ecbaf2b86546",
                "expectation_hash": "cf678626f0e8abc9ddbe691554a8d05d",
            }
        ],
        # source_agg_result
        None,  # final_agg_result
        None,  # source_query_dq_res
        None,  # final_query_dq_res
        {
            "rules": {"num_dq_rules": 3, "num_row_dq_rules": 2},
            "query_dq_rules": {
                "num_final_query_dq_rules": 0,
                "num_source_query_dq_rules": 0,
                "num_query_dq_rules": 0,
            },
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 1,
                "num_agg_dq_rules": 1,
                "num_final_agg_dq_rules": 1,
            },
        },  # dq_rules
        {
            "row_dq_status": "Passed",
            "source_agg_dq_status": "Passed",
            "final_agg_dq_status": "Skipped",
            "run_status": "Passed",
            "source_query_dq_status": "Skipped",
            "final_query_dq_status": "Skipped",
        },  # status
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
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col3_threshold_4",
                "column_name": "col3",
                "expectation": "col3 > 4",
                "action_if_failed": "drop",
                "tag": "validity",
                "description": "col3 value must be greater than 4",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": True,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col2_set",
                "column_name": "col2",
                "expectation": "col2 in ('a', 'b')",
                "action_if_failed": "ignore",
                "tag": "validity",
                "description": "col2 value must be in (a, b)",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": True,
                "error_drop_threshold": "2",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "agg_dq",
                "rule": "avg_col1_threshold",
                "column_name": "col1",
                "expectation": "avg(col1) > 4",
                "action_if_failed": "ignore",
                "tag": "accuracy",
                "description": "avg of col1 value must be greater than 4",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": True,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
        ],
        True,  # write to table
        True,  # write to temp table
        spark.createDataFrame(
            [  # expected_output
                {"col1": 2, "col2": "b", "col3": 5},
                {"col1": 3, "col2": "c", "col3": 6},
            ]
        ),  # expected result
        3,  # input count
        2,  # error count
        2,  # output count
        [
            {
                "action_if_failed": "ignore",
                "column_name": "col1",
                "priority": "medium",
                "description": "avg of col1 value must be greater than 4",
                "rule": "avg_col1_threshold",
                "rule_type": "agg_dq",
                "status": "fail",
                "tag": "accuracy",
                "id_hash": "c1ca1eec98a44d5be7240469fc77d590",
                "expectation_hash": "a51484f52d7d52053993663b1c323146",
            }
        ],
        # source_agg_dq
        [
            {
                "action_if_failed": "ignore",
                "column_name": "col1",
                "description": "avg of col1 value must be greater than 4",
                "priority": "medium",
                "rule": "avg_col1_threshold",
                "rule_type": "agg_dq",
                "status": "fail",
                "tag": "accuracy",
                "id_hash": "c1ca1eec98a44d5be7240469fc77d590",
                "expectation_hash": "a51484f52d7d52053993663b1c323146",
            }
        ],
        # final_agg_dq
        None,  # source_query_dq_res
        None,  # final_query_dq_res
        {
            "rules": {"num_dq_rules": 3, "num_row_dq_rules": 2},
            "query_dq_rules": {
                "num_final_query_dq_rules": 0,
                "num_source_query_dq_rules": 0,
                "num_query_dq_rules": 0,
            },
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 1,
                "num_agg_dq_rules": 1,
                "num_final_agg_dq_rules": 1,
            },
        },  # dq_rules
        {
            "row_dq_status": "Passed",
            "source_agg_dq_status": "Passed",
            "final_agg_dq_status": "Passed",
            "run_status": "Passed",
            "source_query_dq_status": "Skipped",
            "final_query_dq_status": "Skipped",
        },  # status
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
                {"col1": 2, "col2": "d", "col3": 7},
            ]
        ),
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col3_and_col1_threshold_4",
                "column_name": "col3, col1",
                "expectation": "((col3 * col1) - col3) > 5",
                "action_if_failed": "drop",
                "tag": "validity",
                "description": "col3 and col1 operation value must be greater than 3",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": True,
                "error_drop_threshold": "25",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col2_set",
                "column_name": "col2",
                "expectation": "col2 in ('b', 'c')",
                "action_if_failed": "ignore",
                "tag": "validity",
                "description": "col2 value must be in (b, c)",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": True,
                "error_drop_threshold": "30",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "agg_dq",
                "rule": "avg_col1_threshold",
                "column_name": "col1",
                "expectation": "avg(col1) > 4",
                "action_if_failed": "ignore",
                "tag": "validity",
                "description": "avg of col1 value must be greater than 4",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": True,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "agg_dq",
                "rule": "stddev_col3_threshold",
                "column_name": "col3",
                "expectation": "stddev(col3) > 1",
                "action_if_failed": "ignore",
                "tag": "validity",
                "description": "stddev of col3 value must be greater than one",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": True,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
        ],
        True,  # write to table
        True,  # write to temp table
        spark.createDataFrame(
            [  # expected_output
                {"col1": 3, "col2": "c", "col3": 6},
                {"col1": 2, "col2": "d", "col3": 7},
            ]
        ),  # expected result
        4,  # input count
        3,  # error count
        2,  # output count
        [
            {
                "action_if_failed": "ignore",
                "column_name": "col1",
                "description": "avg of col1 value must be greater than 4",
                "priority": "medium",
                "rule": "avg_col1_threshold",
                "rule_type": "agg_dq",
                "status": "fail",
                "tag": "validity",
                "id_hash": "c1ca1eec98a44d5be7240469fc77d590",
                "expectation_hash": "a51484f52d7d52053993663b1c323146",
            },
            {
                "action_if_failed": "ignore",
                "column_name": "col3",
                "description": "stddev of col3 value must be greater than one",
                "priority": "medium",
                "rule": "stddev_col3_threshold",
                "rule_type": "agg_dq",
                "status": "pass",
                "tag": "validity",
                "id_hash": "498e650365dd33643cfcf23bbe853492",
                "expectation_hash": "c68da6a7916362c1e537f541864dc07b",
            },
        ],
        # source_agg_result
        [
            {
                "action_if_failed": "ignore",
                "column_name": "col1",
                "description": "avg of col1 value must be greater than 4",
                "priority": "medium",
                "rule": "avg_col1_threshold",
                "rule_type": "agg_dq",
                "status": "fail",
                "tag": "validity",
                "id_hash": "c1ca1eec98a44d5be7240469fc77d590",
                "expectation_hash": "a51484f52d7d52053993663b1c323146",
            },
            {
                "action_if_failed": "ignore",
                "column_name": "col3",
                "description": "stddev of col3 value must be greater than one",
                "priority": "medium",
                "rule": "stddev_col3_threshold",
                "rule_type": "agg_dq",
                "status": "fail",
                "tag": "validity",
                "id_hash": "498e650365dd33643cfcf23bbe853492",
                "expectation_hash": "c68da6a7916362c1e537f541864dc07b",
            },
        ],
        # final_agg_result
        None,  # source_query_dq_res
        None,  # final_query_dq_res
        {
            "rules": {"num_dq_rules": 5, "num_row_dq_rules": 2},
            "query_dq_rules": {
                "num_final_query_dq_rules": 0,
                "num_source_query_dq_rules": 0,
                "num_query_dq_rules": 0,
            },  # dq_rules
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 2,
                "num_agg_dq_rules": 3,
                "num_final_agg_dq_rules": 2,
            },
        },
        {
            "row_dq_status": "Passed",
            "source_agg_dq_status": "Passed",
            "final_agg_dq_status": "Passed",
            "run_status": "Passed",
            "source_query_dq_status": "Skipped",
            "final_query_dq_status": "Skipped",
        },  # status
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
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "query_dq",
                "rule": "sum_col1_threshold",
                "column_name": "col1",
                "expectation": "(select sum(col1) from test_table) > 10",
                "action_if_failed": "ignore",
                "tag": "validity",
                "description": "sum of col1 value must be greater than 10",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": True,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "query_dq",
                "rule": "stddev_col3_threshold",
                "column_name": "col3",
                "expectation": "(select stddev(col3) from test_table) > 0",
                "action_if_failed": "ignore",
                "tag": "validity",
                "description": "stddev of col3 value must be greater than 0",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": True,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
        ],
        False,  # write to table
        False,  # write to temp table
        None,  # expected result
        3,  # input count
        0,  # error count
        0,  # output count
        None,  # source_agg_result
        None,  # final_agg_result
        # final_agg_result
        [
            {
                "action_if_failed": "ignore",
                "column_name": "col1",
                "description": "sum of col1 value must be greater than 10",
                "priority": "medium",
                "rule": "sum_col1_threshold",
                "rule_type": "query_dq",
                "status": "fail",
                "tag": "validity",
                "id_hash": "8a57df68dfff830da91cfc2464c32876",
                "expectation_hash": "4968c959ece598482a67ce2abe763069",
            },
            {
                "action_if_failed": "ignore",
                "column_name": "col3",
                "description": "stddev of col3 value must be greater than 0",
                "priority": "medium",
                "rule": "stddev_col3_threshold",
                "rule_type": "query_dq",
                "status": "pass",
                "tag": "validity",
                "id_hash": "46f1bab7314858ce80c98261ae130d15",
                "expectation_hash": "1775e158779369d0fec27fde14f63009",
            },
        ],
        # source_query_dq_res
        None,  # final_query_dq_res
        {
            "rules": {"num_dq_rules": 2, "num_row_dq_rules": 0},
            "query_dq_rules": {
                "num_final_query_dq_rules": 2,
                "num_source_query_dq_rules": 2,
                "num_query_dq_rules": 2,
            },  # dq_rules
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 0,
                "num_agg_dq_rules": 0,
                "num_final_agg_dq_rules": 0,
            },
        },
        {
            "row_dq_status": "Skipped",
            "source_agg_dq_status": "Skipped",
            "final_agg_dq_status": "Skipped",
            "run_status": "Passed",
            "source_query_dq_status": "Passed",
            "final_query_dq_status": "Skipped",
        },  # status
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
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col3_and_col1_threshold_4",
                "column_name": "col3, col1",
                "expectation": "((col3 * col1) - col3) > 5",
                "action_if_failed": "drop",
                "tag": "validity",
                "description": "col3 and col1 operation value must be greater than 3",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": True,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "query_dq",
                "rule": "max_col1_threshold",
                "column_name": "col1",
                "expectation": "(select max(col1) from test_table) > 10",
                "action_if_failed": "ignore",
                "tag": "strict",
                "description": "max of col1 value must be greater than 10",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": True,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "query_dq",
                "rule": "min_col3_threshold",
                "column_name": "col3",
                "expectation": "(select min(col3) from test_table) > 0",
                "action_if_failed": "ignore",
                "tag": "validity",
                "description": "min of col3 value must be greater than 0",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": True,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
        ],
        True,  # write to table
        False,  # write to temp table
        spark.createDataFrame(
            [
                {"col1": 3, "col2": "c", "col3": 6},
            ]
        ),  # expected result
        3,  # input count
        2,  # error count
        1,  # output count
        None,  # source_agg_result
        None,  # final_agg_result
        # final_agg_result
        None,  # source_query_dq_res
        [
            {
                "action_if_failed": "ignore",
                "column_name": "col1",
                "description": "max of col1 value must be greater than 10",
                "priority": "medium",
                "rule": "max_col1_threshold",
                "rule_type": "query_dq",
                "status": "fail",
                "tag": "strict",
                "id_hash": "3d9bdf8accb4a182f74026cdf287b187",
                "expectation_hash": "6e8c4bcfc8599ff8477320ec8d5a6e10",
            },
            {
                "action_if_failed": "ignore",
                "column_name": "col3",
                "description": "min of col3 value must be greater than 0",
                "priority": "medium",
                "rule": "min_col3_threshold",
                "rule_type": "query_dq",
                "status": "pass",
                "tag": "validity",
                "id_hash": "182f5d2c7da8e7b200b84327accb2dc0",
                "expectation_hash": "c0a5ecb297bae70f862f2808f108dd93",
            },
        ],
        # final_query_dq_res
        {
            "rules": {"num_dq_rules": 3, "num_row_dq_rules": 1},
            "query_dq_rules": {
                "num_final_query_dq_rules": 2,
                "num_source_query_dq_rules": 0,
                "num_query_dq_rules": 2,
            },  # dq_rules
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 0,
                "num_agg_dq_rules": 0,
                "num_final_agg_dq_rules": 0,
            },
        },
        {
            "row_dq_status": "Passed",
            "source_agg_dq_status": "Skipped",
            "final_agg_dq_status": "Skipped",
            "run_status": "Passed",
            "source_query_dq_status": "Skipped",
            "final_query_dq_status": "Passed",
        },  # status
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
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "query_dq",
                "rule": "min_col1_threshold",
                "column_name": "col1",
                "expectation": "(select min(col1) from test_table) > 10",
                "action_if_failed": "fail",
                "tag": "validity",
                "description": "min of col1 value must be greater than 10",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "query_dq",
                "rule": "stddev_col3_threshold",
                "column_name": "col3",
                "expectation": "(select stddev(col3) from test_table) > 0",
                "action_if_failed": "ignore",
                "tag": "validity",
                "description": "stddev of col3 value must be greater than 0",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
        ],
        False,  # write to table
        False,  # write to temp table
        SparkExpectationsMiscException,  # expected result
        3,  # input count
        0,  # error count
        0,  # output count
        None,  # source_agg_result
        None,  # final_agg_result
        [
            {
                "action_if_failed": "fail",
                "column_name": "col1",
                "description": "min of col1 value must be greater than 10",
                "priority": "medium",
                "rule": "min_col1_threshold",
                "rule_type": "query_dq",
                "status": "fail",
                "tag": "validity",
                "id_hash": "62cc13186e16986b1ceb3dfafba5a99e",
                "expectation_hash": "f3004c6354379aea6ab32956c560656f",
            },
            {
                "action_if_failed": "ignore",
                "column_name": "col3",
                "description": "stddev of col3 value must be greater than 0",
                "priority": "medium",
                "rule": "stddev_col3_threshold",
                "rule_type": "query_dq",
                "status": "pass",
                "tag": "validity",
                "id_hash": "46f1bab7314858ce80c98261ae130d15",
                "expectation_hash": "1775e158779369d0fec27fde14f63009",
            },
        ],  # source_query_dq_res
        None,  # final_query_dq_res
        {
            "rules": {"num_dq_rules": 2, "num_row_dq_rules": 0},
            "query_dq_rules": {
                "num_final_query_dq_rules": 2,
                "num_source_query_dq_rules": 2,
                "num_query_dq_rules": 2,
            },  # dq_rules
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 0,
                "num_agg_dq_rules": 0,
                "num_final_agg_dq_rules": 0,
            },
        },
        {
            "row_dq_status": "Skipped",
            "source_agg_dq_status": "Skipped",
            "final_agg_dq_status": "Skipped",
            "run_status": "Failed",
            "source_query_dq_status": "Failed",
            "final_query_dq_status": "Skipped",
        },  # status
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
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col3_and_col1_threshold_4",
                "column_name": "col3, col1",
                "expectation": "((col3 * col1) - col3) > 5",
                "action_if_failed": "drop",
                "tag": "validity",
                "description": "col3 and col1 operation value must be greater than 3",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": True,
                "error_drop_threshold": "25",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "query_dq",
                "rule": "max_col1_threshold",
                "column_name": "col1",
                "expectation": "(select max(col1) from test_table) > 10",
                "action_if_failed": "fail",
                "tag": "strict",
                "description": "max of col1 value must be greater than 10",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": True,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "query_dq",
                "rule": "min_col3_threshold",
                "column_name": "col3",
                "expectation": "(select min(col3) from test_table) > 0",
                "action_if_failed": "ignore",
                "tag": "validity",
                "description": "min of col3 value must be greater than 0",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": True,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
        ],
        True,  # write to table
        False,  # write to temp table
        SparkExpectationsMiscException,  # expected result
        3,  # input count
        2,  # error count
        1,  # output count
        None,  # source_agg_result
        None,  # final_agg_result
        # final_agg_result
        None,  # source_query_dq_res
        [
            {
                "action_if_failed": "fail",
                "column_name": "col1",
                "description": "max of col1 value must be greater than 10",
                "priority": "medium",
                "rule": "max_col1_threshold",
                "rule_type": "query_dq",
                "status": "fail",
                "tag": "strict",
                "id_hash": "3d9bdf8accb4a182f74026cdf287b187",
                "expectation_hash": "6e8c4bcfc8599ff8477320ec8d5a6e10",
            },
            {
                "action_if_failed": "ignore",
                "column_name": "col3",
                "description": "min of col3 value must be greater than 0",
                "priority": "medium",
                "rule": "min_col3_threshold",
                "rule_type": "query_dq",
                "status": "pass",
                "tag": "validity",
                "id_hash": "182f5d2c7da8e7b200b84327accb2dc0",
                "expectation_hash": "c0a5ecb297bae70f862f2808f108dd93",
            },
        ],
        # final_query_dq_res
        {
            "rules": {"num_dq_rules": 3, "num_row_dq_rules": 1},
            "query_dq_rules": {
                "num_final_query_dq_rules": 2,
                "num_source_query_dq_rules": 0,
                "num_query_dq_rules": 2,
            },  # dq_rules
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 0,
                "num_agg_dq_rules": 0,
                "num_final_agg_dq_rules": 0,
            },
        },
        {
            "row_dq_status": "Passed",
            "source_agg_dq_status": "Skipped",
            "final_agg_dq_status": "Skipped",
            "run_status": "Failed",
            "source_query_dq_status": "Skipped",
            "final_query_dq_status": "Failed",
        },  # status
    ),
    (
        # Test case 20
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
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col3_mod_2",
                "column_name": "col3",
                "expectation": "(col3 % 2) = 0",
                "action_if_failed": "drop",
                "tag": "validity",
                "description": "col3 mod must equals to 0",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": True,
                "error_drop_threshold": "40",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "query_dq",
                "rule": "min_col1_threshold",
                "column_name": "col1",
                "expectation": "(select min(col1) from test_table) > 10",
                "action_if_failed": "ignore",
                "tag": "strict",
                "description": "min of col1 value must be greater than 10",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "query_dq",
                "rule": "max_col1_threshold",
                "column_name": "col1",
                "expectation": "(select max(col1) from test_table) > 100",
                "action_if_failed": "ignore",
                "tag": "strict",
                "description": "max of col1 value must be greater than 100",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": True,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "query_dq",
                "rule": "min_col3_threshold",
                "column_name": "col3",
                "expectation": "(select min(col3) from test_table) > 0",
                "action_if_failed": "fail",
                "tag": "validity",
                "description": "min of col3 value must be greater than 0",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": True,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
        ],
        True,  # write to table
        False,  # write to temp table
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
        [
            {
                "action_if_failed": "ignore",
                "column_name": "col1",
                "description": "min of col1 value must be greater than 10",
                "priority": "medium",
                "rule": "min_col1_threshold",
                "rule_type": "query_dq",
                "status": "fail",
                "tag": "strict",
                "id_hash": "62cc13186e16986b1ceb3dfafba5a99e",
                "expectation_hash": "f3004c6354379aea6ab32956c560656f",
            }
        ],
        # source_query_dq_res
        [
            {
                "action_if_failed": "ignore",
                "column_name": "col1",
                "description": "max of col1 value must be greater than 100",
                "priority": "medium",
                "rule": "max_col1_threshold",
                "rule_type": "query_dq",
                "status": "fail",
                "tag": "strict",
                "id_hash": "3d9bdf8accb4a182f74026cdf287b187",
                "expectation_hash": "70214a48bee03c7f05f81d5c3ae23c6e",
            },
            {
                "action_if_failed": "fail",
                "column_name": "col3",
                "description": "min of col3 value must be greater than 0",
                "priority": "medium",
                "rule": "min_col3_threshold",
                "rule_type": "query_dq",
                "status": "pass",
                "tag": "validity",
                "id_hash": "182f5d2c7da8e7b200b84327accb2dc0",
                "expectation_hash": "c0a5ecb297bae70f862f2808f108dd93",
            },
        ],
        # final_query_dq_res
        {
            "rules": {"num_dq_rules": 4, "num_row_dq_rules": 1},
            "query_dq_rules": {
                "num_final_query_dq_rules": 2,
                "num_source_query_dq_rules": 1,
                "num_query_dq_rules": 3,
            },  # dq_rules
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 0,
                "num_agg_dq_rules": 0,
                "num_final_agg_dq_rules": 0,
            },
        },
        {
            "row_dq_status": "Passed",
            "source_agg_dq_status": "Skipped",
            "final_agg_dq_status": "Skipped",
            "run_status": "Passed",
            "source_query_dq_status": "Passed",
            "final_query_dq_status": "Passed",
        },  # status
    ),
    (
        # Test case 21
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
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col3_mod_2",
                "column_name": "col3",
                "expectation": "(col3 % 2) = 0",
                "action_if_failed": "drop",
                "tag": "validity",
                "description": "col3 mod must equals to 0",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "10",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "query_dq",
                "rule": "min_col1_threshold",
                "column_name": "col1",
                "expectation": "(select min(col1) from test_table) > 10",
                "action_if_failed": "ignore",
                "tag": "strict",
                "description": "min of col1 value must be greater than 10",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "query_dq",
                "rule": "max_col1_threshold",
                "column_name": "col1",
                "expectation": "(select max(col1) from test_table) > 100",
                "action_if_failed": "fail",
                "tag": "strict",
                "description": "max of col1 value must be greater than 100",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": True,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "query_dq",
                "rule": "min_col3_threshold",
                "column_name": "col3",
                "expectation": "(select min(col3) from test_table) > 0",
                "action_if_failed": "ignore",
                "tag": "validity",
                "description": "min of col3 value must be greater than 0",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": True,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
        ],
        True,  # write to table
        False,  # write to temp table
        SparkExpectationsMiscException,  # expected result
        3,  # input count
        1,  # error count
        2,  # output count
        None,  # source_agg_result
        None,  # final_agg_result
        # final_agg_result
        [
            {
                "action_if_failed": "ignore",
                "column_name": "col1",
                "description": "min of col1 value must be greater than 10",
                'priority': 'medium',
                "rule": "min_col1_threshold",
                "rule_type": "query_dq",
                "status": "fail",
                "tag": "strict",
                "id_hash": "62cc13186e16986b1ceb3dfafba5a99e",
                "expectation_hash": "f3004c6354379aea6ab32956c560656f",
            }
        ],
        # source_query_dq_res
        [
            {
                "action_if_failed": "fail",
                "column_name": "col1",
                "description": "max of col1 value must be greater than 100",
                'priority': 'medium',
                "rule": "max_col1_threshold",
                "rule_type": "query_dq",
                "status": "fail",
                "tag": "strict",
                "id_hash": "3d9bdf8accb4a182f74026cdf287b187",
                "expectation_hash": "70214a48bee03c7f05f81d5c3ae23c6e",
            },
            {
                "action_if_failed": "ignore",
                "column_name": "col3",
                "description": "min of col3 value must be greater than 0",
                'priority': 'medium',
                "rule": "min_col3_threshold",
                "rule_type": "query_dq",
                "status": "pass",
                "tag": "validity",
                "id_hash": "182f5d2c7da8e7b200b84327accb2dc0",
                "expectation_hash": "c0a5ecb297bae70f862f2808f108dd93",
            },
        ],
        # final_query_dq_res
        {
            "rules": {"num_dq_rules": 4, "num_row_dq_rules": 1},
            "query_dq_rules": {
                "num_final_query_dq_rules": 2,
                "num_source_query_dq_rules": 1,
                "num_query_dq_rules": 3,
            },  # dq_rules
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 0,
                "num_agg_dq_rules": 0,
                "num_final_agg_dq_rules": 0,
            },
        },
        {
            "row_dq_status": "Passed",
            "source_agg_dq_status": "Skipped",
            "final_agg_dq_status": "Skipped",
            "run_status": "Failed",
            "source_query_dq_status": "Passed",
            "final_query_dq_status": "Failed",
        },  # status
    ),
    (
        # Test case 22
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
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "agg_dq",
                "rule": "col3_max_value",
                "column_name": "col3",
                "expectation": "max(col3) > 1",
                "action_if_failed": "fail",
                "tag": "validity",
                "description": "col3 mod must equals to 0",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": True,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "row_dq",
                "rule": "col3_mod_2",
                "column_name": "col3",
                "expectation": "(col3 % 2) = 0",
                "action_if_failed": "drop",
                "tag": "validity",
                "description": "col3 mod must equals to 0",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "100",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "query_dq",
                "rule": "count_col1_threshold",
                "column_name": "col1",
                "expectation": "(select count(col1) from test_table) > 3",
                "action_if_failed": "ignore",
                "tag": "strict",
                "description": "count of col1 value must be greater than 3",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "query_dq",
                "rule": "col3_positive_threshold",
                "column_name": "col1",
                "expectation": "(select count(case when col3>0 then 1 else 0 end) from "
                "test_table) > 10",
                "action_if_failed": "ignore",
                "tag": "strict",
                "description": "count of col3 positive value must be greater than 10",
                "enable_for_source_dq_validation": False,
                "enable_for_target_dq_validation": True,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
        ],
        True,  # write to table
        False,  # write to temp table
        spark.createDataFrame(
            [
                {"col1": 1, "col2": "a", "col3": 4},
                {"col1": 3, "col2": "c", "col3": 6},
            ]
        ),  # expected result
        3,  # input count
        1,  # error count
        2,  # output count
        [
            {
                "action_if_failed": "fail",
                "column_name": "col3",
                "description": "col3 mod must equals to 0",
                "priority": "medium",
                "rule": "col3_max_value",
                "rule_type": "agg_dq",
                "status": "pass",
                "tag": "validity",
                "id_hash": "7fdca236f3344717aa478f3ee75c87df",
                "expectation_hash": "8dd39c0fb55bf1dc100693a850009905",
            }
        ],  # source_agg_result
        [
            {
                "action_if_failed": "fail",
                "column_name": "col3",
                "description": "col3 mod must equals to 0",
                "priority": "medium",
                "rule": "col3_max_value",
                "rule_type": "agg_dq",
                "status": "pass",
                "tag": "validity",
                "id_hash": "7fdca236f3344717aa478f3ee75c87df",
                "expectation_hash": "8dd39c0fb55bf1dc100693a850009905",
            }
        ],  # final_agg_result
        # final_agg_result
        [
            {
                "action_if_failed": "ignore",
                "column_name": "col1",
                "description": "count of col1 value must be greater than 3",
                "priority": "medium",
                "rule": "count_col1_threshold",
                "rule_type": "query_dq",
                "status": "fail",
                "tag": "strict",
                "id_hash": "72ed584e50ee1f60c98eca9ea7805726",
                "expectation_hash": "81cc6c457dd32ca18eddd3b9629de455",
            }
        ],
        # source_query_dq_res
        [
            {
                "action_if_failed": "ignore",
                "column_name": "col1",
                "description": "count of col3 positive value must be greater than 10",
                "priority": "medium",
                "rule": "col3_positive_threshold",
                "rule_type": "query_dq",
                "status": "fail",
                "tag": "strict",
                "id_hash": "f05ee49f1aec6beb6cef0239159cd87e",
                "expectation_hash": "0fa13c2c43aa84546d1d90d071c62afe",
            }
        ],
        # final_query_dq_res
        {
            "rules": {"num_dq_rules": 4, "num_row_dq_rules": 1},
            "query_dq_rules": {
                "num_final_query_dq_rules": 1,
                "num_source_query_dq_rules": 1,
                "num_query_dq_rules": 2,
            },  # dq_rules
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 1,
                "num_agg_dq_rules": 1,
                "num_final_agg_dq_rules": 1,
            },
        },
        {
            "row_dq_status": "Passed",
            "source_agg_dq_status": "Passed",
            "final_agg_dq_status": "Passed",
            "run_status": "Passed",
            "source_query_dq_status": "Passed",
            "final_query_dq_status": "Passed",
        },  # status
    ),
    (
        # Test case 23
        # In this test case, dq run set for query_dq source_query_dq and one of the rule is parameterized
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
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "query_dq",
                "rule": "sum_col1_threshold",
                "column_name": "col1",
                "expectation": "(select sum(col1) from {table}) > 10",
                "action_if_failed": "ignore",
                "tag": "validity",
                "description": "sum of col1 value must be greater than 10",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": True,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "query_dq",
                "rule": "stddev_col3_threshold",
                "column_name": "col3",
                "expectation": "(select stddev(col3) from test_table) > 0",
                "action_if_failed": "ignore",
                "tag": "validity",
                "description": "stddev of col3 value must be greater than 0",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": True,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            },
        ],
        False,  # write to table
        False,  # write to temp table
        None,  # expected result
        3,  # input count
        0,  # error count
        0,  # output count
        None,  # source_agg_result
        None,  # final_agg_result
        # final_agg_result
        [
            {
                "action_if_failed": "ignore",
                "column_name": "col1",
                "description": "sum of col1 value must be greater than 10",
                "priority": "medium",
                "rule": "sum_col1_threshold",
                "rule_type": "query_dq",
                "status": "fail",
                "tag": "validity",
                "id_hash": "8a57df68dfff830da91cfc2464c32876",
                "expectation_hash": "3b48f48522172bcaf76b1329b22bc074",  # hash of original {table} expectation
            },
            {
                "action_if_failed": "ignore",
                "column_name": "col3",
                "description": "stddev of col3 value must be greater than 0",
                "priority": "medium",
                "rule": "stddev_col3_threshold",
                "rule_type": "query_dq",
                "status": "pass",
                "tag": "validity",
                "id_hash": "46f1bab7314858ce80c98261ae130d15",
                "expectation_hash": "1775e158779369d0fec27fde14f63009",
            },
        ],
        # source_query_dq_res
        None,  # final_query_dq_res
        {
            "rules": {"num_dq_rules": 2, "num_row_dq_rules": 0},
            "query_dq_rules": {
                "num_final_query_dq_rules": 2,
                "num_source_query_dq_rules": 2,
                "num_query_dq_rules": 2,
            },  # dq_rules
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 0,
                "num_agg_dq_rules": 0,
                "num_final_agg_dq_rules": 0,
            },
        },
        {
            "row_dq_status": "Skipped",
            "source_agg_dq_status": "Skipped",
            "final_agg_dq_status": "Skipped",
            "run_status": "Passed",
            "source_query_dq_status": "Passed",
            "final_query_dq_status": "Skipped",
        },  # status
    ),
    (
        # Test case 24
        # In this test case, dq run set for source_agg_dq with action_if_failed fail
        # with the sql syntax > lower_bound and < upper_bound
        # collect stats in the test_stats_table
        spark.createDataFrame(
            [
                # avg of col3 is greater than 18 and not more than 25
                {"col1": 1, "col2": "a", "col3": 4},
                {"col1": 2, "col2": "b", "col3": 5},
                {"col1": 3, "col2": "c", "col3": 6},
            ]
        ),
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "agg_dq",
                "rule": "avg_col3_range",
                "column_name": "col3",
                "expectation": "avg(col3) > 18 and avg(col3) < 25",
                "action_if_failed": "fail",
                "tag": "strict",
                "description": "avg col3 value must be greater than 18 and less than 25",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            }
        ],
        True,  # write to table
        True,  # write to temp table
        SparkExpectationsMiscException,  # excepted result
        3,  # input count
        0,  # error count
        0,  # output count
        [
            {
                "action_if_failed": "fail",
                "column_name": "col3",
                "description": "avg col3 value must be greater than 18 and less than 25",  # source_agg_result
                "priority": "medium",
                "rule": "avg_col3_range",
                "rule_type": "agg_dq",
                "status": "fail",
                "tag": "strict",
                "id_hash": "8a2edc8702b347f8609468fd4c33b4b2",
                "expectation_hash": "a5ff054e6ccbc8a6e04d63f0e14bca80",
            }
        ],
        None,  # final_agg_result
        None,  # source_query_dq_res
        None,  # final_query_dq_res
        {
            "rules": {"num_dq_rules": 1, "num_row_dq_rules": 0},
            "query_dq_rules": {
                "num_final_query_dq_rules": 0,
                "num_source_query_dq_rules": 0,
                "num_query_dq_rules": 0,
            },
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 1,
                "num_agg_dq_rules": 1,
                "num_final_agg_dq_rules": 1,
            },
        },  # dq_rules
        {
            "row_dq_status": "Skipped",
            "source_agg_dq_status": "Failed",  # status
            "final_agg_dq_status": "Skipped",
            "run_status": "Failed",
            "source_query_dq_status": "Skipped",
            "final_query_dq_status": "Skipped",
        },
    ),
    (
        # Test case 25
        # In this test case, dq run set for source_agg_dq with action_if_failed fail
        # with the sql syntax between lower_bound and upper_bound
        # collect stats in the test_stats_table
        spark.createDataFrame(
            [
                # avg of col3 is greater than 18 and not more than 25
                {"col1": 1, "col2": "a", "col3": 4},
                {"col1": 2, "col2": "b", "col3": 5},
                {"col1": 3, "col2": "c", "col3": 6},
            ]
        ),
        [
            {
                "product_id": "product1",
                "table_name": "dq_spark.test_final_table",
                "rule_type": "agg_dq",
                "rule": "avg_col3_range",
                "column_name": "col3",
                "expectation": "avg(col3) between 18 and 25",
                "action_if_failed": "fail",
                "tag": "strict",
                "description": "avg col3 value must be greater than 18 and less than 25",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": False,
                "is_active": True,
                "enable_error_drop_alert": False,
                "error_drop_threshold": "20",
                "priority": "medium",
            }
        ],
        True,  # write to table
        True,  # write to temp table
        SparkExpectationsMiscException,  # excepted result
        3,  # input count
        0,  # error count
        0,  # output count
        [
            {
                "action_if_failed": "fail",
                "column_name": "col3",
                "description": "avg col3 value must be greater than 18 and less than 25",  # source_agg_result
                "priority": "medium",
                "rule": "avg_col3_range",
                "rule_type": "agg_dq",
                "status": "fail",
                "tag": "strict",
                "id_hash": "8a2edc8702b347f8609468fd4c33b4b2",
                "expectation_hash": "471252a50008dadaff9367eacf6a67ce",
            }
        ],
        None,  # final_agg_result
        None,  # source_query_dq_res
        None,  # final_query_dq_res
        {
            "rules": {"num_dq_rules": 1, "num_row_dq_rules": 0},
            "query_dq_rules": {
                "num_final_query_dq_rules": 0,
                "num_source_query_dq_rules": 0,
                "num_query_dq_rules": 0,
            },
            "agg_dq_rules": {
                "num_source_agg_dq_rules": 1,
                "num_agg_dq_rules": 1,
                "num_final_agg_dq_rules": 1,
            },
        },  # dq_rules
        {
            "row_dq_status": "Skipped",
            "source_agg_dq_status": "Failed",  # status
            "final_agg_dq_status": "Skipped",
            "run_status": "Failed",
            "source_query_dq_status": "Skipped",
            "final_query_dq_status": "Skipped",
        },
    ),
]

