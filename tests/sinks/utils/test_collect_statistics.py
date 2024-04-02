import os
from unittest.mock import patch
import pytest
from pyspark.sql.functions import col
from spark_expectations.core import get_spark_session
from spark_expectations.sinks.utils.collect_statistics import (
    SparkExpectationsCollectStatistics,
)
from spark_expectations.sinks.utils.writer import SparkExpectationsWriter
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from unittest.mock import Mock
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core.expectations import WrappedDataFrameWriter

spark = get_spark_session()


@pytest.fixture(name="_fixture_local_kafka_topic")
def fixture_setup_local_kafka_topic():
    current_dir = os.path.dirname(os.path.abspath(__file__))

    if (
        os.getenv("UNIT_TESTING_ENV")
        != "spark_expectations_unit_testing_on_github_actions"
    ):
        # remove if docker conatiner is running
        os.system(
            f"sh {current_dir}/../../../spark_expectations/examples/docker_scripts/docker_kafka_stop_script.sh"
        )

        # start docker container and create the topic
        os.system(
            f"sh {current_dir}/../../../spark_expectations/examples/docker_scripts/docker_kafka_start_script.sh"
        )

        yield "docker container started"

        # remove docker container
        os.system(
            f"sh {current_dir}/../../../spark_expectations/examples/docker_scripts/docker_kafka_stop_script.sh"
        )

    else:
        yield "A Kafka server has been launched within a Docker container for the purpose of conducting tests in" " a Jenkins environment"


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


@pytest.mark.parametrize(
    "input_record, expected_result",
    [
        (
            {
                "input_count": 100,
                "error_count": 10,
                "output_count": 90,
                "source_agg_results": [
                    {
                        "rule_name": "rule1",
                        "action_if_failed": "ignore",
                        "description": "not null values in col1",
                        "rule_type": "agg_dq",
                    }
                ],
                "final_agg_results": [
                    {
                        "rule_name": "rule1",
                        "action_if_failed": "ignore",
                        "description": "not null values in col1",
                        "rule_type": "agg_dq",
                    }
                ],
                "source_query_dq_results": [
                    {
                        "rule_name": "rule5",
                        "action_if_failed": "ignore",
                        "description": "sum of col5 must be gt 100",
                        "rule_type": "query_dq",
                    }
                ],
                "final_query_dq_results": [
                    {
                        "rule_name": "rule6",
                        "action_if_failed": "ignore",
                        "description": "distinct in col6 must be lt 3",
                        "rule_type": "query_dq",
                    }
                ],
                "row_dq_res_summary": [
                    {
                        "rule_name": "rule1",
                        "action_if_failed": "ignore",
                        "rule_type": "row_dq",
                        "failed_count": 10,
                    },
                    {
                        "rule_name": "rule2",
                        "action_if_failed": "drop",
                        "rule_type": "row_dq",
                        "failed_count": 5,
                    },
                    {
                        "rule_name": "rule3",
                        "action_if_failed": "ignore",
                        "rule_type": "row_dq",
                        "failed_count": 3,
                    },
                ],
                "row_dq_error_threshold": [
                    {
                        "rule_name": "rule1",
                        "action_if_failed": "ignore",
                        "description": "description1",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "15",
                        "error_drop_percentage": "1.0",
                    },
                    {
                        "rule_name": "rule2",
                        "action_if_failed": "drop",
                        "description": "description2",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "10",
                        "error_drop_percentage": "0.5",
                    },
                    {
                        "rule_name": "rule3",
                        "action_if_failed": "ignore",
                        "description": "description3",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "5",
                        "error_drop_percentage": "0.3",
                    },
                ],
                "dq_run_time": {
                    "final_query_dq_run_time": 22.7,
                    "source_agg_dq_run_time": 17.2,
                    "row_dq_run_time": 29.3,
                    "source_query_dq_run_time": 22.4,
                    "final_agg_dq_run_time": 11.0,
                    "run_time": 108.5,
                },
                "dq_rules": {
                    "rules": {"num_dq_rules": 17, "num_row_dq_rules": 5},
                    "query_dq_rules": {
                        "num_final_query_dq_rules": 8,
                        "num_source_query_dq_rules": 3,
                        "num_query_dq_rules": 5,
                    },
                    "agg_dq_rules": {
                        "num_source_agg_dq_rules": 4,
                        "num_agg_dq_rules": 4,
                        "num_final_agg_dq_rules": 1,
                    },
                },
                "status": {
                    "run_status": "Passed",
                    "source_agg_dq": "Passed",
                    "row_dq": "Passed",
                    "final_agg_dq": "Passed",
                    "source_query_dq": "Passed",
                    "final_query_dq": "Passed",
                },
            },
            {
                "output_percentage": 90.0,
                "success_percentage": 90.0,
                "error_percentage": 10.0,
            },
        ),
        (
            {
                "input_count": 100,
                "error_count": 10,
                "output_count": 95,
                "source_agg_results": None,
                "final_agg_results": [
                    {
                        "rule_name": "rule2",
                        "action_if_failed": "drop",
                        "description": "not null values in col2",
                        "rule_type": "agg_dq",
                    }
                ],
                "source_query_dq_results": None,
                "final_query_dq_results": None,
                "row_dq_res_summary": [
                    {
                        "rule_name": "rule1",
                        "action_if_failed": "ignore",
                        "rule_type": "row_dq",
                        "failed_count": 10,
                    },
                    {
                        "rule_name": "rule2",
                        "action_if_failed": "drop",
                        "rule_type": "row_dq",
                        "failed_count": 7,
                    },
                    {
                        "rule_name": "rule3",
                        "action_if_failed": "ignore",
                        "rule_type": "row_dq",
                        "failed_count": 8,
                    },
                ],
                "row_dq_error_threshold": [
                    {
                        "rule_name": "rule1",
                        "action_if_failed": "ignore",
                        "description": "description1",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "15",
                        "error_drop_percentage": "1.0",
                    },
                    {
                        "rule_name": "rule2",
                        "action_if_failed": "drop",
                        "description": "description2",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "10",
                        "error_drop_percentage": "0.7",
                    },
                    {
                        "rule_name": "rule3",
                        "action_if_failed": "ignore",
                        "description": "description3",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "5",
                        "error_drop_percentage": "0.8",
                    },
                ],
                "dq_run_time": {
                    "final_query_dq_run_time": 0.0,
                    "source_agg_dq_run_time": 0.0,
                    "row_dq_run_time": 29.3,
                    "source_query_dq_run_time": 0.0,
                    "final_agg_dq_run_time": 11.0,
                    "run_time": 108.5,
                },
                "dq_rules": {
                    "rules": {"num_dq_rules": 14, "num_row_dq_rules": 3},
                    "query_dq_rules": {
                        "num_final_query_dq_rules": 5,
                        "num_source_query_dq_rules": 5,
                        "num_query_dq_rules": 1,
                    },
                    "agg_dq_rules": {
                        "num_source_agg_dq_rules": 4,
                        "num_agg_dq_rules": 2,
                        "num_final_agg_dq_rules": 4,
                    },
                },
                "status": {
                    "run_status": "Passed",
                    "source_agg_dq": "Skipped",
                    "row_dq": "Passed",
                    "final_agg_dq": "Passed",
                    "source_query_dq": "Skipped",
                    "final_query_dq": "Skipped",
                },
            },
            {
                "output_percentage": 95.0,
                "success_percentage": 90.0,
                "error_percentage": 10.0,
            },
        ),
        (
            {
                "input_count": 100,
                "error_count": 100,
                "output_count": 100,
                "source_agg_results": [
                    {
                        "rule_name": "rule2",
                        "action_if_failed": "drop",
                        "description": "not null values in col2",
                        "rule_type": "agg_dq",
                    }
                ],
                "final_agg_results": None,
                "source_query_dq_results": None,
                "final_query_dq_results": [
                    {
                        "rule_name": "rule5",
                        "action_if_failed": "ignore",
                        "description": "sum of col5 must be gt 100",
                        "rule_type": "query_dq",
                    }
                ],
                "row_dq_res_summary": [
                    {
                        "rule_name": "rule1",
                        "action_if_failed": "ignore",
                        "rule_type": "row_dq",
                        "failed_count": 10,
                    },
                    {
                        "rule_name": "rule2",
                        "action_if_failed": "drop",
                        "rule_type": "row_dq",
                        "failed_count": 7,
                    },
                    {
                        "rule_name": "rule3",
                        "action_if_failed": "ignore",
                        "rule_type": "row_dq",
                        "failed_count": 8,
                    },
                ],
                "row_dq_error_threshold": [
                    {
                        "rule_name": "rule1",
                        "action_if_failed": "ignore",
                        "description": "description1",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "15",
                        "error_drop_percentage": "1.0",
                    },
                    {
                        "rule_name": "rule2",
                        "action_if_failed": "drop",
                        "description": "description2",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "10",
                        "error_drop_percentage": "0.7",
                    },
                    {
                        "rule_name": "rule3",
                        "action_if_failed": "ignore",
                        "description": "description3",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "5",
                        "error_drop_percentage": "0.8",
                    },
                ],
                "dq_run_time": {
                    "final_query_dq_run_time": 22.7,
                    "source_agg_dq_run_time": 17.2,
                    "row_dq_run_time": 29.3,
                    "source_query_dq_run_time": 0.0,
                    "final_agg_dq_run_time": 0.0,
                    "run_time": 108.5,
                },
                "dq_rules": {
                    "rules": {"num_dq_rules": 17, "num_row_dq_rules": 10},
                    "query_dq_rules": {
                        "num_final_query_dq_rules": 5,
                        "num_source_query_dq_rules": 2,
                        "num_query_dq_rules": 3,
                    },
                    "agg_dq_rules": {
                        "num_source_agg_dq_rules": 4,
                        "num_agg_dq_rules": 2,
                        "num_final_agg_dq_rules": 2,
                    },
                },
                "status": {
                    "run_status": "Passed",
                    "source_agg_dq": "Passed",
                    "row_dq": "Passed",
                    "final_agg_dq": "Skipped",
                    "source_query_dq": "Skipped",
                    "final_query_dq": "Passed",
                },
            },
            {
                "output_percentage": 100.0,
                "success_percentage": 0.0,
                "error_percentage": 100.0,
            },
        ),
    ],
)
def test_collect_stats_on_success_failure(
    input_record,
    expected_result,
    _fixture_local_kafka_topic,
    _fixture_create_stats_table,
):
    # create mock _context object
    _mock_context = Mock(spec=SparkExpectationsContext)
    setattr(_mock_context, "get_dq_stats_table_name", "test_dq_stats_table")
    setattr(_mock_context, "get_run_date_name", "meta_dq_run_date")
    setattr(_mock_context, "get_run_date_time_name", "meta_dq_run_datetime")
    setattr(_mock_context, "get_run_date", "2022-12-27 10:39:44")
    setattr(_mock_context, "get_run_id_name", "meta_dq_run_id")
    setattr(_mock_context, "get_run_id", "product1_run_test")
    setattr(
        _mock_context, "get_dq_run_status", input_record.get("status").get("run_status")
    )
    setattr(
        _mock_context,
        "get_source_agg_dq_status",
        input_record.get("status").get("source_agg_dq"),
    )
    setattr(
        _mock_context, "get_row_dq_status", input_record.get("status").get("row_dq")
    )
    setattr(
        _mock_context,
        "get_final_agg_dq_status",
        input_record.get("status").get("final_agg_dq"),
    )
    setattr(
        _mock_context,
        "get_source_query_dq_status",
        input_record.get("status").get("source_query_dq"),
    )
    setattr(
        _mock_context,
        "get_final_query_dq_status",
        input_record.get("status").get("final_query_dq"),
    )
    setattr(_mock_context, "get_input_count", input_record.get("input_count"))
    setattr(_mock_context, "get_error_count", input_record.get("error_count"))
    setattr(_mock_context, "get_output_count", input_record.get("output_count"))
    setattr(
        _mock_context,
        "get_source_agg_dq_result",
        input_record.get("source_agg_results"),
    )
    setattr(
        _mock_context, "get_final_agg_dq_result", input_record.get("final_agg_results")
    )
    setattr(_mock_context, "get_table_name", "employee_table")
    setattr(
        _mock_context,
        "get_output_percentage",
        round(
            (input_record.get("output_count") / input_record.get("input_count")) * 100,
            2,
        ),
    )
    setattr(
        _mock_context,
        "get_error_percentage",
        round(
            (input_record.get("error_count") / input_record.get("input_count")) * 100, 2
        ),
    )
    setattr(
        _mock_context,
        "get_success_percentage",
        round(
            (
                (input_record.get("input_count") - input_record.get("error_count"))
                / input_record.get("input_count")
            )
            * 100,
            2,
        ),
    )
    setattr(_mock_context, "get_env", "local")
    setattr(
        _mock_context, "get_se_streaming_stats_topic_name", "dq-sparkexpectations-stats"
    )
    setattr(
        _mock_context,
        "get_source_query_dq_result",
        input_record.get("source_query_dq_results"),
    )
    setattr(
        _mock_context,
        "get_final_query_dq_result",
        input_record.get("final_query_dq_results"),
    )
    setattr(
        _mock_context,
        "get_summarized_row_dq_res",
        input_record.get("row_dq_res_summary"),
    )
    setattr(
        _mock_context,
        "get_rules_exceeds_threshold",
        input_record.get("row_dq_error_threshold"),
    )

    setattr(
        _mock_context,
        "get_dq_run_time",
        round(input_record.get("dq_run_time").get("run_time"), 1),
    )
    setattr(
        _mock_context,
        "get_source_agg_dq_run_time",
        round(input_record.get("dq_run_time").get("source_agg_dq_run_time"), 1),
    )
    setattr(
        _mock_context,
        "get_source_query_dq_run_time",
        round(input_record.get("dq_run_time").get("source_query_dq_run_time"), 1),
    )
    setattr(
        _mock_context,
        "get_row_dq_run_time",
        round(input_record.get("dq_run_time").get("row_dq_run_time"), 1),
    )
    setattr(
        _mock_context,
        "get_final_agg_dq_run_time",
        round(input_record.get("dq_run_time").get("final_agg_dq_run_time"), 1),
    )
    setattr(
        _mock_context,
        "get_final_query_dq_run_time",
        round(input_record.get("dq_run_time").get("final_query_dq_run_time"), 1),
    )

    setattr(
        _mock_context,
        "get_num_row_dq_rules",
        input_record.get("dq_rules").get("rules").get("num_row_dq_rules"),
    )
    setattr(
        _mock_context,
        "get_num_dq_rules",
        input_record.get("dq_rules").get("rules").get("num_dq_rules"),
    )
    setattr(
        _mock_context,
        "get_num_agg_dq_rules",
        input_record.get("dq_rules").get("agg_dq_rules"),
    )
    setattr(
        _mock_context,
        "get_num_query_dq_rules",
        input_record.get("dq_rules").get("query_dq_rules"),
    )
    setattr(
        _mock_context,
        "_stats_table_writer_config",
        WrappedDataFrameWriter().mode("overwrite").format("delta").build(),
    )
    setattr(
        _mock_context,
        "get_stats_table_writer_config",
        WrappedDataFrameWriter().mode("overwrite").format("delta").build(),
    )
    setattr(_mock_context, "get_se_streaming_stats_dict", {"se.enable.streaming": True})

    _mock_context.spark = spark
    _mock_context.product_id = "product1"

    writer = SparkExpectationsWriter(_mock_context)
    statistics_writer_obj = SparkExpectationsCollectStatistics(_mock_context, writer)

    @statistics_writer_obj.collect_stats_on_success_failure()
    def exec_func():
        return "success"

    result = exec_func()
    assert result == "success"

    stats_table = spark.table("test_dq_stats_table")
    assert stats_table.count() == 1
    row = stats_table.first()
    assert row.product_id == "product1"
    assert row.table_name == "employee_table"
    assert row.input_count == input_record.get("input_count")
    assert row.error_count == input_record.get("error_count")
    assert row.output_count == input_record.get("output_count")
    assert row.output_percentage == expected_result.get("output_percentage")
    assert row.success_percentage == expected_result.get("success_percentage")
    assert row.error_percentage == expected_result.get("error_percentage")
    assert row.source_agg_dq_results == input_record.get("source_agg_results")
    assert row.final_agg_dq_results == input_record.get("final_agg_results")
    assert row.source_query_dq_results == input_record.get("source_query_dq_results")
    assert row.final_query_dq_results == input_record.get("final_query_dq_results")
    assert row.dq_rules == input_record.get("dq_rules")
    # assert row.dq_run_time == input_record.get("dq_run_time")
    assert row.dq_status == input_record.get("status")
    assert row.meta_dq_run_id == "product1_run_test"

    assert (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "dq-sparkexpectations-stats")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .orderBy(col("timestamp").desc())
        .limit(1)
        .selectExpr("cast(value as string) as value")
        .collect()
        == stats_table.selectExpr("to_json(struct(*)) AS value").collect()
    )


@pytest.mark.parametrize(
    "input_record, expected_result",
    [
        (
            {
                "input_count": 100,
                "error_count": 100,
                "output_count": 0,
                "source_agg_results": [
                    {
                        "rule_name": "rule2",
                        "action_if_failed": "drop",
                        "description": "not null values in col2",
                        "rule_type": "agg_dq",
                    }
                ],
                "final_agg_results": None,
                "source_query_dq_results": None,
                "final_query_dq_results": None,
                "row_dq_res_summary": [
                    {
                        "rule_name": "rule1",
                        "action_if_failed": "fail",
                        "rule_type": "row_dq",
                        "failed_count": 10,
                    },
                    {
                        "rule_name": "rule2",
                        "action_if_failed": "drop",
                        "rule_type": "row_dq",
                        "failed_count": 7,
                    },
                    {
                        "rule_name": "rule3",
                        "action_if_failed": "ignore",
                        "rule_type": "row_dq",
                        "failed_count": 8,
                    },
                ],
                "row_dq_error_threshold": [
                    {
                        "rule_name": "rule1",
                        "action_if_failed": "fail",
                        "description": "description1",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "15",
                        "error_drop_percentage": "1.0",
                    },
                    {
                        "rule_name": "rule2",
                        "action_if_failed": "drop",
                        "description": "description2",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "10",
                        "error_drop_percentage": "0.7",
                    },
                    {
                        "rule_name": "rule3",
                        "action_if_failed": "ignore",
                        "description": "description3",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "5",
                        "error_drop_percentage": "0.8",
                    },
                ],
                "dq_run_time": {
                    "final_query_dq_run_time": 0.7,
                    "source_agg_dq_run_time": 17.2,
                    "row_dq_run_time": 29.3,
                    "source_query_dq_run_time": 0.0,
                    "final_agg_dq_run_time": 0.0,
                    "run_time": 118.5,
                },
                "dq_rules": {
                    "rules": {"num_dq_rules": 18, "num_row_dq_rules": 5},
                    "query_dq_rules": {
                        "num_final_query_dq_rules": 5,
                        "num_source_query_dq_rules": 5,
                        "num_query_dq_rules": 5,
                    },
                    "agg_dq_rules": {
                        "num_source_agg_dq_rules": 8,
                        "num_agg_dq_rules": 4,
                        "num_final_agg_dq_rules": 8,
                    },
                },
                "status": {
                    "run_status": "Failed",
                    "source_agg_dq": "Passed",
                    "row_dq": "Passed",
                    "final_agg_dq": "Skipped",
                    "source_query_dq": "Skipped",
                    "final_query_dq": "Skipped",
                },
            },
            {
                "output_percentage": 0.0,
                "success_percentage": 0.0,
                "error_percentage": 100.0,
            },
        ),
        (
            {
                "input_count": 100,
                "error_count": 100,
                "output_count": 0,
                "source_agg_results": [
                    {
                        "rule_name": "rule2",
                        "action_if_failed": "drop",
                        "description": "not null values in col2",
                        "rule_type": "agg_dq",
                    }
                ],
                "final_agg_results": None,
                "source_query_dq_results": [
                    {
                        "rule_name": "rule5",
                        "action_if_failed": "ignore",
                        "description": "sum of col5 must be gt 100",
                        "rule_type": "query_dq",
                    }
                ],
                "final_query_dq_results": [
                    {
                        "rule_name": "rule5",
                        "action_if_failed": "ignore",
                        "description": "sum of col5 must be gt 100",
                        "rule_type": "query_dq",
                    }
                ],
                "row_dq_res_summary": [
                    {
                        "rule_name": "rule1",
                        "action_if_failed": "ignore",
                        "rule_type": "row_dq",
                        "failed_count": 100,
                    },
                    {
                        "rule_name": "rule2",
                        "action_if_failed": "drop",
                        "rule_type": "row_dq",
                        "failed_count": 100,
                    },
                    {
                        "rule_name": "rule3",
                        "action_if_failed": "ignore",
                        "rule_type": "row_dq",
                        "failed_count": 88,
                    },
                ],
                "row_dq_error_threshold": [
                    {
                        "rule_name": "rule1",
                        "action_if_failed": "ignore",
                        "description": "description1",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "15",
                        "error_drop_percentage": "100.0",
                    },
                    {
                        "rule_name": "rule2",
                        "action_if_failed": "drop",
                        "description": "description2",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "10",
                        "error_drop_percentage": "100.0",
                    },
                    {
                        "rule_name": "rule3",
                        "action_if_failed": "ignore",
                        "description": "description3",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "5",
                        "error_drop_percentage": "88.0",
                    },
                ],
                "dq_run_time": {
                    "final_query_dq_run_time": 22.7,
                    "source_agg_dq_run_time": 0.0,
                    "row_dq_run_time": 29.3,
                    "source_query_dq_run_time": 10.8,
                    "final_agg_dq_run_time": 0.0,
                    "run_time": 108.5,
                },
                "dq_rules": {
                    "rules": {"num_dq_rules": 18, "num_row_dq_rules": 8},
                    "query_dq_rules": {
                        "num_final_query_dq_rules": 6,
                        "num_source_query_dq_rules": 5,
                        "num_query_dq_rules": 3,
                    },
                    "agg_dq_rules": {
                        "num_source_agg_dq_rules": 4,
                        "num_agg_dq_rules": 4,
                        "num_final_agg_dq_rules": 3,
                    },
                },
                "status": {
                    "run_status": "Failed",
                    "source_agg_dq": "Passed",
                    "row_dq": "Passed",
                    "final_agg_dq": "Failed",
                    "source_query_dq": "Passed",
                    "final_query_dq": "Passed",
                },
            },
            {
                "output_percentage": 0.0,
                "success_percentage": 0.0,
                "error_percentage": 100.0,
            },
        ),
        (
            {
                "input_count": 100,
                "error_count": 100,
                "output_count": 0,
                "source_agg_results": None,
                "final_agg_results": None,
                "source_query_dq_results": None,
                "final_query_dq_results": None,
                "row_dq_res_summary": [
                    {
                        "rule_name": "rule1",
                        "action_if_failed": "ignore",
                        "rule_type": "row_dq",
                        "failed_count": 100,
                    },
                    {
                        "rule_name": "rule2",
                        "action_if_failed": "drop",
                        "rule_type": "row_dq",
                        "failed_count": 100,
                    },
                    {
                        "rule_name": "rule3",
                        "action_if_failed": "ignore",
                        "rule_type": "row_dq",
                        "failed_count": 88,
                    },
                    {
                        "rule_name": "rule4",
                        "action_if_failed": "fail",
                        "rule_type": "row_dq",
                        "failed_count": 60,
                    },
                ],
                "row_dq_error_threshold": [
                    {
                        "rule_name": "rule1",
                        "action_if_failed": "ignore",
                        "description": "description1",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "15",
                        "error_drop_percentage": "100.0",
                    },
                    {
                        "rule_name": "rule2",
                        "action_if_failed": "drop",
                        "description": "description2",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "10",
                        "error_drop_percentage": "100.0",
                    },
                    {
                        "rule_name": "rule3",
                        "action_if_failed": "ignore",
                        "description": "description3",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "5",
                        "error_drop_percentage": "88.0",
                    },
                    {
                        "rule_name": "rule4",
                        "action_if_failed": "ignore",
                        "description": "description4",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "5",
                        "error_drop_percentage": "60.0",
                    },
                ],
                "dq_run_time": {
                    "final_query_dq_run_time": 0.0,
                    "source_agg_dq_run_time": 0.0,
                    "row_dq_run_time": 29.3,
                    "source_query_dq_run_time": 0.0,
                    "final_agg_dq_run_time": 0.0,
                    "run_time": 108.5,
                },
                "dq_rules": {
                    "rules": {"num_dq_rules": 23, "num_row_dq_rules": 5},
                    "query_dq_rules": {
                        "num_final_query_dq_rules": 10,
                        "num_source_query_dq_rules": 5,
                        "num_query_dq_rules": 5,
                    },
                    "agg_dq_rules": {
                        "num_source_agg_dq_rules": 8,
                        "num_agg_dq_rules": 4,
                        "num_final_agg_dq_rules": 4,
                    },
                },
                "status": {
                    "run_status": "Failed",
                    "source_agg_dq": "Skipped",
                    "row_dq": "Failed",
                    "final_agg_dq": "Skipped",
                    "source_query_dq": "Skipped",
                    "final_query_dq": "Skipped",
                },
            },
            {
                "output_percentage": 0.0,
                "success_percentage": 0.0,
                "error_percentage": 100.0,
            },
        ),
    ],
)
def test_collect_stats_on_success_failure_exception(
    input_record,
    expected_result,
    _fixture_local_kafka_topic,
    _fixture_create_stats_table,
):
    _mock_context = Mock(spec=SparkExpectationsContext)
    setattr(_mock_context, "get_dq_stats_table_name", "test_dq_stats_table")
    setattr(_mock_context, "get_run_date_name", "meta_dq_run_date")
    setattr(_mock_context, "get_run_date_time_name", "meta_dq_run_datetime")
    setattr(_mock_context, "get_run_date", "2022-12-27 10:39:44")
    setattr(_mock_context, "get_run_id_name", "meta_dq_run_id")
    setattr(_mock_context, "get_run_id", "product1_run_test")
    setattr(
        _mock_context, "get_dq_run_status", input_record.get("status").get("run_status")
    )
    setattr(
        _mock_context,
        "get_source_agg_dq_status",
        input_record.get("status").get("source_agg_dq"),
    )
    setattr(
        _mock_context, "get_row_dq_status", input_record.get("status").get("row_dq")
    )
    setattr(
        _mock_context,
        "get_final_agg_dq_status",
        input_record.get("status").get("final_agg_dq"),
    )
    setattr(
        _mock_context,
        "get_source_query_dq_status",
        input_record.get("status").get("source_query_dq"),
    )
    setattr(
        _mock_context,
        "get_final_query_dq_status",
        input_record.get("status").get("final_query_dq"),
    )
    setattr(_mock_context, "get_input_count", input_record.get("input_count"))
    setattr(_mock_context, "get_error_count", input_record.get("error_count"))
    setattr(_mock_context, "get_output_count", input_record.get("output_count"))
    setattr(
        _mock_context,
        "get_source_agg_dq_result",
        input_record.get("source_agg_results"),
    )
    setattr(
        _mock_context, "get_final_agg_dq_result", input_record.get("final_agg_results")
    )
    setattr(_mock_context, "get_table_name", "employee_table")
    setattr(
        _mock_context,
        "get_output_percentage",
        round(
            (input_record.get("output_count") / input_record.get("input_count")) * 100,
            2,
        ),
    )
    setattr(
        _mock_context,
        "get_error_percentage",
        round(
            (input_record.get("error_count") / input_record.get("input_count")) * 100, 2
        ),
    )
    setattr(
        _mock_context,
        "get_success_percentage",
        round(
            (
                (input_record.get("input_count") - input_record.get("error_count"))
                / input_record.get("input_count")
            )
            * 100,
            2,
        ),
    )
    setattr(_mock_context, "get_env", "local")
    setattr(
        _mock_context, "get_se_streaming_stats_topic_name", "dq-sparkexpectations-stats"
    )
    setattr(
        _mock_context,
        "get_source_query_dq_result",
        input_record.get("source_query_dq_results"),
    )
    setattr(
        _mock_context,
        "get_final_query_dq_result",
        input_record.get("final_query_dq_results"),
    )
    setattr(
        _mock_context,
        "get_summarized_row_dq_res",
        input_record.get("row_dq_res_summary"),
    )
    setattr(
        _mock_context,
        "get_rules_exceeds_threshold",
        input_record.get("row_dq_error_threshold"),
    )

    setattr(
        _mock_context,
        "get_dq_run_time",
        round(input_record.get("dq_run_time").get("run_time"), 1),
    )
    setattr(
        _mock_context,
        "get_source_agg_dq_run_time",
        round(input_record.get("dq_run_time").get("source_agg_dq_run_time"), 1),
    )
    setattr(
        _mock_context,
        "get_source_query_dq_run_time",
        round(input_record.get("dq_run_time").get("source_query_dq_run_time"), 1),
    )
    setattr(
        _mock_context,
        "get_row_dq_run_time",
        round(input_record.get("dq_run_time").get("row_dq_run_time"), 1),
    )
    setattr(
        _mock_context,
        "get_final_agg_dq_run_time",
        round(input_record.get("dq_run_time").get("final_agg_dq_run_time"), 1),
    )
    setattr(
        _mock_context,
        "get_final_query_dq_run_time",
        round(input_record.get("dq_run_time").get("final_query_dq_run_time"), 1),
    )

    setattr(
        _mock_context,
        "get_num_row_dq_rules",
        input_record.get("dq_rules").get("rules").get("num_row_dq_rules"),
    )
    setattr(
        _mock_context,
        "get_num_dq_rules",
        input_record.get("dq_rules").get("rules").get("num_dq_rules"),
    )
    setattr(
        _mock_context,
        "get_num_agg_dq_rules",
        input_record.get("dq_rules").get("agg_dq_rules"),
    )
    setattr(
        _mock_context,
        "get_num_query_dq_rules",
        input_record.get("dq_rules").get("query_dq_rules"),
    )
    _mock_context.spark = spark
    _mock_context.product_id = "product1"
    setattr(
        _mock_context,
        "_stats_table_writer_config",
        WrappedDataFrameWriter().mode("overwrite").format("delta").build(),
    )
    setattr(
        _mock_context,
        "get_stats_table_writer_config",
        WrappedDataFrameWriter().mode("overwrite").format("delta").build(),
    )
    setattr(_mock_context, "get_se_streaming_stats_dict", {"se.enable.streaming": True})

    writer = SparkExpectationsWriter(_mock_context)
    statistics_writer_obj = SparkExpectationsCollectStatistics(_mock_context, writer)

    @statistics_writer_obj.collect_stats_on_success_failure()
    def func_exception():
        raise SparkExpectationsMiscException("test exception")

    with pytest.raises(SparkExpectationsMiscException):
        func_exception()

    stats_table = spark.table("test_dq_stats_table")
    assert stats_table.count() == 1
    row = stats_table.first()
    assert row.product_id == "product1"
    assert row.table_name == "employee_table"
    assert row.input_count == input_record.get("input_count")
    assert row.error_count == input_record.get("error_count")
    assert row.output_count == input_record.get("output_count")
    assert row.output_percentage == expected_result.get("output_percentage")
    assert row.success_percentage == expected_result.get("success_percentage")
    assert row.error_percentage == expected_result.get("error_percentage")
    assert row.source_agg_dq_results == input_record.get("source_agg_results")
    assert row.final_agg_dq_results == input_record.get("final_agg_results")
    assert row.source_query_dq_results == input_record.get("source_query_dq_results")
    assert row.final_query_dq_results == input_record.get("final_query_dq_results")
    assert row.dq_rules == input_record.get("dq_rules")
    # assert row.dq_run_time == input_record.get("dq_run_time")
    assert row.dq_status == input_record.get("status")
    assert row.meta_dq_run_id == "product1_run_test"

    assert (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "dq-sparkexpectations-stats")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .orderBy(col("timestamp").desc())
        .limit(1)
        .selectExpr("cast(value as string) as value")
        .collect()
        == stats_table.selectExpr("to_json(struct(*)) AS value").collect()
    )
