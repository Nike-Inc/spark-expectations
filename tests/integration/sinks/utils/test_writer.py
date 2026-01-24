import os
import unittest.mock
from datetime import datetime
from unittest.mock import MagicMock, patch, Mock

import pytest
from unittest.mock import Mock, patch
from pyspark.sql.functions import col
from pyspark.sql.functions import lit, to_timestamp
from pyspark.sql.streaming import StreamingQuery
from spark_expectations.core import get_spark_session
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.sinks.utils.writer import SparkExpectationsWriter
from spark_expectations.core.exceptions import (
    SparkExpectationsMiscException,
    SparkExpectationsUserInputOrConfigInvalidException,
)
from spark_expectations.core.expectations import WrappedDataFrameWriter

spark = get_spark_session()


@pytest.fixture(name="_fixture_mock_context")
def fixture_mock_context():
    # fixture for mock context
    mock_object = Mock(spec=SparkExpectationsContext)

    mock_object.get_dq_expectations = {
        "rule": "table_row_count_gt_1",
        "column_name": "col1",
        "description": "table count should be greater than 1",
        "rule_type": "query_dq",
        "tag": "validity",
        "action_if_failed": "ignore",
    }

    return mock_object


@pytest.fixture(name="_fixture_local_kafka_topic",scope="session",autouse=True)
def fixture_setup_local_kafka_topic():
    current_dir = os.path.dirname(os.path.abspath(__file__))

    if os.getenv("UNIT_TESTING_ENV") != "spark_expectations_unit_testing_on_github_actions":
        # remove if docker conatiner is running
        os.system(f"sh {current_dir}/../../../../containers/kafka/scripts/docker_kafka_stop_script.sh")

        # start docker container and create the topic
        os.system(f"sh {current_dir}/../../../../containers/kafka/scripts/docker_kafka_start_script.sh")

        yield "docker container started"

        # remove docker container
        os.system(f"sh {current_dir}/../../../../containers/kafka/scripts/docker_kafka_stop_script.sh")

    else:
        yield (
            "A Kafka server has been launched within a Docker container for the purpose of conducting tests "
            "in a Jenkins environment"
        )


@pytest.fixture(name="_fixture_employee")
def fixture_employee_df():
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(os.path.dirname(__file__), "../../resources/employee.csv"))
    )


@pytest.fixture(name="_fixture_context")
def fixture_context():
    expectations = {
        "row_dq_rules": [
            {
                "product_id": "product1",
                "table_name": "test_final_table",
                "rule_type": "row_dq",
                "rule": "rule1",
                "column_name": "col1",
                "expectation": "col1 is not null",
                "action_if_failed": "ignore",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": True,
                "tag": "validity",
                "description": "col1 should not be null",
                "enable_error_drop_alert": False,
                "error_drop_threshold": 0,
                "priority": "medium"
            },
            {
                "product_id": "product1",
                "table_name": "test_final_table",
                "rule_type": "row_dq",
                "rule": "rule2",
                "column_name": "col2",
                "expectation": "substr(col2, 1, 1) = 'A'",
                "action_if_failed": "ignore",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": True,
                "tag": "validity",
                "description": "col2 should start with A",
                "enable_error_drop_alert": False,
                "error_drop_threshold": 0,
                "priority": "medium"
            },
        ],
        "agg_dq_rules": [
            {
                "product_id": "product1",
                "table_name": "test_final_table",
                "rule_type": "agg_dq",
                "rule": "rule3",
                "expectation": "avg(col3) > 0",
                "action_if_failed": "fail",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": True,
                "tag": "validity",
                "description": "avg of col3 should be greater than 0",
                "enable_error_drop_alert": False,
                "error_drop_threshold": 0,
                "priority": "medium"
            }
        ],
        "query_dq_rules": [
            {
                "product_id": "product1",
                "table_name": "test_final_table",
                "rule_type": "query_dq",
                "rule": "rule4",
                "expectation": "(select count(*) from test_table_query_dq where col1 is null) = 0",
                "action_if_failed": "ignore",
                "enable_for_source_dq_validation": True,
                "enable_for_target_dq_validation": True,
                "tag": "validity",
                "description": "col1 should not have null values",
                "enable_error_drop_alert": False,
                "error_drop_threshold": 0,
                "priority": "medium"
            }
        ],
    }

    context = SparkExpectationsContext(
        product_id="product1",
        spark=spark,
    )

    context.set_dq_expectations(expectations)

    return context


@pytest.fixture(name="_fixture_writer")
def fixture_writer(_fixture_context):
    return SparkExpectationsWriter(_fixture_context)


@pytest.fixture(name="_fixture_create_stats_table")
def fixture_create_stats_table():
    spark.sql("DROP TABLE IF EXISTS test_dq_stats_table")
    spark.sql(
        """
    CREATE TABLE IF NOT EXISTS test_dq_stats_table (
    product_id STRING,
    table_name STRING,
    input_count BIGINT,
    error_count BIGINT,
    output_count BIGINT,
    output_percentage FLOAT,
    success_percentage FLOAT,
    error_percentage FLOAT,
    source_agg_dq_results array<map<string, string>>,
    final_agg_dq_results array<map<string, string>>,
    source_query_dq_results array<map<string, string>>,
    final_query_dq_results array<map<string, string>>,
    dq_status map<string, string>,
    dq_rules map<string, map<string,int>>,
    dq_run_time map<string, float>,
    dq_run_status STRING,
    meta_dq_run_id STRING,
    meta_dq_run_date DATE,
    meta_dq_run_datetime TIMESTAMP,
    dq_env STRING,
    databricks_workspace_id STRING,
    databricks_hostname STRING
    )
    USING delta
    """
    )


@pytest.fixture(name="_fixture_dq_dataset")
def fixture_dq_dataset(_fixture_employee):
    from pyspark.sql.functions import create_map
    # Add mock row_dq columns that write_error_records_final expects
    # Maps must have "status": "fail" to pass through remove_passing_status_maps filter
    return (_fixture_employee.select("*")
            .withColumn("meta_dq_run_id", lit("product1_run_test"))
            .withColumn("row_dq_rule1", create_map(lit("status"), lit("fail"), lit("rule"), lit("rule1"), lit("action_if_failed"), lit("ignore")))
            .withColumn("row_dq_rule2", create_map(lit("status"), lit("fail"), lit("rule"), lit("rule2"), lit("action_if_failed"), lit("ignore"))))


def test_expectations_writer_instantiation(_fixture_context):
    writer = SparkExpectationsWriter(_fixture_context)
    assert isinstance(writer, SparkExpectationsWriter)


def test_expectations_writer_save_df_as_table(_fixture_employee, _fixture_context):
    writer = SparkExpectationsWriter(_fixture_context)
    spark.sql("DROP TABLE IF EXISTS employee_table")
    writer.save_df_as_table(
        _fixture_employee,
        "employee_table",
        {"mode": "overwrite", "format": "delta", "mergeSchema": "true"},
    )
    stats_table = spark.table("employee_table")
    assert stats_table.count() == 1000


def test_expectations_writer_save_df_as_table_partition(_fixture_employee, _fixture_context):
    writer = SparkExpectationsWriter(_fixture_context)
    spark.sql("DROP TABLE IF EXISTS employee_table")
    writer.save_df_as_table(
        _fixture_employee,
        "employee_table",
        {
            "mode": "overwrite",
            "format": "delta",
            "partitionBy": ["department"],
            "mergeSchema": "true",
        },
    )
    stats_table = spark.table("employee_table")
    assert stats_table.count() == 1000


def test_expectations_writer_save_df_as_table_sortby(_fixture_employee, _fixture_context):
    writer = SparkExpectationsWriter(_fixture_context)
    spark.sql("DROP TABLE IF EXISTS employee_table")
    writer.save_df_as_table(
        _fixture_employee,
        "employee_table",
        {
            "mode": "overwrite",
            "format": "parquet",
            "bucketBy": {"numBuckets": 4, "colName": "department"},
            "sortBy": ["full_name"],
        },
    )
    stats_table = spark.table("employee_table")
    assert stats_table.count() == 1000


def test_expectations_writer_save_df_as_table_with_bucketby(_fixture_employee, _fixture_context):
    writer = SparkExpectationsWriter(_fixture_context)
    spark.sql("DROP TABLE IF EXISTS employee_table")
    writer.save_df_as_table(
        _fixture_employee,
        "employee_table",
        {
            "mode": "overwrite",
            "format": "parquet",
            "bucketBy": {"numBuckets": 4, "colName": "department"},
        },
    )
    stats_table = spark.table("employee_table")
    assert stats_table.count() == 1000


# write_error_records_source tests
def test_write_error_records_source(_fixture_employee, _fixture_context, _fixture_dq_dataset):
    writer = SparkExpectationsWriter(_fixture_context)
    spark.sql("DROP TABLE IF EXISTS employee_table")
    spark.sql("DROP TABLE IF EXISTS employee_table_error")

    writer.write_error_records_final(_fixture_dq_dataset, "employee_table_error", "row_dq")

    error_table = spark.table("employee_table_error")
    assert error_table.count() == 1000
    # run_id is set from context (product_id + uuid), not from fixture
    assert error_table.select("meta_dq_run_id").first()[0].startswith("product1_")


@pytest.fixture(name="_fixture_create_employee_error_table")
def fixture_create_employee_error_table():
    spark.sql("DROP TABLE IF EXISTS employee_table_error")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS employee_table_error (
        eeid STRING,
        full_name STRING,
        job_title STRING,
        department STRING,
        business_unit STRING,
        gender STRING,
        ethnicity STRING,
        age INT,
        hire_date STRING,
        annual_salary STRING,
        bonus INT,
        country STRING,
        city STRING,
        exit_date STRING,
        meta_dq_run_id STRING
        )
        USING delta
        """
    )


def test_write_error_records_source_with_multiple_loads(
    _fixture_employee, _fixture_context, _fixture_dq_dataset, _fixture_create_employee_error_table
):
    writer = SparkExpectationsWriter(_fixture_context)

    _fixture_dq_dataset_2 = _fixture_dq_dataset.withColumn("meta_dq_run_id", lit("product1_run_test_2"))

    writer.write_error_records_final(_fixture_dq_dataset, "employee_table_error", "row_dq")

    writer.write_error_records_final(_fixture_dq_dataset_2, "employee_table_error", "row_dq")

    error_table = spark.table("employee_table_error")
    assert error_table.count() == 2000


def test_write_error_records_final(_fixture_employee, _fixture_context, _fixture_dq_dataset):
    writer = SparkExpectationsWriter(_fixture_context)
    spark.sql("DROP TABLE IF EXISTS employee_table")
    spark.sql("DROP TABLE IF EXISTS employee_table_error")

    writer.write_error_records_final(_fixture_dq_dataset, "employee_table_error", "row_dq")

    error_table = spark.table("employee_table_error")
    assert error_table.count() == 1000
    # run_id is set from context (product_id + uuid), not from fixture
    assert error_table.select("meta_dq_run_id").first()[0].startswith("product1_")


def test_write_error_records_final_without_error_table(_fixture_employee, _fixture_context, _fixture_dq_dataset):
    _fixture_context.set_se_enable_error_table(False)
    writer = SparkExpectationsWriter(_fixture_context)
    spark.sql("DROP TABLE IF EXISTS employee_table")
    spark.sql("DROP TABLE IF EXISTS employee_table_error")

    writer.write_error_records_final(_fixture_dq_dataset, "employee_table", "row_dq")

    tables = spark.sql("SHOW TABLES").select("tableName").collect()
    table_names = [row.tableName for row in tables]
    assert "employee_table_error" not in table_names


@pytest.fixture(name="_fixture_create_employee_final_table")
def fixture_create_employee_final_table():
    spark.sql("DROP TABLE IF EXISTS employee_table")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS employee_table (
        eeid STRING,
        full_name STRING,
        job_title STRING,
        department STRING,
        business_unit STRING,
        gender STRING,
        ethnicity STRING,
        age INT,
        hire_date STRING,
        annual_salary STRING,
        bonus INT,
        country STRING,
        city STRING,
        exit_date STRING,
        meta_dq_run_id STRING
        )
        USING delta
        """
    )


def test_write_error_records_final_with_multiple_loads(
    _fixture_employee, _fixture_context, _fixture_dq_dataset, _fixture_create_employee_final_table
):
    writer = SparkExpectationsWriter(_fixture_context)

    writer.write_error_records_final(_fixture_dq_dataset, "employee_table", "row_dq")

    _fixture_dq_dataset_2 = _fixture_dq_dataset.withColumn("meta_dq_run_id", lit("product1_run_test_2"))

    writer.write_error_records_final(_fixture_dq_dataset_2, "employee_table", "row_dq")

    final_table = spark.table("employee_table")
    assert final_table.count() == 2000


def test_write_error_records_final_with_error_table_config(_fixture_employee, _fixture_context, _fixture_dq_dataset):
    error_table_writer_config = {
        "format": "delta",
        "mode": "append",
        "partitionBy": ["department"],
    }
    _fixture_context.set_target_and_error_table_writer_config(error_table_writer_config)

    writer = SparkExpectationsWriter(_fixture_context)
    spark.sql("DROP TABLE IF EXISTS employee_table")
    spark.sql("DROP TABLE IF EXISTS employee_table_error")

    writer.write_error_records_final(_fixture_dq_dataset, "employee_table_error", "row_dq")

    error_table = spark.table("employee_table_error")
    assert error_table.count() == 1000


@pytest.fixture(name="_fixture_bq_employee_table")
def fixture_bq_employee_table(_fixture_employee):
    return _fixture_employee


def test_write_to_bq_final_table(_fixture_bq_employee_table, _fixture_context, _fixture_dq_dataset):
    writer = SparkExpectationsWriter(_fixture_context)
    target_table_writer_config = {
        "format": "bigquery",
        "mode": "append",
        "table": "test_project.test_dataset.employee_table",
        "temporaryGcsBucket": "test-bucket",
    }
    _fixture_context.set_target_and_error_table_writer_config(target_table_writer_config)

    with patch("pyspark.sql.DataFrameWriter.save", autospec=True, spec_set=True) as mock_bq:
        writer.write_error_records_final(_fixture_dq_dataset, "employee_table", "row_dq")
        mock_bq.assert_called()


def test_write_to_bq_error_table(_fixture_bq_employee_table, _fixture_context, _fixture_dq_dataset):
    writer = SparkExpectationsWriter(_fixture_context)
    error_table_writer_config = {
        "format": "bigquery",
        "mode": "append",
        "table": "test_project.test_dataset.employee_table_error",
        "temporaryGcsBucket": "test-bucket",
    }
    _fixture_context.set_target_and_error_table_writer_config(error_table_writer_config)

    with patch("pyspark.sql.DataFrameWriter.save", autospec=True, spec_set=True) as mock_bq:
        writer.write_error_records_final(_fixture_dq_dataset, "employee_table_error", "row_dq")
        mock_bq.assert_called()


def test_write_to_bq_stats_table(_fixture_bq_employee_table, _fixture_context, _fixture_dq_dataset):
    stats_table_writer_config = {
        "format": "bigquery",
        "mode": "append",
        "table": "test_project.test_dataset.employee_table_stats",
        "temporaryGcsBucket": "test-bucket",
    }
    _fixture_context.set_stats_table_writer_config(stats_table_writer_config)

    writer = SparkExpectationsWriter(_fixture_context)
    with patch("pyspark.sql.DataFrameWriter.save", autospec=True, spec_set=True) as mock_bq:
        writer.save_df_as_table(
            _fixture_dq_dataset,
            "employee_table_stats",
            stats_table_writer_config,
            stats_table=True,
        )
        mock_bq.assert_called()


def test_write_to_iceberg_table(_fixture_employee, _fixture_context, _fixture_dq_dataset):
    writer = SparkExpectationsWriter(_fixture_context)
    target_table_writer_config = {
        "format": "iceberg",
        "mode": "append",
    }
    _fixture_context.set_target_and_error_table_writer_config(target_table_writer_config)

    with patch("pyspark.sql.DataFrameWriter.save", autospec=True, spec_set=True) as mock_iceberg:
        writer.write_error_records_final(_fixture_dq_dataset, "employee_table", "row_dq")
        mock_iceberg.assert_called()


@pytest.mark.parametrize(
    "input_record, expected_result, writer_config",
    [
        (
            {
                "product_id": "product1",
                "table_name": "employee_table",
                "input_count": 100,
                "error_count": 10,
                "output_count": 90,
                "source_agg_results": [
                    {"rule_name": "rule1", "rule_type": "agg_dq", "action_if_failed": "ignore"}
                ],
                "final_agg_results": [
                    {"rule_name": "rule2", "rule_type": "agg_dq", "action_if_failed": "fail"}
                ],
                "source_query_dq_results": [
                    {"rule_name": "query_rule1", "rule_type": "query_dq", "action_if_failed": "ignore"}
                ],
                "final_query_dq_results": [
                    {"rule_name": "query_rule2", "rule_type": "query_dq", "action_if_failed": "ignore"}
                ],
                "dq_rules": {
                    "rules": {"num_dq_rules": 5, "num_row_dq_rules": 1},
                    "query_dq_rules": {
                        "num_query_dq_rules": 2,
                        "num_source_query_dq_rules": 1,
                        "num_final_query_dq_rules": 1,
                    },
                    "agg_dq_rules": {
                        "num_agg_dq_rules": 2,
                        "num_source_agg_dq_rules": 1,
                        "num_final_agg_dq_rules": 1,
                    },
                },
                "dq_run_time": {
                    "source_agg_dq": 1.0,
                    "final_agg_dq": 1.0,
                    "source_query_dq": 1.0,
                    "final_query_dq": 1.0,
                    "row_dq": 6.0,
                    "run_time": 10.0,
                },
                "status": {
                    "run_status": "Passed",
                    "source_agg_dq": "Passed",
                    "row_dq": "Passed",
                    "final_agg_dq": "Passed",
                    "source_query_dq": "Passed",
                    "final_query_dq": "Passed",
                },
                "agg_dq_detailed_stats_status": False,
                "query_dq_detailed_stats_status": False,
                "detailed_stats_table_writer_config": {},
                "test_dq_detailed_stats_table": None,
                "source_agg_dq_detailed_stats": None,
                "target_agg_dq_detailed_stats": None,
                "target_query_dq_detailed_stats": None,
                "source_query_dq_detailed_stats": None,
                "test_querydq_output_custom_table_name": None,
                "source_query_dq_output": None,
                "target_query_dq_output": None,
                "rules_execution_settings_config": {},
                "dq_expectations": {
                    "row_dq_rules": [
                        {
                            "product_id": "product1",
                            "table_name": "test_final_table",
                            "rule_type": "row_dq",
                            "rule": "rule1",
                            "column_name": "col1",
                            "expectation": "col1 is not null",
                            "action_if_failed": "ignore",
                            "enable_for_source_dq_validation": True,
                            "enable_for_target_dq_validation": True,
                            "tag": "validity",
                            "description": "col1 should not be null",
                            "enable_error_drop_alert": False,
                            "error_drop_threshold": 0,
                            "priority": "medium"
                        }
                    ],
                    "agg_dq_rules": [
                        {
                            "product_id": "product1",
                            "table_name": "test_final_table",
                            "rule_type": "agg_dq",
                            "rule": "rule3",
                            "expectation": "avg(col3) > 0",
                            "action_if_failed": "fail",
                            "enable_for_source_dq_validation": True,
                            "enable_for_target_dq_validation": True,
                            "tag": "validity",
                            "description": "avg of col3 should be greater than 0",
                            "enable_error_drop_alert": False,
                            "error_drop_threshold": 0,
                            "priority": "medium"
                        }
                    ],
                    "query_dq_rules": [
                        {
                            "product_id": "product1",
                            "table_name": "test_final_table",
                            "rule_type": "query_dq",
                            "rule": "rule4",
                            "expectation": "(select count(*) from test_table_query_dq where col1 is null) = 0",
                            "action_if_failed": "ignore",
                            "enable_for_source_dq_validation": True,
                            "enable_for_target_dq_validation": True,
                            "tag": "validity",
                            "description": "col1 should not have null values",
                            "enable_error_drop_alert": False,
                            "error_drop_threshold": 0,
                            "priority": "medium"
                        }
                    ],
                },
            },
            {"output_percentage": 90.0, "success_percentage": 90.0, "error_percentage": 10.0},
            None,
        ),
        (
            {
                "product_id": "product1",
                "table_name": "employee_table",
                "input_count": 100,
                "error_count": 10,
                "output_count": 90,
                "source_agg_results": [],
                "final_agg_results": [],
                "source_query_dq_results": [],
                "final_query_dq_results": [],
                "dq_rules": {
                    "rules": {"num_dq_rules": 5, "num_row_dq_rules": 1},
                    "query_dq_rules": {
                        "num_query_dq_rules": 2,
                        "num_source_query_dq_rules": 1,
                        "num_final_query_dq_rules": 1,
                    },
                    "agg_dq_rules": {
                        "num_agg_dq_rules": 2,
                        "num_source_agg_dq_rules": 1,
                        "num_final_agg_dq_rules": 1,
                    },
                },
                "dq_run_time": {
                    "source_agg_dq": 1.0,
                    "final_agg_dq": 1.0,
                    "source_query_dq": 1.0,
                    "final_query_dq": 1.0,
                    "row_dq": 6.0,
                    "run_time": 10.0,
                },
                "status": {
                    "run_status": "Passed",
                    "source_agg_dq": "Passed",
                    "row_dq": "Passed",
                    "final_agg_dq": "Passed",
                    "source_query_dq": "Passed",
                    "final_query_dq": "Passed",
                },
                "agg_dq_detailed_stats_status": False,
                "query_dq_detailed_stats_status": False,
                "detailed_stats_table_writer_config": {},
                "test_dq_detailed_stats_table": None,
                "source_agg_dq_detailed_stats": None,
                "target_agg_dq_detailed_stats": None,
                "target_query_dq_detailed_stats": None,
                "source_query_dq_detailed_stats": None,
                "test_querydq_output_custom_table_name": None,
                "source_query_dq_output": None,
                "target_query_dq_output": None,
                "rules_execution_settings_config": {},
                "dq_expectations": {
                    "row_dq_rules": [
                        {
                            "product_id": "product1",
                            "table_name": "test_final_table",
                            "rule_type": "row_dq",
                            "rule": "rule1",
                            "column_name": "col1",
                            "expectation": "col1 is not null",
                            "action_if_failed": "ignore",
                            "enable_for_source_dq_validation": True,
                            "enable_for_target_dq_validation": True,
                            "tag": "validity",
                            "description": "col1 should not be null",
                            "enable_error_drop_alert": False,
                            "error_drop_threshold": 0,
                            "priority": "medium"
                        }
                    ],
                    "agg_dq_rules": [
                        {
                            "product_id": "product1",
                            "table_name": "test_final_table",
                            "rule_type": "agg_dq",
                            "rule": "rule3",
                            "expectation": "avg(col3) > 0",
                            "action_if_failed": "fail",
                            "enable_for_source_dq_validation": True,
                            "enable_for_target_dq_validation": True,
                            "tag": "validity",
                            "description": "avg of col3 should be greater than 0",
                            "enable_error_drop_alert": False,
                            "error_drop_threshold": 0,
                            "priority": "medium"
                        }
                    ],
                    "query_dq_rules": [
                        {
                            "product_id": "product1",
                            "table_name": "test_final_table",
                            "rule_type": "query_dq",
                            "rule": "rule4",
                            "expectation": "(select count(*) from test_table_query_dq where col1 is null) = 0",
                            "action_if_failed": "ignore",
                            "enable_for_source_dq_validation": True,
                            "enable_for_target_dq_validation": True,
                            "tag": "validity",
                            "description": "col1 should not have null values",
                            "enable_error_drop_alert": False,
                            "error_drop_threshold": 0,
                            "priority": "medium"
                        }
                    ],
                },
            },
            {"output_percentage": 90.0, "success_percentage": 90.0, "error_percentage": 10.0},
            {"format": "bigquery"},
        ),
    ],
)
def test_write_error_stats(
    input_record,
    expected_result,
    writer_config,
    _fixture_create_stats_table,
    _fixture_local_kafka_topic,
):
    _mock_context = Mock(spec=SparkExpectationsContext)

    setattr(_mock_context, "get_table_name", input_record.get("table_name"))
    setattr(_mock_context, "get_input_count", input_record.get("input_count"))
    setattr(_mock_context, "get_error_count", input_record.get("error_count"))
    setattr(_mock_context, "get_output_count", input_record.get("output_count"))
    setattr(_mock_context, "get_error_percentage", expected_result.get("error_percentage"))
    setattr(_mock_context, "get_output_percentage", expected_result.get("output_percentage"))
    setattr(_mock_context, "get_success_percentage", expected_result.get("success_percentage"))
    setattr(
        _mock_context,
        "get_source_agg_dq_result",
        input_record.get("source_agg_results"),
    )
    setattr(
        _mock_context,
        "get_final_agg_dq_result",
        input_record.get("final_agg_results"),
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
        "get_source_agg_dq_status",
        input_record.get("status").get("source_agg_dq"),
    )
    setattr(_mock_context, "get_row_dq_status", input_record.get("status").get("row_dq"))
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
    setattr(
        _mock_context,
        "get_dq_run_status",
        input_record.get("status").get("run_status"),
    )
    setattr(_mock_context, "get_run_id", "product1_run_test")
    setattr(_mock_context, "get_run_date", "2024-03-14 00:00:00")
    setattr(
        _mock_context,
        "get_dq_run_time",
        input_record.get("dq_run_time").get("run_time"),
    )
    setattr(
        _mock_context,
        "get_source_agg_dq_run_time",
        input_record.get("dq_run_time").get("source_agg_dq"),
    )
    setattr(
        _mock_context,
        "get_final_agg_dq_run_time",
        input_record.get("dq_run_time").get("final_agg_dq"),
    )
    setattr(
        _mock_context,
        "get_source_query_dq_run_time",
        input_record.get("dq_run_time").get("source_query_dq"),
    )
    setattr(
        _mock_context,
        "get_final_query_dq_run_time",
        input_record.get("dq_run_time").get("final_query_dq"),
    )
    setattr(
        _mock_context,
        "get_row_dq_run_time",
        input_record.get("dq_run_time").get("row_dq"),
    )
    setattr(_mock_context, "get_run_id_name", "meta_dq_run_id")
    setattr(_mock_context, "get_run_date_name", "meta_dq_run_date")
    setattr(_mock_context, "get_run_date_time_name", "meta_dq_run_datetime")
    setattr(_mock_context, "get_dq_rules_params", {"env": "test_env"})
    setattr(_mock_context, "get_num_row_dq_rules", input_record.get("dq_rules").get("rules").get("num_row_dq_rules"))
    setattr(_mock_context, "get_num_dq_rules", input_record.get("dq_rules").get("rules").get("num_dq_rules"))
    setattr(_mock_context, "get_summarized_row_dq_res", [])
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
    setattr(_mock_context, "get_dq_stats_table_name", "test_dq_stats_table")

    setattr(
        _mock_context,
        "get_rules_execution_settings_config",
        input_record.get("rules_execution_settings_config"),
    )
    setattr(
        _mock_context,
        "get_agg_dq_detailed_stats_status",
        input_record.get("agg_dq_detailed_stats_status"),
    )
    setattr(
        _mock_context,
        "get_query_dq_detailed_stats_status",
        input_record.get("query_dq_detailed_stats_status"),
    )
    setattr(
        _mock_context,
        "get_source_agg_dq_detailed_stats",
        input_record.get("source_agg_dq_detailed_stats"),
    )
    setattr(
        _mock_context,
        "get_target_agg_dq_detailed_stats",
        input_record.get("target_agg_dq_detailed_stats"),
    )
    setattr(
        _mock_context,
        "get_target_query_dq_detailed_stats",
        input_record.get("target_query_dq_detailed_stats"),
    )
    setattr(
        _mock_context,
        "get_source_query_dq_detailed_stats",
        input_record.get("source_query_dq_detailed_stats"),
    )
    setattr(
        _mock_context,
        "get_detailed_stats_table_writer_config",
        input_record.get("detailed_stats_table_writer_config"),
    )
    setattr(
        _mock_context,
        "get_dq_detailed_stats_table_name",
        input_record.get("test_dq_detailed_stats_table"),
    )
    setattr(
        _mock_context,
        "get_query_dq_output_custom_table_name",
        input_record.get("test_querydq_output_custom_table_name"),
    )
    setattr(
        _mock_context,
        "get_source_query_dq_output",
        input_record.get("source_query_dq_output"),
    )
    setattr(
        _mock_context,
        "get_target_query_dq_output",
        input_record.get("target_query_dq_output"),
    )
    setattr(_mock_context, "product_id", "product_1")
    setattr(_mock_context, "get_dq_expectations", input_record.get("dq_expectations"))
    setattr(
        _mock_context,
        "get_row_dq_start_time",
        datetime.strptime("2024-03-14 00:00:00", "%Y-%m-%d %H:%M:%S"),
    )
    setattr(
        _mock_context,
        "get_row_dq_end_time",
        datetime.strptime("2024-03-14 00:10:00", "%Y-%m-%d %H:%M:%S"),
    )
    setattr(
        _mock_context,
        "get_job_metadata",
        '{"dag": "dag1", "task": "task1", "team": "my_squad"}',
    )
    setattr(_mock_context, "get_topic_name", "dq-sparkexpectations-stats")
    setattr(_mock_context, "get_dbr_workspace_id", "local")
    setattr(_mock_context, "get_dbr_workspace_url", "local")

    if writer_config is None:
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
    else:
        setattr(_mock_context, "_stats_table_writer_config", writer_config)
        setattr(_mock_context, "get_stats_table_writer_config", writer_config)

    _mock_context.spark = spark
    _mock_context.product_id = "product1"

    _fixture_writer = SparkExpectationsWriter(_mock_context)

    if writer_config and writer_config["format"] == "bigquery":
        patcher = patch("pyspark.sql.DataFrameWriter.save", autospec=True, spec_set=True)
        mock_bq = patcher.start()
        setattr(_mock_context, "get_se_streaming_stats_dict", {"se.streaming.enable": False})
        _fixture_writer.write_error_stats()
        mock_bq.assert_called_with(unittest.mock.ANY)

    else:
        setattr(_mock_context, "get_se_streaming_stats_dict", {"se.streaming.enable": True})
        _fixture_writer.write_error_stats()
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
        assert row.dq_env == "test_env"
        assert row.databricks_workspace_id == "local"
        assert row.databricks_hostname == "local"

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
            .collect()[0]["value"]
            is not None
        )


def test_save_df_as_table_streaming_without_checkpoint(_fixture_employee):
    _context = SparkExpectationsContext("product1", spark)
    _writer = SparkExpectationsWriter(_context)

    streaming_df = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", 1)
        .option("numPartitions", 1)
        .load()
        .limit(2)
    )

    with pytest.raises(
        SparkExpectationsUserInputOrConfigInvalidException,
        match=r"For streaming writes, checkpointLocation must be provided in the 'options' configuration",
    ):
        _writer.save_df_as_table(
            streaming_df,
            "employee_table",
            {"mode": "append", "format": "delta"},
        )


def test_save_df_as_table_streaming_with_checkpoint(_fixture_employee):
    _context = SparkExpectationsContext("product1", spark)
    _writer = SparkExpectationsWriter(_context)

    streaming_df = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", 1)
        .option("numPartitions", 1)
        .load()
        .limit(2)
    )

    spark.sql("DROP TABLE IF EXISTS employee_table_streaming")

    checkpoint_location = "/tmp/spark_expectations_streaming_test"

    query: StreamingQuery = _writer.save_df_as_table(
        streaming_df,
        "employee_table_streaming",
        {
            "outputMode": "append",
            "format": "delta",
            "queryName": "test_streaming_query",
            "trigger": {"processingTime": "1 seconds"},
            "options": {"checkpointLocation": checkpoint_location},
        },
    )

    query.processAllAvailable()
    query.stop()

    stats_table = spark.table("employee_table_streaming")
    assert stats_table.count() == 2

    spark.sql("DROP TABLE IF EXISTS employee_table_streaming")


def test_write_detailed_stats_to_stats_table(_fixture_context):
    _fixture_context.set_agg_dq_detailed_stats_status(True)
    _fixture_context.set_source_agg_dq_detailed_stats([("rule1", "status1"), ("rule2", "status2")])
    _fixture_context.set_target_agg_dq_detailed_stats([("rule3", "status3")])
    _fixture_context.set_query_dq_detailed_stats_status(True)
    _fixture_context.set_source_query_dq_detailed_stats([("query_rule1", "status1")])
    _fixture_context.set_target_query_dq_detailed_stats([("query_rule2", "status2")])

    spark.sql("DROP TABLE IF EXISTS test_dq_detailed_stats_table")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS test_dq_detailed_stats_table (
        product_id STRING,
        table_name STRING,
        rule_name STRING,
        rule_type STRING,
        status STRING
        )
        USING delta
        """
    )

    _fixture_context.set_dq_detailed_stats_table_name("test_dq_detailed_stats_table")

    detailed_stats_table_writer_config = {"format": "delta", "mode": "append"}
    _fixture_context.set_detailed_stats_table_writer_config(detailed_stats_table_writer_config)

    writer = SparkExpectationsWriter(_fixture_context)
    _fixture_context.set_table_name("test_table")
    writer.write_detailed_stats_to_stats_table()

    detailed_stats_table = spark.table("test_dq_detailed_stats_table")
    assert detailed_stats_table.count() == 5

    spark.sql("DROP TABLE IF EXISTS test_dq_detailed_stats_table")


def test_write_to_query_custom_output_table(_fixture_context):
    _fixture_context.set_query_dq_detailed_stats_status(True)
    _fixture_context.set_source_query_dq_output([{"rule": "rule1", "output": 10}])
    _fixture_context.set_target_query_dq_output([{"rule": "rule2", "output": 20}])

    spark.sql("DROP TABLE IF EXISTS test_query_dq_output_table")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS test_query_dq_output_table (
        rule STRING,
        output INT
        )
        USING delta
        """
    )

    _fixture_context.set_query_dq_output_custom_table_name("test_query_dq_output_table")

    detailed_stats_table_writer_config = {"format": "delta", "mode": "append"}
    _fixture_context.set_detailed_stats_table_writer_config(detailed_stats_table_writer_config)

    writer = SparkExpectationsWriter(_fixture_context)
    _fixture_context.set_table_name("test_table")
    writer.write_to_query_custom_output_table()

    query_output_table = spark.table("test_query_dq_output_table")
    assert query_output_table.count() == 2

    spark.sql("DROP TABLE IF EXISTS test_query_dq_output_table")


def test_generate_rules_exceeds_threshold():
    _context = SparkExpectationsContext("product1", spark)
    _writer = SparkExpectationsWriter(_context)
    _context.set_summarized_row_dq_res(
        [
            {
                "rule": "rule_1",
                "action_if_failed": "ignore",
                "failed_row_count": 2,
                "description": "rule 1 description",
                "rule_type": "row_dq",
                "enable_error_drop_alert": True,
                "error_drop_threshold": 10,
                "tag": "strict",
                "priority": "low"
            },
            {
                "rule": "rule_2",
                "action_if_failed": "fail",
                "failed_row_count": 6,
                "description": "rule 2 description",
                "rule_type": "row_dq",
                "enable_error_drop_alert": True,
                "error_drop_threshold": 5,
                "tag": "strict",
                "priority": "low"
            },
        ]
    )
    _context.set_input_count(100)

    _writer.generate_rules_exceeds_threshold(None)
    rules_error_per = _context.get_rules_exceeds_threshold

    assert rules_error_per == [
        {
            "rule_name": "rule_2",
            "action_if_failed": "fail",
            "description": "rule 2 description",
            "rule_type": "row_dq",
            "error_drop_threshold": "5.0%",
            "error_drop_percentage": "6.0%",
            "tag": "strict",
            "priority": "low"
        }
    ]


def test_save_df_as_table_exception(_fixture_employee, _fixture_writer):
    with pytest.raises(
        SparkExpectationsMiscException,
        match=r"error occurred while writing data in to the table .*",
    ):
        _fixture_writer.save_df_as_table(
            _fixture_employee,
            "employee_table",
            {"mode": "insert", "format": "test", "mergeSchema": "true"},
        )


def test_write_error_stats_exception(_fixture_employee, _fixture_writer):
    with pytest.raises(
        SparkExpectationsMiscException,
        match=r"error occurred while saving the data into the stats table .*",
    ):
        _fixture_writer.write_error_stats()


def test_write_error_records_final_exception(_fixture_employee, _fixture_writer, _fixture_dq_dataset):
    with pytest.raises(
        SparkExpectationsMiscException,
        match=r"error occurred while saving data into the final error table .*",
    ):
        with patch.object(_fixture_writer, 'save_df_as_table', side_effect=Exception("mock table write error")):
            _fixture_writer.write_error_records_final(_fixture_dq_dataset, "employee_table", "row_dq")


def test_generate_rules_exceeds_threshold_exception():
    _context = SparkExpectationsContext("product1", spark)
    _writer = SparkExpectationsWriter(_context)
    _context.set_summarized_row_dq_res([{}])
    _context.set_input_count(100)

    with pytest.raises(
        SparkExpectationsMiscException,
        match=r"An error occurred while creating error threshold list : .*",
    ):
        _writer.generate_rules_exceeds_threshold(None)


@pytest.mark.parametrize(
    "dbr_version,env,expected_options",
    [
        (
            13.3,
            "prod",
            {
                "kafka.bootstrap.servers": "test-server-url",
                "kafka.security.protocol": "SASL_SSL",
                "kafka.sasl.mechanism": "OAUTHBEARER",
                "kafka.sasl.jaas.config": """kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="test-client-id" clientSecret="test-token";""",
                "kafka.sasl.oauthbearer.token.endpoint.url": "test-endpoint",
                "topic": "test-topic",
            },
        ),
        (
            12.2,
            "prod",
            {
                "kafka.bootstrap.servers": "test-server-url",
                "kafka.security.protocol": "SASL_SSL",
                "kafka.sasl.mechanism": "OAUTHBEARER",
                "kafka.sasl.jaas.config": """org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="test-client-id" clientSecret="test-token";""",
                "kafka.sasl.login.callback.handler.class": "org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler",
                "kafka.sasl.oauthbearer.token.endpoint.url": "test-endpoint",
                "topic": "test-topic",
            },
        ),
    ],
)
def test_get_kafka_write_options(dbr_version, env, expected_options):
    context = SparkExpectationsContext("product1", spark)

    with patch(
        "spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret"
    ) as mock_get_secret, patch(
        "spark_expectations.core.context.SparkExpectationsContext.get_dbr_version",
        new_callable=Mock(return_value=dbr_version),
    ), patch(
        "spark_expectations.core.context.SparkExpectationsContext.get_server_url_key",
        new_callable=Mock(return_value="test-server-url-key"),
    ), patch(
        "spark_expectations.core.context.SparkExpectationsContext.get_token_endpoint_url",
        new_callable=Mock(return_value="test-endpoint-key"),
    ), patch(
        "spark_expectations.core.context.SparkExpectationsContext.get_client_id",
        new_callable=Mock(return_value="test-client-id-key"),
    ), patch(
        "spark_expectations.core.context.SparkExpectationsContext.get_token",
        new_callable=Mock(return_value="test-token-key"),
    ), patch(
        "spark_expectations.core.context.SparkExpectationsContext.get_se_streaming_stats_kafka_custom_config_enable",
        new_callable=Mock(return_value=False),
    ), patch(
        "spark_expectations.core.context.SparkExpectationsContext.get_topic_name",
        new_callable=Mock(return_value="test-topic"),
    ):
        # Configure mock to return the value passed to get_secret
        mock_get_secret.side_effect = lambda x: x.replace("-key", "")

        writer = SparkExpectationsWriter(context)
        actual_options = writer.get_kafka_write_options({})
        assert actual_options == expected_options


def test_get_kafka_write_options_custom_config():
    context = SparkExpectationsContext("product1", spark)

    expected_options = {
        "kafka.bootstrap.servers": "test-server",
        "topic": "test-topic",
    }

    with patch(
        "spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret"
    ) as mock_get_secret, patch(
        "spark_expectations.core.context.SparkExpectationsContext.get_dbr_version",
        new_callable=Mock(return_value=13.3),
    ), patch(
        "spark_expectations.core.context.SparkExpectationsContext.get_server_url_key",
        new_callable=Mock(return_value="test-server-url-key"),
    ), patch(
        "spark_expectations.core.context.SparkExpectationsContext.get_token_endpoint_url",
        new_callable=Mock(return_value="test-endpoint-key"),
    ), patch(
        "spark_expectations.core.context.SparkExpectationsContext.get_client_id",
        new_callable=Mock(return_value="test-client-id-key"),
    ), patch(
        "spark_expectations.core.context.SparkExpectationsContext.get_token",
        new_callable=Mock(return_value="test-token-key"),
    ), patch(
        "spark_expectations.core.context.SparkExpectationsContext.get_se_streaming_stats_kafka_custom_config_enable",
        new_callable=Mock(return_value=True),
    ), patch(
        "spark_expectations.core.context.SparkExpectationsContext.get_se_streaming_stats_kafka_bootstrap_server",
        new_callable=Mock(return_value="test-server"),
    ), patch(
        "spark_expectations.core.context.SparkExpectationsContext.get_topic_name",
        new_callable=Mock(return_value="test-topic"),
    ):
        # Configure mock to return the value passed to get_secret
        mock_get_secret.side_effect = lambda x: x

        writer = SparkExpectationsWriter(context)
        actual_options = writer.get_kafka_write_options(
            {}
        )  # Empty dict since we mock everything
        assert actual_options == expected_options


def test_write_error_stats_adds_merge_schema(_fixture_create_stats_table):
    from spark_expectations.core.context import SparkExpectationsContext
    from spark_expectations.config.user_config import Constants as user_config
    
    context = SparkExpectationsContext(product_id="test_product", spark=spark)
    _mock_context = Mock(spec=SparkExpectationsContext)
    
    setattr(_mock_context, "get_table_name", "employee_table")
    setattr(_mock_context, "get_input_count", 100)
    setattr(_mock_context, "get_error_count", 10)
    setattr(_mock_context, "get_output_count", 90)
    setattr(_mock_context, "get_error_percentage", 10.0)
    setattr(_mock_context, "get_output_percentage", 90.0)
    setattr(_mock_context, "get_success_percentage", 90.0)
    setattr(_mock_context, "get_source_agg_dq_result", [])
    setattr(_mock_context, "get_final_agg_dq_result", [])
    setattr(_mock_context, "get_source_query_dq_result", [])
    setattr(_mock_context, "get_final_query_dq_result", [])
    setattr(_mock_context, "get_source_agg_dq_status", "Passed")
    setattr(_mock_context, "get_row_dq_status", "Passed")
    setattr(_mock_context, "get_final_agg_dq_status", "Passed")
    setattr(_mock_context, "get_source_query_dq_status", "Passed")
    setattr(_mock_context, "get_final_query_dq_status", "Passed")
    setattr(_mock_context, "get_dq_run_status", "Passed")
    setattr(_mock_context, "get_run_id", "test_run_id")
    setattr(_mock_context, "get_run_date", "2024-03-14 00:00:00")
    setattr(_mock_context, "get_dq_run_time", 10.0)
    setattr(_mock_context, "get_source_agg_dq_run_time", 1.0)
    setattr(_mock_context, "get_final_agg_dq_run_time", 1.0)
    setattr(_mock_context, "get_source_query_dq_run_time", 1.0)
    setattr(_mock_context, "get_final_query_dq_run_time", 1.0)
    setattr(_mock_context, "get_row_dq_run_time", 6.0)
    setattr(_mock_context, "get_run_id_name", "meta_dq_run_id")
    setattr(_mock_context, "get_run_date_name", "meta_dq_run_date")
    setattr(_mock_context, "get_run_date_time_name", "meta_dq_run_datetime")
    setattr(_mock_context, "get_dq_rules_params", {"env": "test_env"})
    setattr(_mock_context, "get_num_row_dq_rules", 1)
    setattr(_mock_context, "get_num_dq_rules", 5)
    setattr(_mock_context, "get_summarized_row_dq_res", [])
    setattr(_mock_context, "get_dq_stats_table_name", "test_dq_stats_table")
    setattr(_mock_context, "get_num_agg_dq_rules", {"num_source_agg_dq_rules": 1, "num_final_agg_dq_rules": 1, "num_agg_dq_rules": 2})
    setattr(_mock_context, "get_num_query_dq_rules", {"num_source_query_dq_rules": 1, "num_final_query_dq_rules": 1, "num_query_dq_rules": 2})
    setattr(_mock_context, "get_dbr_workspace_id", "local")
    setattr(_mock_context, "get_dbr_workspace_url", "local")
    setattr(_mock_context, "get_job_metadata", None)
    setattr(_mock_context, "product_id", "test_product")
    setattr(_mock_context, "get_rules_execution_settings_config", {})
    setattr(_mock_context, "get_agg_dq_detailed_stats_status", False)
    setattr(_mock_context, "get_query_dq_detailed_stats_status", False)
    setattr(_mock_context, "get_source_agg_dq_detailed_stats", None)
    setattr(_mock_context, "get_target_agg_dq_detailed_stats", None)
    setattr(_mock_context, "get_target_query_dq_detailed_stats", None)
    setattr(_mock_context, "get_source_query_dq_detailed_stats", None)
    setattr(_mock_context, "get_detailed_stats_table_writer_config", {})
    setattr(_mock_context, "get_dq_detailed_stats_table_name", None)
    setattr(_mock_context, "get_query_dq_output_custom_table_name", None)
    setattr(_mock_context, "get_source_query_dq_output", None)
    setattr(_mock_context, "get_target_query_dq_output", None)
    setattr(_mock_context, "get_dq_expectations", {"row_dq_rules": [], "agg_dq_rules": [], "query_dq_rules": []})
    setattr(
        _mock_context,
        "get_row_dq_start_time",
        datetime.strptime("2024-03-14 00:00:00", "%Y-%m-%d %H:%M:%S"),
    )
    setattr(
        _mock_context,
        "get_row_dq_end_time",
        datetime.strptime("2024-03-14 00:10:00", "%Y-%m-%d %H:%M:%S"),
    )
    
    _mock_context.spark = spark
    _mock_context.print_dataframe_with_debugger = Mock()
    _mock_context.set_stats_dict = Mock()
    
    original_config = {"format": "delta", "mode": "overwrite"}
    setattr(_mock_context, "get_stats_table_writer_config", original_config.copy())
    setattr(_mock_context, "get_se_streaming_stats_dict", {user_config.se_enable_streaming: False})
    
    writer = SparkExpectationsWriter(_mock_context)
    writer.write_error_stats()
    
    stats_table = spark.table("test_dq_stats_table")
    row = stats_table.first()
    assert row.databricks_workspace_id == "local"
    assert row.databricks_hostname == "local"
