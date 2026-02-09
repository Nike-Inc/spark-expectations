# pylint: disable=too-many-lines
import datetime
from unittest.mock import MagicMock, Mock, PropertyMock
from unittest.mock import patch
import pytest
from pyspark.sql import DataFrame, SparkSession
from spark_expectations.config.user_config import Constants as SeUserConfig
from examples.scripts.base_setup import RULES_TABLE_SCHEMA, set_up_delta


try:
    from pyspark.sql.connect.dataframe import DataFrame as connectDataFrame
except ImportError:
    pass

from pyspark.sql.functions import lit, to_timestamp, col, array, create_map
from pyspark.sql.types import StringType, IntegerType, StructField, StructType

from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core.expectations import (
    SparkExpectations,
    WrappedDataFrameWriter,
    WrappedDataFrameStreamWriter,
    check_if_pyspark_connect_is_supported,
    get_spark_minor_version,
)
from spark_expectations.config.user_config import Constants as user_config
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.notifications.push.spark_expectations_notify import SparkExpectationsNotify

# Import test parameters and spark session from conftest
from tests.integration.core.conftest import (
    spark,
    TEST_WITH_EXPECTATIONS_PARAM_NAMES,
    TEST_WITH_EXPECTATIONS_PARAMS,
)


def test_spark_session_initialization():
    # Test if spark session is initialized even if dataframe.sparkSession is not accessible
    with patch.object(DataFrame, "sparkSession", new_callable=PropertyMock) as mock_sparkSession:
        mock_sparkSession.side_effect = AttributeError("The 'sparkSession' attribute is not accessible")

        rules_df = spark.createDataFrame([{
            "product_id": "test_product_id",
            "table_name": "test_table",
            "rule_type": "row_dq",
            "rule": "ProductID_not_null",
            "column_name": "ProductID",
            "expectation": "ProductID is not null",
            "action_if_failed": "ignore",
            "tag": "completeness",
            "description": "ProductID must not be null",
            "enable_for_source_dq_validation": True,
            "enable_for_target_dq_validation": False,
            "is_active": True,
            "enable_error_drop_alert": False,
            "error_drop_threshold": 0,
            "priority": "medium",
        }])

        writer = WrappedDataFrameWriter().mode("append").format("parquet")
        se = SparkExpectations(
            product_id="product1",
            rules_df=rules_df,
            stats_table="dq_spark.test_dq_stats_table",
            stats_table_writer=writer,
            target_and_error_table_writer=writer,
            debugger=False,
        )
        assert type(se.spark) == SparkSession

    # Test if exception is raised when rules_df is not a DataFrame type
    writer = WrappedDataFrameWriter().mode("append").format("parquet")
    with pytest.raises(SparkExpectationsMiscException) as e:
        se = SparkExpectations(
            product_id="product1",
            rules_df="not_a_dataframe",  # type: ignore
            stats_table="dq_spark.test_dq_stats_table",
            stats_table_writer=writer,
            target_and_error_table_writer=writer,
            debugger=False,
        )
    assert "Input rules_df is not of dataframe type" in str(e.value)


@pytest.mark.parametrize(
    TEST_WITH_EXPECTATIONS_PARAM_NAMES,
    TEST_WITH_EXPECTATIONS_PARAMS,
)
def test_with_expectations(
    input_df,
    expectations,
    write_to_table,
    write_to_temp_table,
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
    _fixture_create_database,
    _fixture_local_kafka_topic,
):
    input_df.createOrReplaceTempView("test_table")

    spark.conf.set("spark.sql.session.timeZone", "Etc/UTC")

    rules_df = spark.createDataFrame(expectations) if len(expectations) > 0 else expectations
    rules_df.show(truncate=False) if len(expectations) > 0 else None

    writer = WrappedDataFrameWriter().mode("append").format("parquet")
    se = SparkExpectations(
        product_id="product1",
        rules_df=rules_df,
        stats_table="dq_spark.test_dq_stats_table",
        stats_table_writer=writer,
        target_and_error_table_writer=writer,
        debugger=False,
    )
    se._context._run_date = "2022-12-27 10:00:00"
    se._context._env = "local"
    se._context._run_id = "product1_run_test"
    se._context.set_input_count(input_count)
    se._context.set_error_count(error_count)
    se._context.set_output_count(output_count)

    # Decorate the mock function with required args
    @se.with_expectations(
        "dq_spark.test_final_table",
        user_conf={
            user_config.se_notifications_on_fail: False,
            user_config.se_dq_rules_params: {"table": "test_table", "env": "local"},
        },
        write_to_table=write_to_table,
        write_to_temp_table=write_to_temp_table,
    )
    def get_dataset() -> DataFrame:
        return input_df

    input_df.show(truncate=False)

    if isinstance(expected_output, type) and issubclass(expected_output, Exception):
        with pytest.raises(
            expected_output,
            match=r"error occurred while processing spark expectations .*",
        ):
            get_dataset()  # decorated_func()

        if status.get("final_agg_dq_status") == "Failed" or status.get("final_query_dq_status") == "Failed":
            try:
                spark.table("dq_spark.test_final_table")
                assert False
            except Exception as e:
                assert True
        

    else:
        get_dataset()  # decorated_func()

        if write_to_table is True:
            expected_output_df = expected_output.withColumn("run_id", lit("product1_run_test")).withColumn(
                "run_date", to_timestamp(lit("2022-12-27 10:00:00"))
            )

            result_df = spark.table("dq_spark.test_final_table")
            assert result_df.orderBy("col2").collect() == expected_output_df.orderBy("col2").collect()

            if spark.catalog.tableExists("dq_spark.test_final_table_error"):
                error_table = spark.table("dq_spark.test_final_table_error")
                assert error_table.count() == error_count

    stats_table = spark.table("dq_spark.test_dq_stats_table")
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
    assert row.meta_dq_run_id == "product1_run_test"
    assert row.meta_dq_run_date == datetime.date(2022, 12, 27)
    assert row.meta_dq_run_datetime == datetime.datetime(2022, 12, 27, 10, 00, 00)
    assert row.dq_env == "local"
    assert row.databricks_workspace_id == "local"
    assert row.databricks_hostname == "local"
    assert len(stats_table.columns) == 23

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

    # spark.sql("select * from dq_spark.test_final_table").show(truncate=False)
    # spark.sql("select * from dq_spark.test_final_table_error").show(truncate=False)
    # spark.sql("select * from dq_spark.test_dq_stats_table").show(truncate=False)

    for db in spark.catalog.listDatabases():
        if db.name != "default":
            spark.sql(f"DROP DATABASE {db.name} CASCADE")
    spark.sql("CLEAR CACHE")

    # os.system("rm -rf /tmp/hive/warehouse/dq_spark.db/test_final_table_error")


@patch("spark_expectations.core.expectations.SparkExpectationsWriter.write_error_stats")
def test_with_expectations_patch(
    _write_error_stats,
    _fixture_create_database,
    _fixture_spark_expectations,
    _fixture_df,
    _fixture_rules_df,
):
    decorated_func = _fixture_spark_expectations.with_expectations(
        "dq_spark.test_final_table",
        user_conf={
            user_config.se_notifications_on_fail: False,
            user_config.se_enable_query_dq_detailed_result: True,
            user_config.se_enable_agg_dq_detailed_result: True,
        },
    )(Mock(return_value=_fixture_df))

    decorated_func()

    _write_error_stats.assert_called_once_with()


def test_with_expectations_overwrite_writers(
    _fixture_create_database,
    _fixture_spark_expectations,
    _fixture_df,
    _fixture_rules_df,
):
    modified_writer = WrappedDataFrameWriter().mode("overwrite").format("iceberg")
    _fixture_spark_expectations.with_expectations(
        "dq_spark.test_final_table",
        user_conf={user_config.se_notifications_on_fail: False},
        target_and_error_table_writer=modified_writer,
    )(Mock(return_value=_fixture_df))

    assert _fixture_spark_expectations._context.get_target_and_error_table_writer_config == modified_writer.build()


def test_with_expectations_invalid_rules_do_not_raise_exception(
    _fixture_create_database,
    _fixture_spark_expectations,
    _fixture_df,
    _fixture_rules_df,
    _fixture_local_kafka_topic,
):
    """
    Test that invalid rules do not raise exceptions - validation is non-blocking.
    Invalid rules are logged as warnings but execution continues.
    """
    partial_func = _fixture_spark_expectations.with_expectations(
        "dq_spark.test_final_table",
        user_conf={user_config.se_notifications_on_fail: False},
    )

    mock_func = Mock(return_value=_fixture_df)
    decorated_func = partial_func(mock_func)
    
    # Should NOT raise an exception for invalid rules - validation is non-blocking
    result = decorated_func()
    
    assert result is not None
        
    for db in spark.catalog.listDatabases():
        if db.name != "default":
            spark.sql(f"DROP DATABASE {db.name} CASCADE")
    spark.sql("CLEAR CACHE")


def test_with_expectations_exception(_fixture_create_database, _fixture_spark_expectations, _fixture_local_kafka_topic):
    rules_dict = [
        {
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
    ]

    rules_df = spark.createDataFrame(rules_dict)
    writer = WrappedDataFrameWriter().mode("append").format("delta")
    se = SparkExpectations(
        product_id="product1",
        rules_df=rules_df,
        stats_table="dq_spark.test_dq_stats_table",
        stats_table_writer=writer,
        target_and_error_table_writer=writer,
        debugger=False,
    )
    partial_func = se.with_expectations(
        "dq_spark.test_final_table",
        user_conf={user_config.se_notifications_on_fail: False},
    )

    with pytest.raises(
        SparkExpectationsMiscException,
        match=r"error occurred while processing spark expectations",
    ):
        # Create a mock object with a list return value
        mock_func = Mock(return_value=["apple", "banana", "pineapple", "orange"])

        # Decorate the mock function with required args
        decorated_func = partial_func(mock_func)
        decorated_func()

    for db in spark.catalog.listDatabases():
        if db.name != "default":
            spark.sql(f"DROP DATABASE {db.name} CASCADE")
    spark.sql("CLEAR CACHE")


def test_with_expectations_negative_parameter(
    _fixture_create_database, _fixture_spark_expectations, _fixture_local_kafka_topic
):
    rules_dict = [
        {
            "product_id": "product1",
            "table_name": "dq_spark.test_final_table",
            "rule_type": "query_dq",
            "rule": "sum_col1_threshold",
            "column_name": "col1",
            "expectation": "(select sum(col1) from {table2}) > 10",
            "action_if_failed": "ignore",
            "tag": "validity",
            "description": "sum of col1 value must be greater than 10",
            "enable_for_source_dq_validation": True,
            "enable_for_target_dq_validation": True,
            "is_active": True,
            "enable_error_drop_alert": False,
            "error_drop_threshold": "20",
            "priority": "medium",
        }
    ]

    rules_df = spark.createDataFrame(rules_dict)
    writer = WrappedDataFrameWriter().mode("append").format("delta")
    se = SparkExpectations(
        product_id="product1",
        rules_df=rules_df,
        stats_table="dq_spark.test_dq_stats_table",
        stats_table_writer=writer,
        target_and_error_table_writer=writer,
        debugger=False,
    )
    partial_func = se.with_expectations(
        "dq_spark.test_final_table",
        user_conf={user_config.se_notifications_on_fail: False},
    )

    with pytest.raises(
        SparkExpectationsMiscException,
        match=r"error occurred while retrieving rules list from the table 'table2'",
    ):
        # Create a mock object with a list return value
        mock_func = Mock(return_value=["apple", "banana", "pineapple", "orange"])

        # Decorate the mock function with required args
        decorated_func = partial_func(mock_func)
        decorated_func()

    for db in spark.catalog.listDatabases():
        if db.name != "default":
            spark.sql(f"DROP DATABASE {db.name} CASCADE")
    spark.sql("CLEAR CACHE")


# @patch('spark_expectations.core.expectations.SparkExpectationsNotify', autospec=True,
#        spec_set=True)
# @patch('spark_expectations.notifications.push.spark_expectations_notify._notification_hook', autospec=True,
#        spec_set=True)
def test_error_threshold_breach(
    # _mock_notification_hook, _mock_spark_expectations_notify,
    _fixture_create_database,
    _fixture_local_kafka_topic,
):
    input_df = spark.createDataFrame(
        [
            {"col1": 1, "col2": "a", "col3": 4},
            {"col1": 2, "col2": "b", "col3": 5},
            {"col1": 3, "col2": "c", "col3": 6},
        ]
    )

    rules = [
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
            "enable_for_source_dq_validation": True,
            "enable_for_target_dq_validation": True,
            "is_active": True,
            "enable_error_drop_alert": True,
            "error_drop_threshold": "25",
            "priority": "medium",
        },
        {
            "product_id": "product1",
            "table_name": "dq_spark.test_final_table",
            "rule_type": "query_dq",
            "rule": "col3_positive_threshold",
            "column_name": "col3",
            "expectation": "(select count(case when col3>0 then 1 else 0 end) from test_final_table_view) > 10",
            "action_if_failed": "ignore",
            "tag": "strict",
            "description": "count of col3 positive value must be greater than 10",
            "enable_for_source_dq_validation": False,
            "enable_for_target_dq_validation": True,
            "is_active": True,
            "enable_error_drop_alert": True,
            "error_drop_threshold": "10",
            "priority": "medium",
        },
    ]

    # create a PySpark DataFrame from the list of dictionaries
    rules_df = spark.createDataFrame(rules)

    writer = WrappedDataFrameWriter().mode("append").format("delta")

    with patch(
        "spark_expectations.notifications.push.spark_expectations_notify.SparkExpectationsNotify"
        ".notify_on_exceeds_of_error_threshold",
        autospec=True,
        spec_set=True,
    ) as _mock_notification_hook:
        se = SparkExpectations(
            product_id="product1",
            rules_df=rules_df,
            stats_table="dq_spark.test_dq_stats_table",
            stats_table_writer=writer,
            target_and_error_table_writer=writer,
            debugger=False,
        )

        from spark_expectations.config.user_config import Constants as user_config

        conf = {
            user_config.se_notifications_on_fail: True,
            user_config.se_notifications_on_error_drop_exceeds_threshold_breach: True,
            user_config.se_notifications_on_error_drop_threshold: 15,
        }

        @se.with_expectations(
            target_table="dq_spark.test_final_table",
            write_to_table=True,
            user_conf=conf,
        )
        def get_dataset() -> DataFrame:
            return input_df

        get_dataset()

    _mock_notification_hook.assert_called_once()
    for db in spark.catalog.listDatabases():
        if db.name != "default":
            spark.sql(f"DROP DATABASE {db.name} CASCADE")
    spark.sql("CLEAR CACHE")


def test_se_notifications_on_rules_action_if_failed_set_ignore_sends_notification(_fixture_create_database):
    input_df = spark.createDataFrame(
        [
            ("1", "product1", 10),
            ("2", "product2", 20),
            ("3", "product3", 30),
        ],
        ["id", "product", "value"],
    )
    input_df.createOrReplaceTempView("test_table")

    rules = [
        {
            "product_id": "product1",
            "table_name": "dq_spark.test_final_table",
            "rule_type": "row_dq",
            "rule": "value_should_be_bigger_than_0",
            "column_name": "value",
            "expectation": "value > 0",
            "action_if_failed": "ignore",
            "tag": "strict",
            "description": "value must be greater than 10",
            "enable_for_source_dq_validation": True,
            "enable_for_target_dq_validation": True,
            "is_active": True,
            "enable_error_drop_alert": True,
            "error_drop_threshold": "20",
            "priority": "medium",
        },
        {
            "product_id": "product1",
            "table_name": "dq_spark.test_final_table",
            "rule_type": "row_dq",
            "rule": "value_must_be_greater_than_10",
            "column_name": "value",
            "expectation": "value > 10",
            "action_if_failed": "ignore",
            "tag": "strict",
            "description": "value must be greater than 10",
            "enable_for_source_dq_validation": True,
            "enable_for_target_dq_validation": True,
            "is_active": True,
            "enable_error_drop_alert": True,
            "error_drop_threshold": "20",
            "priority": "medium",
        },
        {
            "product_id": "product1",
            "table_name": "dq_spark.test_final_table",
            "rule_type": "agg_dq",
            "rule": "sum_of_value_should_be_less_than_60",
            "column_name": "value",
            "expectation": "sum(value) > 60",
            "action_if_failed": "ignore",
            "tag": "strict",
            "description": "desc_sum_of_value_should_be_less_than_60",
            "enable_for_source_dq_validation": True,
            "enable_for_target_dq_validation": True,
            "is_active": True,
            "enable_error_drop_alert": True,
            "error_drop_threshold": "25",
            "priority": "medium",
        },
        {
            "product_id": "product1",
            "table_name": "dq_spark.test_final_table",
            "rule_type": "query_dq",
            "rule": "count_of_records_must_be_greater_than_10",
            "column_name": "col3",
            "expectation": "(select count(*) from test_table) > 10",
            "action_if_failed": "ignore",
            "tag": "strict",
            "description": "count of records must be greater than 10",
            "enable_for_source_dq_validation": True,
            "enable_for_target_dq_validation": True,
            "is_active": True,
            "enable_error_drop_alert": True,
            "error_drop_threshold": "10",
            "priority": "medium",
        },
    ]
    rules_df = spark.createDataFrame(rules)

    writer = WrappedDataFrameWriter().mode("append").format("delta")

    from spark_expectations.config.user_config import Constants as user_config

    conf = {
        user_config.se_notifications_on_rules_action_if_failed_set_ignore: True,
        user_config.se_enable_query_dq_detailed_result: True,
        user_config.se_enable_agg_dq_detailed_result: True,
        user_config.se_notifications_on_error_drop_threshold: 15,
    }

    with patch(
        "spark_expectations.notifications.push.spark_expectations_notify.SparkExpectationsNotify"
        ".notify_on_ignore_rules",
        autospec=True,
        spec_set=True,
    ) as _mock_notification_hook:
        se = SparkExpectations(
            product_id="product1",
            rules_df=rules_df,
            stats_table="dq_spark.test_dq_stats_table",
            stats_table_writer=writer,
            target_and_error_table_writer=writer,
            debugger=False,
            stats_streaming_options={user_config.se_enable_streaming: False},
        )

        @se.with_expectations(
            target_table="dq_spark.test_final_table",
            write_to_table=True,
            user_conf=conf,
        )
        def get_dataset() -> DataFrame:
            return input_df

        get_dataset()

        expected_rules = [
            {
                "rule_type": "row_dq",
                "rule": "value_must_be_greater_than_10",
                "priority": "medium",
                "description": "value must be greater than 10",
                "column_name": "value",
                "tag": "strict",
                "action_if_failed": "ignore",
                "failed_row_count": 1,
            },
            {
                "rule": "count_of_records_must_be_greater_than_10",
                "description": "count of records must be greater than 10",
                "priority": "medium",
                "rule_type": "query_dq",
                "column_name": "col3",
                "tag": "strict",
                "status": "fail",
                "action_if_failed": "ignore",
            },
            {
                "rule": "sum_of_value_should_be_less_than_60",
                "description": "desc_sum_of_value_should_be_less_than_60",
                "priority": "medium",
                "rule_type": "agg_dq",
                "column_name": "value",
                "tag": "strict",
                "status": "fail",
                "action_if_failed": "ignore",
            },
            {
                "rule": "count_of_records_must_be_greater_than_10",
                "description": "count of records must be greater than 10",
                "priority": "medium",
                "rule_type": "query_dq",
                "column_name": "col3",
                "tag": "strict",
                "status": "fail",
                "action_if_failed": "ignore",
            },
            {
                "rule": "sum_of_value_should_be_less_than_60",
                "description": "desc_sum_of_value_should_be_less_than_60",
                "priority": "medium",
                "rule_type": "agg_dq",
                "column_name": "value",
                "tag": "strict",
                "status": "fail",
                "action_if_failed": "ignore",
            },
        ]

        # Verify the mock was called once
        _mock_notification_hook.assert_called_once()

        # Get the actual call arguments (self, rules_list)
        actual_call_args = _mock_notification_hook.call_args[0]
        actual_rules = actual_call_args[1]

        # Verify that each expected rule is a subset of the corresponding actual rule
        # (actual rules may contain additional fields like id_hash, expectation_hash)
        assert len(actual_rules) == len(expected_rules)
        for expected, actual in zip(expected_rules, actual_rules):
            for key, value in expected.items():
                assert key in actual, f"Missing key '{key}' in actual rule"
                assert actual[key] == value, f"Mismatch for key '{key}': expected {value}, got {actual[key]}"

    for db in spark.catalog.listDatabases():
        if db.name != "default":
            spark.sql(f"DROP DATABASE {db.name} CASCADE")
    spark.sql("CLEAR CACHE")


def test_target_table_view_exception(_fixture_create_database, _fixture_local_kafka_topic):
    rules = [
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
        },
        {
            "product_id": "product1",
            "table_name": "dq_spark.test_final_table",
            "rule_type": "query_dq",
            "rule": "col3_positive_threshold",
            "column_name": "col3",
            "expectation": "(select count(case when col3>0 then 1 else 0 end) from target_test_table) > 10",
            "action_if_failed": "ignore",
            "tag": "strict",
            "description": "count of col3 positive value must be greater than 10",
            "enable_for_source_dq_validation": False,
            "enable_for_target_dq_validation": True,
            "is_active": True,
            "enable_error_drop_alert": True,
            "error_drop_threshold": "10",
            "priority": "medium",
        },
    ]

    input_df = spark.createDataFrame(
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
    )

    # create a PySpark DataFrame from the list of dictionaries
    rules_df = spark.createDataFrame(rules)
    rules_df.createOrReplaceTempView("test_table")

    writer = WrappedDataFrameWriter().mode("append").format("delta")
    se = SparkExpectations(
        product_id="product1",
        rules_df=rules_df,
        stats_table="dq_spark.test_dq_stats_table",
        stats_table_writer=writer,
        target_and_error_table_writer=writer,
        debugger=False,
    )

    @se.with_expectations(
        target_table="dq_spark.test_final_table",
        write_to_table=True,
        target_table_view="test_table",
    )
    def get_dataset() -> DataFrame:
        return input_df

    with pytest.raises(
        SparkExpectationsMiscException,
        match=r"error occurred while processing spark expectations .*",
    ):
        get_dataset()  # decorated_func()

    for db in spark.catalog.listDatabases():
        if db.name != "default":
            spark.sql(f"DROP DATABASE {db.name} CASCADE")
    spark.sql("CLEAR CACHE")


def test_spark_expectations_exception():
    writer = WrappedDataFrameWriter().mode("append").format("parquet")
    with pytest.raises(SparkExpectationsMiscException, match=r"Input rules_df is not of dataframe type"):
        SparkExpectations(
            product_id="product1",
            rules_df=[],
            stats_table="dq_spark.test_dq_stats_table",
            stats_table_writer=writer,
            target_and_error_table_writer=writer,
            debugger=False,
        )


# [UnitTests for WrappedDataFrameWriter class]


def reset_wrapped_dataframe_writer():
    writer = WrappedDataFrameWriter()
    writer._mode = None
    writer._format = None
    writer._partition_by = []
    writer._options = {}
    writer._bucket_by = {}
    writer._sort_by = []


def test_mode():
    assert WrappedDataFrameWriter().mode("overwrite")._mode == "overwrite"


def test_format():
    assert WrappedDataFrameWriter().format("parquet")._format == "parquet"


def test_partitionBy():
    assert WrappedDataFrameWriter().partitionBy("date", "region")._partition_by == [
        "date",
        "region",
    ]


def test_option():
    assert WrappedDataFrameWriter().option("compression", "gzip")._options == {"compression": "gzip"}


def test_options():
    assert WrappedDataFrameWriter().options(path="/path/to/output", inferSchema="true")._options == {
        "path": "/path/to/output",
        "inferSchema": "true",
    }


def test_bucketBy():
    assert WrappedDataFrameWriter().bucketBy(4, "country", "city")._bucket_by == {
        "num_buckets": 4,
        "columns": ("country", "city"),
    }


def test_build():
    writer = (
        WrappedDataFrameWriter()
        .mode("overwrite")
        .format("parquet")
        .partitionBy("date", "region")
        .option("compression", "gzip")
        .options(path="/path/to/output", inferSchema="true")
        .bucketBy(4, "country", "city")
        .sortBy("col1", "col2")
    )
    expected_config = {
        "mode": "overwrite",
        "format": "parquet",
        "partitionBy": ["date", "region"],
        "options": {
            "compression": "gzip",
            "path": "/path/to/output",
            "inferSchema": "true",
        },
        "bucketBy": {"num_buckets": 4, "columns": ("country", "city")},
        "sortBy": ["col1", "col2"],
    }
    assert writer.build() == expected_config


def test_build_some_values():
    writer = WrappedDataFrameWriter().mode("append").format("iceberg")

    expected_config = {
        "mode": "append",
        "format": "iceberg",
        "partitionBy": [],
        "options": {},
        "bucketBy": {},
        "sortBy": [],
    }
    assert writer.build() == expected_config


def test_delta_bucketby_exception():
    writer = WrappedDataFrameWriter().mode("append").format("delta").bucketBy(10, "a", "b")
    with pytest.raises(
        SparkExpectationsMiscException,
        match=r"Bucketing is not supported for delta tables yet",
    ):
        writer.build()


class TestCheckIfPysparkConnectIsSupported:
    def test_if_pyspark_connect_is_not_supported(self):
        """Test that check_if_pyspark_connect_is_supported returns False when
        pyspark connect is not supported."""
        with patch.dict("sys.modules", {"pyspark.sql.connect": None}):
            assert check_if_pyspark_connect_is_supported() is False

    def test_check_if_pyspark_connect_is_supported(self):
        """Test that check_if_pyspark_connect_is_supported returns True when
        pyspark connect is supported."""
        with (
            patch("spark_expectations.core.expectations.SPARK_MINOR_VERSION", 3.5),
            patch.dict(
                "sys.modules",
                {
                    "pyspark.sql.connect.column": MagicMock(Column=MagicMock()),
                    "pyspark.sql.connect": MagicMock(),
                },
            ),
        ):
            assert check_if_pyspark_connect_is_supported() is True


def test_get_spark_minor_version():
    """Test that get_spark_minor_version returns the correctly formatted version."""
    with patch("spark_expectations.core.expectations.spark_version", "9.9.42"):
        assert get_spark_minor_version() == 9.9

def test_agg_rule_for_non_int_column():
    """
    Test that the agg_dq rule works for columns of type date and string.
    """
    # Initialize Spark session
    spark = set_up_delta()

    RULES_DATA = """
        ("my_product", "dq_spark_dev.d", "agg_dq", "r1", "dt", "min(dt) < current_date()", "fail", "accuracy", "rn1", true, false, true, false, 0, null, null, "medium"),
        ("my_product", "dq_spark_dev.d", "agg_dq", "r2", "dt", "max(dt) >= current_date()", "fail", "accuracy", "rn2", true, false, true, false, 0, null, null, "medium"),
        ("my_product", "dq_spark_dev.d", "agg_dq", "r3", "dt", "min(str) < 'B'", "fail", "accuracy", "rn3", true, false, true, false, 0, null, null, "medium"),
        ("my_product", "dq_spark_dev.d", "agg_dq", "r4", "dt", "max(str) > 'B'", "fail", "accuracy", "rn4", true, false, true, false, 0, null, null, "medium")
        """

    spark.sql("create database if not exists dq_spark_dev")
    spark.sql("use dq_spark_dev")
    spark.sql("drop table if exists dq_stats")
    spark.sql("drop table if exists dq_rules")
    spark.sql(f" CREATE TABLE dq_rules {RULES_TABLE_SCHEMA} USING DELTA")
    spark.sql(f" INSERT INTO dq_rules VALUES {RULES_DATA}")

    spark.sql(f" CREATE TABLE d (str STRING, dt DATE) USING DELTA")
    spark.sql(f" INSERT INTO d VALUES ('A', '2030-01-01'), ('D', '2022-01-01')")

    table_name = "dq_spark_dev.d"

    writer = WrappedDataFrameWriter().mode("append").format("delta")

    se: SparkExpectations = SparkExpectations(
        product_id="my_product",
        rules_df=spark.table("dq_spark_dev.dq_rules"),
        stats_table="dq_spark_dev.dq_stats",
        stats_table_writer=writer,
        target_and_error_table_writer=writer,
        stats_streaming_options={SeUserConfig.se_enable_streaming: False},
        #spark = spark
    )

    se_user_conf={
            SeUserConfig.se_notifications_enable_email: False,
            SeUserConfig.se_notifications_enable_slack: False,
            SeUserConfig.se_enable_query_dq_detailed_result: True,
            SeUserConfig.se_enable_agg_dq_detailed_result: True,
            SeUserConfig.se_enable_error_table: True,
            SeUserConfig.se_dq_rules_params: {"table": table_name},
        }
        
    @se.with_expectations(
            target_table=table_name,
            user_conf=se_user_conf,
            write_to_table=False,
            write_to_temp_table=False,
        )
    def inner() -> DataFrame:
        return spark.table(table_name)

    try:
        output_df = inner()
        output_df.show(truncate=False)
        assert True  # Assert that the method runs without exceptions
    except Exception as e:
        assert False, f"Method raised an exception: {e}"


# [Unit Tests for WrappedDataFrameStreamWriter class]


def test_stream_writer_output_mode():
    assert WrappedDataFrameStreamWriter().outputMode("append")._output_mode == "append"


def test_stream_writer_format():
    assert WrappedDataFrameStreamWriter().format("delta")._format == "delta"


def test_stream_writer_query_name():
    assert WrappedDataFrameStreamWriter().queryName("test_query")._query_name == "test_query"


def test_stream_writer_trigger():
    writer = WrappedDataFrameStreamWriter().trigger(processingTime="10 seconds")
    assert writer._trigger == {"processingTime": "10 seconds"}


def test_stream_writer_partition_by():
    assert WrappedDataFrameStreamWriter().partitionBy("date", "region")._partition_by == [
        "date",
        "region",
    ]


def test_stream_writer_partition_by_with_list():
    """Test partitionBy with a list argument"""
    assert WrappedDataFrameStreamWriter().partitionBy(["date", "region"])._partition_by == [
        "date",
        "region",
    ]


def test_stream_writer_partition_by_chained():
    """Test partitionBy can be chained and handles both string args and list args"""
    writer = (
        WrappedDataFrameStreamWriter()
        .partitionBy("date")
        .partitionBy(["region", "country"])
    )
    assert writer._partition_by == ["date", "region", "country"]


def test_stream_writer_option():
    assert WrappedDataFrameStreamWriter().option("checkpointLocation", "/path/to/checkpoint")._options == {
        "checkpointLocation": "/path/to/checkpoint"
    }


def test_stream_writer_options():
    assert WrappedDataFrameStreamWriter().options(
        checkpointLocation="/path/to/checkpoint", maxFilesPerTrigger="100"
    )._options == {
        "checkpointLocation": "/path/to/checkpoint",
        "maxFilesPerTrigger": "100",
    }


def test_stream_writer_build():
    writer = (
        WrappedDataFrameStreamWriter()
        .outputMode("append")
        .format("delta")
        .queryName("test_query")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", "/path/to/checkpoint")
        .options(maxFilesPerTrigger="100")
        .partitionBy("date", "region")
    )
    expected_config = {
        "outputMode": "append",
        "format": "delta",
        "queryName": "test_query",
        "trigger": {"processingTime": "10 seconds"},
        "partitionBy": ["date", "region"],
        "options": {
            "checkpointLocation": "/path/to/checkpoint",
            "maxFilesPerTrigger": "100",
        },
    }
    assert writer.build() == expected_config


def test_stream_writer_build_some_values():
    writer = WrappedDataFrameStreamWriter().outputMode("append").format("delta")

    expected_config = {
        "outputMode": "append",
        "format": "delta",
        "queryName": None,
        "trigger": None,
        "partitionBy": [],
        "options": {},
    }
    assert writer.build() == expected_config


# [Unit Tests for SparkExpectations writer type initialization]


def test_spark_expectations_with_batch_writers(_fixture_rules_df):
    """Test SparkExpectations initialization with WrappedDataFrameWriter for both writers"""
    batch_writer = WrappedDataFrameWriter().mode("append").format("delta")
    
    se = SparkExpectations(
        product_id="test_product",
        rules_df=_fixture_rules_df,
        stats_table="dq_spark.test_stats",
        stats_table_writer=batch_writer,
        target_and_error_table_writer=batch_writer,
        debugger=False,
    )
    
    # Verify that streaming type is not set for batch writers
    # The context should have default (non-streaming) configuration
    assert se._context.get_target_and_error_table_writer_config == batch_writer.build()
    assert se._context.get_stats_table_writer_config == batch_writer.build()


def test_spark_expectations_with_streaming_target_and_error_writer(_fixture_rules_df):
    """Test SparkExpectations initialization with WrappedDataFrameStreamWriter for target_and_error_table_writer"""
    stream_writer = WrappedDataFrameStreamWriter().outputMode("append").format("delta").option("checkpointLocation", "/tmp/checkpoint1")
    batch_writer = WrappedDataFrameWriter().mode("append").format("delta")
    
    se = SparkExpectations(
        product_id="test_product",
        rules_df=_fixture_rules_df,
        stats_table="dq_spark.test_stats",
        stats_table_writer=batch_writer,
        target_and_error_table_writer=stream_writer,
        debugger=False,
    )
    
    expected_stream_writer_build = {'outputMode': "append",
                                    'format': 'delta',
                                    'queryName': None,
                                    'trigger': None,
                                    'partitionBy': [],
                                    'options': {'checkpointLocation': "/tmp/checkpoint1"}}
    
    expected_batch_writer_build = {'mode': "append",
                                   'format': 'delta',
                                   "partitionBy": [],
                                    "options": {},
                                    "bucketBy": {},
                                    "sortBy": []
                                    }

    # Verify that streaming type is set for target_and_error_table_writer
    assert se._context.get_target_and_error_table_writer_config == expected_stream_writer_build
    assert se._context.get_stats_table_writer_config == expected_batch_writer_build
    # Verify the writer type was set to streaming
    assert isinstance(se.target_and_error_table_writer, WrappedDataFrameStreamWriter)


def test_spark_expectations_with_streaming_stats_writer(_fixture_rules_df):
    """Test SparkExpectations initialization with WrappedDataFrameStreamWriter for stats_table_writer"""
    stream_writer = WrappedDataFrameStreamWriter().outputMode("append").format("delta").option("checkpointLocation", "/tmp/checkpoint2")
    batch_writer = WrappedDataFrameWriter().mode("append").format("delta")
    
    se = SparkExpectations(
        product_id="test_product",
        rules_df=_fixture_rules_df,
        stats_table="dq_spark.test_stats",
        stats_table_writer=stream_writer,
        target_and_error_table_writer=batch_writer,
        debugger=False,
    )
    
    # Verify that streaming type is set for stats_table_writer
    assert se._context.get_target_and_error_table_writer_config == batch_writer.build()
    assert se._context.get_stats_table_writer_config == stream_writer.build()
    # Verify the writer type was set to streaming
    assert isinstance(se.stats_table_writer, WrappedDataFrameStreamWriter)


def test_spark_expectations_with_both_streaming_writers(_fixture_rules_df):
    """Test SparkExpectations initialization with WrappedDataFrameStreamWriter for both writers"""
    stream_writer_target = WrappedDataFrameStreamWriter().outputMode("append").format("delta").option("checkpointLocation", "/tmp/checkpoint3")
    stream_writer_stats = WrappedDataFrameStreamWriter().outputMode("complete").format("delta").option("checkpointLocation", "/tmp/checkpoint4")
    
    se = SparkExpectations(
        product_id="test_product",
        rules_df=_fixture_rules_df,
        stats_table="dq_spark.test_stats",
        stats_table_writer=stream_writer_stats,
        target_and_error_table_writer=stream_writer_target,
        debugger=False,
    )
    
    # Verify that streaming type is set for both writers
    assert se._context.get_target_and_error_table_writer_config == stream_writer_target.build()
    assert se._context.get_stats_table_writer_config == stream_writer_stats.build()
    # Verify both writer types were set to streaming
    assert isinstance(se.target_and_error_table_writer, WrappedDataFrameStreamWriter)
    assert isinstance(se.stats_table_writer, WrappedDataFrameStreamWriter)


def test_spark_expectations_writer_type_mixed_configurations(_fixture_rules_df):
    """Test SparkExpectations with different writer configurations"""
    # Test case 1: Stream target, batch stats
    stream_writer = WrappedDataFrameStreamWriter().outputMode("append").format("delta")
    batch_writer = WrappedDataFrameWriter().mode("overwrite").format("parquet")
    
    se1 = SparkExpectations(
        product_id="test_product_1",
        rules_df=_fixture_rules_df,
        stats_table="dq_spark.test_stats_1",
        stats_table_writer=batch_writer,
        target_and_error_table_writer=stream_writer,
        debugger=False,
    )
    
    assert se1._context.get_target_and_error_table_writer_config["outputMode"] == "append"
    assert se1._context.get_stats_table_writer_config["mode"] == "overwrite"
    
    # Test case 2: Batch target, stream stats
    se2 = SparkExpectations(
        product_id="test_product_2",
        rules_df=_fixture_rules_df,
        stats_table="dq_spark.test_stats_2",
        stats_table_writer=stream_writer,
        target_and_error_table_writer=batch_writer,
        debugger=False,
    )
    
    assert se2._context.get_target_and_error_table_writer_config["mode"] == "overwrite"
    assert se2._context.get_stats_table_writer_config["outputMode"] == "append"


def test_spark_expectations_writer_configs_are_correctly_set(_fixture_rules_df):
    """Test that writer configurations are properly stored in context during initialization"""
    # Create writers with specific configurations
    target_writer = WrappedDataFrameWriter().mode("append").format("delta").partitionBy("date", "region")
    stats_writer = WrappedDataFrameStreamWriter().outputMode("complete").format("kafka").option("topic", "test-topic")
    
    se = SparkExpectations(
        product_id="test_product",
        rules_df=_fixture_rules_df,
        stats_table="dq_spark.test_stats",
        stats_table_writer=stats_writer,
        target_and_error_table_writer=target_writer,
        debugger=False,
    )
    
    # Verify target writer config
    target_config = se._context.get_target_and_error_table_writer_config
    assert target_config["mode"] == "append"
    assert target_config["format"] == "delta"
    assert target_config["partitionBy"] == ["date", "region"]
    
    # Verify stats writer config
    stats_config = se._context.get_stats_table_writer_config
    assert stats_config["outputMode"] == "complete"
    assert stats_config["format"] == "kafka"
    assert stats_config["options"]["topic"] == "test-topic"
    
    # Verify detailed stats writer config is also set
    detailed_stats_config = se._context.get_detailed_stats_table_writer_config
    assert detailed_stats_config["outputMode"] == "complete"
    assert detailed_stats_config["format"] == "kafka"


def test_streaming_dataframe_detection_log_agg_dq():
    """Test that streaming DataFrame detection logs the appropriate message"""
    
    # Create a streaming DataFrame
    streaming_df = spark.readStream.format("rate").option("rowsPerSecond", "1").load()
    streaming_df = streaming_df.withColumn(
        "meta_row_dq_results", array(create_map(lit("status"), lit("pass"), lit("action_if_failed"), lit("ignore")))
    ).withColumn("col1", lit(1))

    stream_writer = WrappedDataFrameStreamWriter().outputMode("append").format("delta").option("checkpointLocation", "/tmp/checkpoint1")
    batch_writer = WrappedDataFrameWriter().mode("append").format("delta")
    
    spark.sql("create database if not exists dq_spark")
    spark.sql("use dq_spark")
    
    rules_df = spark.createDataFrame([
        {
            "product_id": "test_product",
            "table_name": "dq_spark.test_target_table",
            "rule_type": "agg_dq",
            "rule": "data_existing",
            "column_name": "sales",
            "expectation": "count(*) > 0",
            "action_if_failed": "fail",
            "tag": "completeness",
            "description": "Data should be present",
            "enable_for_source_dq_validation": True,
            "enable_for_target_dq_validation": True,
            "is_active": True,
            "enable_error_drop_alert": False,
            "error_drop_threshold": 0,
        }
    ])
        
    # Create SparkExpectations instance
    se = SparkExpectations(
        product_id="test_product",
        rules_df=rules_df,
        stats_table="dq_spark.test_stats_table",
        target_and_error_table_writer=stream_writer,
        stats_table_writer= batch_writer
    )

    se._context.set_table_name("dq_spark.test_target_table")
    se._context.set_final_table_name("dq_spark.test_target_table")
    se._context.set_error_table_name("dq_spark.test_target_table_error")
    se._context.set_dq_stats_table_name("dq_spark.test_stats_table")

    with patch.object(se.reader, 'get_rules_from_df',
                      return_value=({}, [], {
                          'row_dq': False,
                          'source_agg_dq': True,
                          'target_agg_dq': True,
                          'source_query_dq': False,
                          'target_query_dq': False
                      })), \
        patch('spark_expectations.core.expectations._log') as mock_log,\
        patch.object(se._process, 'execute_dq_process', return_value=(streaming_df, [], 0, {})), \
        patch('spark_expectations.sinks.utils.writer.SparkExpectationsWriter.write_error_stats', return_value=None):
            
        @se.with_expectations(
            target_table="test_target_table",
            write_to_table=False
        )
        def streaming_data_function():
            return streaming_df
    
        streaming_data_function()
    
        logged = [args[0] for args, _ in mock_log.info.call_args_list]

        # Assert presence only (ignores other logs)
        assert "Streaming dataframe detected. Only row_dq checks applicable." in logged
        assert "agg_dq expectations provided. Not applicable for streaming dataframe." in logged


def test_streaming_dataframe_detection_log_query_dq():
    """Test that streaming DataFrame detection logs the appropriate message"""
    
    # Create a streaming DataFrame
    streaming_df = spark.readStream.format("rate").option("rowsPerSecond", "1").load()
    streaming_df = streaming_df.withColumn(
        "meta_row_dq_results", array(create_map(lit("status"), lit("pass"), lit("action_if_failed"), lit("ignore")))
    ).withColumn("col1", lit(1))

    stream_writer = WrappedDataFrameStreamWriter().outputMode("append").format("delta").option("checkpointLocation", "/tmp/checkpoint1")
    batch_writer = WrappedDataFrameWriter().mode("append").format("delta")
    
    spark.sql("create database if not exists dq_spark")
    spark.sql("use dq_spark")
    
    rules_df = spark.createDataFrame([
        {
            "product_id": "test_product",
            "table_name": "dq_spark.test_target_table",
            "rule_type": "query_dq",
            "rule": "data_existing",
            "column_name": "sales",
            "expectation": "(select count(*) from dq_spark.test_target_table) > 0",
            "action_if_failed": "fail",
            "tag": "completeness",
            "description": "Data should be present",
            "enable_for_source_dq_validation": True,
            "enable_for_target_dq_validation": True,
            "is_active": True,
            "enable_error_drop_alert": False,
            "error_drop_threshold": 0,
        }
    ])
        
    # Create SparkExpectations instance
    se = SparkExpectations(
        product_id="test_product",
        rules_df=rules_df,
        stats_table="dq_spark.test_stats_table",
        target_and_error_table_writer=stream_writer,
        stats_table_writer= batch_writer
    )

    se._context.set_table_name("dq_spark.test_target_table")
    se._context.set_final_table_name("dq_spark.test_target_table")
    se._context.set_error_table_name("dq_spark.test_target_table_error")
    se._context.set_dq_stats_table_name("dq_spark.test_stats_table")

    with patch.object(se.reader, 'get_rules_from_df',
                      return_value=({}, [], {
                          'row_dq': False,
                          'source_agg_dq': False,
                          'target_agg_dq': False,
                          'source_query_dq': True,
                          'target_query_dq': True
                      })), \
        patch('spark_expectations.core.expectations._log') as mock_log,\
        patch.object(se._process, 'execute_dq_process', return_value=(streaming_df, [], 0, {})), \
        patch('spark_expectations.sinks.utils.writer.SparkExpectationsWriter.write_error_stats', return_value=None):
            
        @se.with_expectations(
            target_table="test_target_table",
            write_to_table=False
        )
        def streaming_data_function():
            return streaming_df
    
        streaming_data_function()
    
        logged = [args[0] for args, _ in mock_log.info.call_args_list]

        # Assert presence only (ignores other logs)
        assert "Streaming dataframe detected. Only row_dq checks applicable." in logged
        assert "query_dq expectations provided. Not applicable for streaming dataframe." in logged


def test_streaming_dataframe_detection_log_agg_query_dq():
    """Test that streaming DataFrame detection logs the appropriate message"""
    
    # Create a streaming DataFrame
    streaming_df = spark.readStream.format("rate").option("rowsPerSecond", "1").load()
    streaming_df = streaming_df.withColumn(
        "meta_row_dq_results", array(create_map(lit("status"), lit("pass"), lit("action_if_failed"), lit("ignore")))
    ).withColumn("col1", lit(1))

    stream_writer = WrappedDataFrameStreamWriter().outputMode("append").format("delta").option("checkpointLocation", "/tmp/checkpoint1")
    batch_writer = WrappedDataFrameWriter().mode("append").format("delta")
    
    spark.sql("create database if not exists dq_spark")
    spark.sql("use dq_spark")
    
    rules_df = spark.createDataFrame([
        {
            "product_id": "test_product",
            "table_name": "dq_spark.test_target_table",
            "rule_type": "query_dq",
            "rule": "data_existing",
            "column_name": "sales",
            "expectation": "(select count(*) from dq_spark.test_target_table) > 0",
            "action_if_failed": "fail",
            "tag": "completeness",
            "description": "Data should be present",
            "enable_for_source_dq_validation": True,
            "enable_for_target_dq_validation": True,
            "is_active": True,
            "enable_error_drop_alert": False,
            "error_drop_threshold": 0,
        },
        {
           "product_id": "test_product",
            "table_name": "dq_spark.test_target_table",
            "rule_type": "agg_dq",
            "rule": "data_existing",
            "column_name": "sales",
            "expectation": "count(*) > 0",
            "action_if_failed": "fail",
            "tag": "completeness",
            "description": "Data should be present",
            "enable_for_source_dq_validation": True,
            "enable_for_target_dq_validation": True,
            "is_active": True,
            "enable_error_drop_alert": False,
            "error_drop_threshold": 0, 
        }
    ])
        
    # Create SparkExpectations instance
    se = SparkExpectations(
        product_id="test_product",
        rules_df=rules_df,
        stats_table="dq_spark.test_stats_table",
        target_and_error_table_writer=stream_writer,
        stats_table_writer= batch_writer
    )

    se._context.set_table_name("dq_spark.test_target_table")
    se._context.set_final_table_name("dq_spark.test_target_table")
    se._context.set_error_table_name("dq_spark.test_target_table_error")
    se._context.set_dq_stats_table_name("dq_spark.test_stats_table")

    with patch.object(se.reader, 'get_rules_from_df',
                      return_value=({}, [], {
                          'row_dq': False,
                          'source_agg_dq': True,
                          'target_agg_dq': True,
                          'source_query_dq': True,
                          'target_query_dq': True
                      })), \
        patch('spark_expectations.core.expectations._log') as mock_log,\
        patch.object(se._process, 'execute_dq_process', return_value=(streaming_df, [], 0, {})), \
        patch('spark_expectations.sinks.utils.writer.SparkExpectationsWriter.write_error_stats', return_value=None):
            
        @se.with_expectations(
            target_table="test_target_table",
            write_to_table=False
        )
        def streaming_data_function():
            return streaming_df
    
        streaming_data_function()
    
        logged = [args[0] for args, _ in mock_log.info.call_args_list]

        # Assert presence only (ignores other logs)
        assert "Streaming dataframe detected. Only row_dq checks applicable." in logged
        assert "agg_dq expectations provided. Not applicable for streaming dataframe." in logged
        assert "query_dq expectations provided. Not applicable for streaming dataframe." in logged