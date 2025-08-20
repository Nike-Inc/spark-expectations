import os
import unittest.mock
from datetime import datetime
from unittest.mock import MagicMock, patch, Mock

import pytest
from unittest.mock import Mock, patch
from pyspark.sql.functions import col
from pyspark.sql.functions import lit, to_timestamp
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
            },
        ]
    }

    # create mock _context object
    mock_context = Mock(spec=SparkExpectationsContext)
    setattr(mock_context, "get_dq_stats_table_name", "test_dq_stats_table")
    setattr(mock_context, "get_run_date", "2022-12-27 10:39:44")
    setattr(mock_context, "get_run_id", "product1_run_test")
    setattr(mock_context, "get_run_id_name", "meta_dq_run_id")
    setattr(mock_context, "get_run_date_time_name", "meta_dq_run_date")
    setattr(mock_context, "get_dq_expectations", expectations)
    mock_context.set_summarized_row_dq_res = MagicMock()
    mock_context.spark = spark
    mock_context.product_id = "product1"

    return mock_context


@pytest.fixture(name="_fixture_writer")
def fixture_writer(_fixture_context):
    # Create an instance of the class and set the product_id
    return SparkExpectationsWriter(_fixture_context)


@pytest.fixture(name="_fixture_dq_dataset")
def fixture_dq_dataset():
    return spark.createDataFrame(
        [
            (1, "a", {"id": "1", "rule": "rule1", "status": "fail"}, {}),
            (2, "b", {}, {}),
            (3, "c", {"id": "3", "rule": "rule1", "status": "fail"}, {"name": "c", "rule": "rule2", "status": "fail"}),
            (4, "d", {}, {"name": "d", "rule": "rule2", "status": "fail"}),
        ],
        ["id", "name", "row_dq_id", "row_dq_name"],
    )


@pytest.fixture(name="_fixture_expected_error_dataset")
def fixture_expected_error_dataset():
    spark.conf.set("spark.sql.session.timeZone", "Etc/UTC")
    return spark.createDataFrame(
        [
            (1, "a", [{"id": "1", "rule": "rule1", "status": "fail"}], "product1_run_test"),
            (
                3,
                "c",
                [{"id": "3", "rule": "rule1", "status": "fail"}, {"name": "c", "rule": "rule2", "status": "fail"}],
                "product1_run_test",
            ),
            (4, "d", [{"name": "d", "rule": "rule2", "status": "fail"}], "product1_run_test"),
        ],
        ["id", "name", "meta_row_dq_results", "run_id"],
    ).withColumn("meta_dq_run_date", to_timestamp(lit("2022-12-27 10:39:44")))


@pytest.fixture(name="_fixture_expected_dq_dataset")
def fixture_expected_dq_dataset():
    spark.conf.set("spark.sql.session.timeZone", "Etc/UTC")
    return spark.createDataFrame(
        [
            (1, "a", [{"id": "1", "rule": "rule1", "status": "fail"}], "product1_run_test"),
            (2, "b", [], "product1_run_test"),
            (
                3,
                "c",
                [{"id": "3", "rule": "rule1", "status": "fail"}, {"name": "c", "rule": "rule2", "status": "fail"}],
                "product1_run_test",
            ),
            (4, "d", [{"name": "d", "rule": "rule2", "status": "fail"}], "product1_run_test"),
        ],
        ["id", "name", "meta_row_dq_results", "meta_dq_run_id"],
    ).withColumn("meta_dq_run_date", to_timestamp(lit("2022-12-27 10:39:44")))


@pytest.mark.parametrize(
    "input_record",
    [
        (
            {
                "row_dq_rules": {
                    "product_id": "your_product",
                    "table_name": "dq_spark_local.customer_order",
                    "rule_type": "row_dq",
                    "rule": "sales_greater_than_zero",
                    "column_name": "sales",
                    "expectation": "sales > 2",
                    "action_if_failed": "drop",
                    "enable_for_source_dq_validation": False,
                    "enable_for_target_dq_validation": True,
                    "tag": "accuracy",
                    "description": "sales value should be greater than zero",
                    "enable_error_drop_alert": False,
                    "error_drop_threshold": 0,
                },
            }
        )
    ],
)
def test_get_row_dq_detailed_stats_exception(input_record, _fixture_writer):
    _mock_context = Mock(spec=SparkExpectationsContext)
    _mock_context.get_dq_rules_params = {"env": "test_env"}
    setattr(_mock_context, "get_dq_expectations", input_record.get("row_dq_rules"))
    _mock_context.spark = spark
    _fixture_writer = SparkExpectationsWriter(_mock_context)
    # faulty user input is given to test the exception functionality of the agg_dq_result
    with pytest.raises(
        SparkExpectationsMiscException,
        match=r"error occurred while fetching the stats from get_row_dq_detailed_stats .*",
    ):
        _fixture_writer.get_row_dq_detailed_stats()


@pytest.mark.parametrize(
    "input_record, expected_result,dq_check, writer_config",
    [
        (
            {
                "input_count": 100,
                "error_count": 10,
                "output_count": 90,
                "rules_execution_settings_config": {
                    "row_dq": True,
                    "source_agg_dq": True,
                    "source_query_dq": True,
                    "target_agg_dq": True,
                    "target_query_dq": True,
                },
                "agg_dq_detailed_stats_status": True,
                "source_agg_dq_status": "Passed",
                "final_agg_dq_status": "Passed",
                "query_dq_detailed_stats_status": False,
                "source_query_dq_status": "Passed",
                "final_query_dq_status": "Passed",
                "row_dq_status": "Passed",
                "summarised_row_dq_res": [
                    {
                        "rule_type": "row_dq",
                        "rule": "sales_greater_than_zero",
                        "description": "sales value should be greater than zero",
                        "failed_row_count": 1,
                        "tag": "validity",
                        "action_if_failed": "drop",
                    }
                ],
                "run_id": "product_1_01450932-d5c2-11ee-a9ca-88e9fe5a7109",
                "input_count": 5,
                "dq_expectations": {
                    "row_dq_rules": [
                        {
                            "product_id": "your_product",
                            "table_name": "dq_spark_local.customer_order",
                            "rule_type": "row_dq",
                            "rule": "sales_greater_than_zero",
                            "column_name": "sales",
                            "expectation": "sales > 2",
                            "action_if_failed": "drop",
                            "enable_for_source_dq_validation": False,
                            "enable_for_target_dq_validation": True,
                            "tag": "accuracy",
                            "description": "sales value should be greater than zero",
                            "enable_error_drop_alert": False,
                            "error_drop_threshold": 0,
                        }
                    ],
                },
                "test_dq_detailed_stats_table": "test_dq_detailed_stats_table",
                "test_querydq_output_custom_table_name": "test_querydq_output_custom_table_name",
                "detailed_stats_table_writer_config": {
                    "mode": "overwrite",
                    "format": "delta",
                    "partitionBy": [],
                    "bucketBy": {},
                    "sortBy": [],
                    "options": {"mergeSchema": "true"},
                },
                "rowdq_detailed_stats": [
                    (
                        "product_1_01450932-d5c2-11ee-a9ca-88e9fe5a7109",
                        "product_1",
                        "dq_spark_local.customer_order",
                        "row_dq",
                        "sales_greater_than_zero",
                        "sales",
                        "sales > 2",
                        "accuracy",
                        "sales value should be greater than zero",
                        "fail",
                        None,
                        None,
                        None,
                        4,
                        0,
                        4,
                        "2024-03-14 00:00:00",
                        "2024-03-14 00:10:00",
                    ),
                ],
                "source_agg_dq_detailed_stats": [
                    (
                        "product_1_01450932-d5c2-11ee-a9ca-88e9fe5a7109",
                        "product_1",
                        "dq_spark_local.customer_order",
                        "agg_dq",
                        "sum_of_sales",
                        "sales",
                        "sum(sales)>10000",
                        "validity",
                        "regex format validation for quantity",
                        "fail",
                        1988,
                        ">10000",
                        5,
                        0,
                        5,
                        "2024-03-14 00:00:00",
                        "2024-03-14 00:10:00",
                    ),
                ],
                "target_agg_dq_detailed_stats": [
                    (
                        "product_1_01450932-d5c2-11ee-a9ca-88e9fe5a7109",
                        "product_1",
                        "dq_spark_local.customer_order",
                        "agg_dq",
                        "sum_of_sales",
                        "sales",
                        "sum(sales)>10000",
                        "validity",
                        "regex format validation for quantity",
                        "fail",
                        1030,
                        ">10000",
                        4,
                        0,
                        4,
                        "2024-03-14 01:00:00",
                        "2024-03-14 01:10:00",
                    ),
                ],
                "source_query_dq_detailed_stats": [
                    (
                        "product_1_52fed65a-d670-11ee-8dfb-ae03267c3341",
                        "product_1",
                        "dq_spark_local.customer_order",
                        "query_dq",
                        "product_missing_count_threshold",
                        "product_id",
                        "((select count(*) from (select distinct product_id,order_id from order_source) a) - (select count(*) from (select distinct product_id,order_id from order_target) b) ) < 3",
                        "validity",
                        "row count threshold",
                        "pass",
                        1,
                        "<3",
                        5,
                        0,
                        5,
                        "2024-03-14 02:00:00",
                        "2024-03-14 02:10:00",
                    )
                ],
                "target_query_dq_detailed_stats": [
                    (
                        "product_1_52fed65a-d670-11ee-8dfb-ae03267c3341",
                        "product_1",
                        "dq_spark_local.customer_order",
                        "query_dq",
                        "product_missing_count_threshold",
                        "product_id",
                        "((select count(*) from (select distinct product_id,order_id from order_source) a) - (select count(*) from (select distinct product_id,order_id from order_target) b) ) < 3",
                        "validity",
                        "row count threshold",
                        "pass",
                        1,
                        "<3",
                        5,
                        0,
                        5,
                        "2024-03-14 03:00:00",
                        "2024-03-14 03:10:00",
                    )
                ],
                "source_query_dq_output": [
                    (
                        "your_product_96bb003e-e1cf-11ee-9a59-ae03267c3340",
                        "your_product",
                        "dq_spark_local.customer_order",
                        "product_missing_count_threshold",
                        "product_id",
                        "source_f1",
                        "_source_dq",
                        {
                            "source_f1": [
                                '{"product_id":"FUR-TA-10000577","order_id":"US-2015-108966"}',
                                '{"product_id":"OFF-ST-10000760","order_id":"US-2015-108966"}',
                                '{"product_id":"FUR-CH-10000454","order_id":"CA-2016-152156"}',
                                '{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156"}',
                                '{"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688"}',
                            ]
                        },
                        "2024-03-14 06:53:39",
                    ),
                    (
                        "your_product_96bb003e-e1cf-11ee-9a59-ae03267c3340",
                        "your_product",
                        "dq_spark_local.customer_order",
                        "product_missing_count_threshold",
                        "product_id",
                        "target_f1",
                        "_source_dq",
                        {
                            "target_f1": [
                                '{"product_id":"FUR-TA-10000577","order_id":"US-2015-108966"}',
                                '{"product_id":"FUR-CH-10000454","order_id":"CA-2016-152156"}',
                                '{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156"}',
                                '{"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688"}',
                            ]
                        },
                        "2024-03-14 06:53:39",
                    ),
                ],
                "target_query_dq_output": [
                    (
                        "your_product_96bb003e-e1cf-11ee-9a59-ae03267c3340",
                        "your_product",
                        "dq_spark_local.customer_order",
                        "product_missing_count_threshold",
                        "product_id",
                        "source_f1",
                        "_target_dq",
                        {
                            "source_f1": [
                                '{"product_id":"FUR-TA-10000577","order_id":"US-2015-108966"}',
                                '{"product_id":"OFF-ST-10000760","order_id":"US-2015-108966"}',
                                '{"product_id":"FUR-CH-10000454","order_id":"CA-2016-152156"}',
                                '{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156"}',
                                '{"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688"}',
                            ]
                        },
                        "2024-03-14 06:53:39",
                    ),
                    (
                        "your_product_96bb003e-e1cf-11ee-9a59-ae03267c3340",
                        "your_product",
                        "dq_spark_local.customer_order",
                        "product_missing_count_threshold",
                        "product_id",
                        "target_f1",
                        "_target_dq",
                        {
                            "target_f1": [
                                '{"product_id":"FUR-TA-10000577","order_id":"US-2015-108966"}',
                                '{"product_id":"FUR-CH-10000454","order_id":"CA-2016-152156"}',
                                '{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156"}',
                                '{"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688"}',
                            ]
                        },
                        "2024-03-14 06:53:39",
                    ),
                ],
            },
            {
                "product_id": "product_1",
                "table_name": "dq_spark_local.customer_order",
                "rule": "sum_of_sales",
                "column_name": "sales",
                "rule_type": "agg_dq",
                "source_expectations": "sum(sales)>10000",
                "source_dq_status": "fail",
                "source_dq_actual_result": "1988",
                "source_dq_row_count": "5",
                "target_expectations": "sum(sales)>10000",
                "target_dq_status": "fail",
                "target_dq_actual_result": "1030",
                "target_dq_row_count": "4",
                "source_expectations": "sum(sales)>10000",
            },
            "agg_dq",
            None,
        ),
        (
            {
                "input_count": 100,
                "error_count": 10,
                "output_count": 90,
                "rules_execution_settings_config": {
                    "row_dq": True,
                    "source_agg_dq": True,
                    "source_query_dq": True,
                    "target_agg_dq": True,
                    "target_query_dq": True,
                },
                "agg_dq_detailed_stats_status": True,
                "source_agg_dq_status": "Passed",
                "final_agg_dq_status": "Passed",
                "query_dq_detailed_stats_status": False,
                "source_query_dq_status": "Passed",
                "final_query_dq_status": "Passed",
                "row_dq_status": "Passed",
                "summarised_row_dq_res": [
                    {
                        "rule_type": "row_dq",
                        "rule": "sales_greater_than_zero",
                        "description": "sales value should be greater than zero",
                        "failed_row_count": 1,
                        "tag": "validity",
                        "action_if_failed": "drop",
                    }
                ],
                "run_id": "product_1_01450932-d5c2-11ee-a9ca-88e9fe5a7109",
                "input_count": 5,
                "dq_expectations": {
                    "row_dq_rules": [
                        {
                            "product_id": "your_product",
                            "table_name": "dq_spark_local.customer_order",
                            "rule_type": "row_dq",
                            "rule": "sales_greater_than_zero",
                            "column_name": "sales",
                            "expectation": "sales > 2",
                            "action_if_failed": "drop",
                            "enable_for_source_dq_validation": False,
                            "enable_for_target_dq_validation": True,
                            "tag": "accuracy",
                            "description": "sales value should be greater than zero",
                            "enable_error_drop_alert": False,
                            "error_drop_threshold": 0,
                        }
                    ],
                },
                "test_dq_detailed_stats_table": "test_dq_detailed_stats_table",
                "test_querydq_output_custom_table_name": "test_querydq_output_custom_table_name",
                "detailed_stats_table_writer_config": {
                    "mode": "overwrite",
                    "format": "delta",
                    "partitionBy": [],
                    "bucketBy": {},
                    "sortBy": [],
                    "options": {"mergeSchema": "true"},
                },
                "rowdq_detailed_stats": [
                    (
                        "product_1_01450932-d5c2-11ee-a9ca-88e9fe5a7109",
                        "product_1",
                        "dq_spark_local.customer_order",
                        "row_dq",
                        "sales_greater_than_zero",
                        "sales",
                        "sales > 2",
                        "accuracy",
                        "sales value should be greater than zero",
                        "fail",
                        None,
                        None,
                        None,
                        4,
                        0,
                        4,
                        "2024-03-14 00:00:00",
                        "2024-03-14 00:10:00",
                    ),
                ],
                "source_agg_dq_detailed_stats": [
                    (
                        "product_1_01450932-d5c2-11ee-a9ca-88e9fe5a7109",
                        "product_1",
                        "dq_spark_local.customer_order",
                        "agg_dq",
                        "sum_of_sales",
                        "sales",
                        "sum(sales)>10000",
                        "validity",
                        "regex format validation for quantity",
                        "fail",
                        1988,
                        ">10000",
                        5,
                        0,
                        5,
                        "2024-03-14 00:00:00",
                        "2024-03-14 00:10:00",
                    ),
                ],
                "target_agg_dq_detailed_stats": [
                    (
                        "product_1_01450932-d5c2-11ee-a9ca-88e9fe5a7109",
                        "product_1",
                        "dq_spark_local.customer_order",
                        "agg_dq",
                        "sum_of_sales",
                        "sales",
                        "sum(sales)>10000",
                        "validity",
                        "regex format validation for quantity",
                        "fail",
                        1030,
                        ">10000",
                        4,
                        0,
                        4,
                        "2024-03-14 01:00:00",
                        "2024-03-14 01:10:00",
                    ),
                ],
                "source_query_dq_detailed_stats": [
                    (
                        "product_1_52fed65a-d670-11ee-8dfb-ae03267c3341",
                        "product_1",
                        "dq_spark_local.customer_order",
                        "query_dq",
                        "product_missing_count_threshold",
                        "product_id",
                        "((select count(*) from (select distinct product_id,order_id from order_source) a) - (select count(*) from (select distinct product_id,order_id from order_target) b) ) < 3",
                        "validity",
                        "row count threshold",
                        "pass",
                        1,
                        "<3",
                        5,
                        0,
                        5,
                        "2024-03-14 02:00:00",
                        "2024-03-14 02:10:00",
                    )
                ],
                "target_query_dq_detailed_stats": [
                    (
                        "product_1_52fed65a-d670-11ee-8dfb-ae03267c3341",
                        "product_1",
                        "dq_spark_local.customer_order",
                        "query_dq",
                        "product_missing_count_threshold",
                        "product_id",
                        "((select count(*) from (select distinct product_id,order_id from order_source) a) - (select count(*) from (select distinct product_id,order_id from order_target) b) ) < 3",
                        "validity",
                        "row count threshold",
                        "pass",
                        1,
                        "<3",
                        5,
                        0,
                        5,
                        "2024-03-14 03:00:00",
                        "2024-03-14 03:10:00",
                    )
                ],
                "source_query_dq_output": [
                    (
                        "your_product_96bb003e-e1cf-11ee-9a59-ae03267c3340",
                        "your_product",
                        "dq_spark_local.customer_order",
                        "product_missing_count_threshold",
                        "product_id",
                        "source_f1",
                        "_source_dq",
                        {
                            "source_f1": [
                                '{"product_id":"FUR-TA-10000577","order_id":"US-2015-108966"}',
                                '{"product_id":"OFF-ST-10000760","order_id":"US-2015-108966"}',
                                '{"product_id":"FUR-CH-10000454","order_id":"CA-2016-152156"}',
                                '{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156"}',
                                '{"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688"}',
                            ]
                        },
                        "2024-03-14 06:53:39",
                    ),
                    (
                        "your_product_96bb003e-e1cf-11ee-9a59-ae03267c3340",
                        "your_product",
                        "dq_spark_local.customer_order",
                        "product_missing_count_threshold",
                        "product_id",
                        "target_f1",
                        "_source_dq",
                        {
                            "target_f1": [
                                '{"product_id":"FUR-TA-10000577","order_id":"US-2015-108966"}',
                                '{"product_id":"FUR-CH-10000454","order_id":"CA-2016-152156"}',
                                '{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156"}',
                                '{"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688"}',
                            ]
                        },
                        "2024-03-14 06:53:39",
                    ),
                ],
                "target_query_dq_output": [
                    (
                        "your_product_96bb003e-e1cf-11ee-9a59-ae03267c3340",
                        "your_product",
                        "dq_spark_local.customer_order",
                        "product_missing_count_threshold",
                        "product_id",
                        "source_f1",
                        "_target_dq",
                        {
                            "source_f1": [
                                '{"product_id":"FUR-TA-10000577","order_id":"US-2015-108966"}',
                                '{"product_id":"OFF-ST-10000760","order_id":"US-2015-108966"}',
                                '{"product_id":"FUR-CH-10000454","order_id":"CA-2016-152156"}',
                                '{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156"}',
                                '{"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688"}',
                            ]
                        },
                        "2024-03-14 06:53:39",
                    ),
                    (
                        "your_product_96bb003e-e1cf-11ee-9a59-ae03267c3340",
                        "your_product",
                        "dq_spark_local.customer_order",
                        "product_missing_count_threshold",
                        "product_id",
                        "target_f1",
                        "_target_dq",
                        {
                            "target_f1": [
                                '{"product_id":"FUR-TA-10000577","order_id":"US-2015-108966"}',
                                '{"product_id":"FUR-CH-10000454","order_id":"CA-2016-152156"}',
                                '{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156"}',
                                '{"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688"}',
                            ]
                        },
                        "2024-03-14 06:53:39",
                    ),
                ],
            },
            {
                "product_id": "product1",
                "table_name": "dq_spark_local.customer_order",
                "rule": "sales_greater_than_zero",
                "column_name": "sales",
                "rule_type": "row_dq",
                "source_expectations": "sales > 2",
                "source_dq_status": "fail",
                "source_dq_actual_result": None,
                "source_dq_row_count": "5",
                "target_expectations": None,
                "target_dq_status": None,
                "target_dq_actual_result": None,
                "target_dq_row_count": None,
            },
            "row_dq",
            None,
        ),
        (
            {
                "input_count": 100,
                "error_count": 10,
                "output_count": 90,
                "rules_execution_settings_config": {
                    "row_dq": False,
                    "source_agg_dq": False,
                    "source_query_dq": True,
                    "target_agg_dq": False,
                    "target_query_dq": False,
                },
                "agg_dq_detailed_stats_status": False,
                "source_agg_dq_status": "Skipped",
                "final_agg_dq_status": "Skipped",
                "query_dq_detailed_stats_status": True,
                "source_query_dq_status": "Passed",
                "final_query_dq_status": "Skipped",
                "row_dq_status": "Passed",
                "summarised_row_dq_res": [],
                "run_id": "product_1_01450932-d5c2-11ee-a9ca-88e9fe5a7109",
                "input_count": 5,
                "dq_expectations": {
                    "row_dq_rules": [],
                },
                "test_dq_detailed_stats_table": "test_dq_detailed_stats_table",
                "test_querydq_output_custom_table_name": "test_querydq_output_custom_table_name",
                "detailed_stats_table_writer_config": {
                    "mode": "overwrite",
                    "format": "delta",
                    "partitionBy": [],
                    "bucketBy": {},
                    "sortBy": [],
                    "options": {"mergeSchema": "true"},
                },
                "rowdq_detailed_stats": [],
                "source_agg_dq_detailed_stats": [],
                "target_agg_dq_detailed_stats": [],
                "source_query_dq_detailed_stats": [
                    (
                        "product_1_52fed65a-d670-11ee-8dfb-ae03267c3341",
                        "product_1",
                        "dq_spark_local.customer_order",
                        "query_dq",
                        "product_missing_count_threshold",
                        "product_id",
                        "((select count(*) from (select distinct product_id,order_id from order_source) a) - (select count(*) from (select distinct product_id,order_id from order_target) b) ) < 3",
                        "validity",
                        "row count threshold",
                        "pass",
                        1,
                        "<3",
                        5,
                        0,
                        5,
                        "2024-03-14 00:00:00",
                        "2024-03-14 00:10:00",
                    )
                ],
                "target_query_dq_detailed_stats": [],
                "source_query_dq_output": [
                    (
                        "your_product_96bb003e-e1cf-11ee-9a59-ae03267c3340",
                        "your_product",
                        "dq_spark_local.customer_order",
                        "product_missing_count_threshold",
                        "product_id",
                        "source_f1",
                        "_source_dq",
                        {
                            "source_f1": [
                                '{"product_id":"FUR-TA-10000577","order_id":"US-2015-108966"}',
                                '{"product_id":"OFF-ST-10000760","order_id":"US-2015-108966"}',
                                '{"product_id":"FUR-CH-10000454","order_id":"CA-2016-152156"}',
                                '{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156"}',
                                '{"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688"}',
                            ]
                        },
                        "2024-03-14 06:53:39",
                    ),
                    (
                        "your_product_96bb003e-e1cf-11ee-9a59-ae03267c3340",
                        "your_product",
                        "dq_spark_local.customer_order",
                        "product_missing_count_threshold",
                        "product_id",
                        "target_f1",
                        "_source_dq",
                        {
                            "target_f1": [
                                '{"product_id":"FUR-TA-10000577","order_id":"US-2015-108966"}',
                                '{"product_id":"FUR-CH-10000454","order_id":"CA-2016-152156"}',
                                '{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156"}',
                                '{"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688"}',
                            ]
                        },
                        "2024-03-14 06:53:39",
                    ),
                ],
                "target_query_dq_output": [],
            },
            {
                "product_id": "product_1",
                "table_name": "dq_spark_local.customer_order",
                "rule": "product_missing_count_threshold",
                "column_name": "product_id",
                "rule_type": "query_dq",
                "source_expectations": "((select count(*) from (select distinct product_id,order_id from order_source) a) - (select count(*) from (select distinct product_id,order_id from order_target) b) ) < 3",
                "source_dq_status": "pass",
                "source_dq_actual_result": "1",
                "source_dq_row_count": "5",
                "target_expectations": None,
                "target_dq_status": None,
                "target_dq_actual_result": None,
                "target_dq_row_count": None,
            },
            "query_dq",
            None,
        ),
        (
            {
                "input_count": 100,
                "error_count": 10,
                "output_count": 90,
                "rules_execution_settings_config": {
                    "row_dq": True,
                    "source_agg_dq": True,
                    "source_query_dq": True,
                    "target_agg_dq": True,
                    "target_query_dq": True,
                },
                "agg_dq_detailed_stats_status": False,
                "source_agg_dq_status": "Passed",
                "final_agg_dq_status": "Passed",
                "query_dq_detailed_stats_status": True,
                "source_query_dq_status": "Passed",
                "final_query_dq_status": "Passed",
                "row_dq_status": "Passed",
                "summarised_row_dq_res": [
                    {
                        "rule_type": "row_dq",
                        "rule": "sales_greater_than_zero",
                        "description": "sales value should be greater than zero",
                        "failed_row_count": 1,
                        "tag": "validity",
                        "action_if_failed": "drop",
                    }
                ],
                "run_id": "product_1_01450932-d5c2-11ee-a9ca-88e9fe5a7109",
                "input_count": 5,
                "dq_expectations": {
                    "row_dq_rules": [
                        {
                            "product_id": "your_product",
                            "table_name": "dq_spark_local.customer_order",
                            "rule_type": "row_dq",
                            "rule": "sales_greater_than_zero",
                            "column_name": "sales",
                            "expectation": "sales > 2",
                            "action_if_failed": "drop",
                            "enable_for_source_dq_validation": False,
                            "enable_for_target_dq_validation": True,
                            "tag": "accuracy",
                            "description": "sales value should be greater than zero",
                            "enable_error_drop_alert": False,
                            "error_drop_threshold": 0,
                        }
                    ],
                },
                "test_dq_detailed_stats_table": "test_dq_detailed_stats_table",
                "test_querydq_output_custom_table_name": "test_querydq_output_custom_table_name",
                "detailed_stats_table_writer_config": {
                    "mode": "overwrite",
                    "format": "delta",
                    "partitionBy": [],
                    "bucketBy": {},
                    "sortBy": [],
                    "options": {"mergeSchema": "true"},
                },
                "rowdq_detailed_stats": [
                    (
                        "product_1_01450932-d5c2-11ee-a9ca-88e9fe5a7109",
                        "product_1",
                        "dq_spark_local.customer_order",
                        "row_dq",
                        "sales_greater_than_zero",
                        "Sales",
                        "sales > 2",
                        "accuracy",
                        "sales value should be greater than zero",
                        "fail",
                        None,
                        None,
                        None,
                        4,
                        "2024-03-14 00:00:00",
                        "2024-03-14 00:10:00",
                    ),
                ],
                "source_agg_dq_detailed_stats": [
                    (
                        "product_1_01450932-d5c2-11ee-a9ca-88e9fe5a7109",
                        "product_1",
                        "dq_spark_local.customer_order",
                        "agg_dq",
                        "sum_of_sales",
                        "sales",
                        "sum(sales)>10000",
                        "validity",
                        "regex format validation for quantity",
                        "fail",
                        [1988],
                        [">10000"],
                        5,
                        "2024-03-14 00:00:00",
                        "2024-03-14 00:10:00",
                    ),
                ],
                "target_agg_dq_detailed_stats": [
                    (
                        "product_1_01450932-d5c2-11ee-a9ca-88e9fe5a7109",
                        "product_1",
                        "dq_spark_local.customer_order",
                        "agg_dq",
                        "sum_of_sales",
                        "sales",
                        "sum(sales)>10000",
                        "validity",
                        "regex format validation for quantity",
                        "fail",
                        [1030],
                        [">10000"],
                        4,
                        "2024-03-14 01:00:00",
                        "2024-03-14 01:10:00",
                    ),
                ],
                "source_query_dq_detailed_stats": [
                    (
                        "product_1_52fed65a-d670-11ee-8dfb-ae03267c3341",
                        "product_1",
                        "dq_spark_local.customer_order",
                        "query_dq",
                        "product_missing_count_threshold",
                        "product_id",
                        "((select count(*) from (select distinct product_id,order_id from order_source) a) - (select count(*) from (select distinct product_id,order_id from order_target) b) ) < 3",
                        "validity",
                        "row count threshold",
                        "pass",
                        1,
                        "<3",
                        5,
                        0,
                        5,
                        "2024-03-14 02:00:00",
                        "2024-03-14 02:10:00",
                    )
                ],
                "target_query_dq_detailed_stats": [
                    (
                        "product_1_52fed65a-d670-11ee-8dfb-ae03267c3341",
                        "product_1",
                        "dq_spark_local.customer_order",
                        "query_dq",
                        "product_missing_count_threshold",
                        "product_id",
                        "((select count(*) from (select distinct product_id,order_id from order_source) a) - (select count(*) from (select distinct product_id,order_id from order_target) b) ) < 3",
                        "validity",
                        "row count threshold",
                        "pass",
                        1,
                        "<3",
                        4,
                        0,
                        4,
                        "2024-03-14 03:00:00",
                        "2024-03-14 03:10:00",
                    )
                ],
                "source_query_dq_output": [
                    (
                        "your_product_96bb003e-e1cf-11ee-9a59-ae03267c3340",
                        "your_product",
                        "dq_spark_local.customer_order",
                        "product_missing_count_threshold",
                        "product_id",
                        "source_f1",
                        "_source_dq",
                        {
                            "source_f1": [
                                '{"product_id":"FUR-TA-10000577","order_id":"US-2015-108966"}',
                                '{"product_id":"OFF-ST-10000760","order_id":"US-2015-108966"}',
                                '{"product_id":"FUR-CH-10000454","order_id":"CA-2016-152156"}',
                                '{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156"}',
                                '{"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688"}',
                            ]
                        },
                        "2024-03-14 06:53:39",
                    ),
                    (
                        "your_product_96bb003e-e1cf-11ee-9a59-ae03267c3340",
                        "your_product",
                        "dq_spark_local.customer_order",
                        "product_missing_count_threshold",
                        "product_id",
                        "target_f1",
                        "_source_dq",
                        {
                            "target_f1": [
                                '{"product_id":"FUR-TA-10000577","order_id":"US-2015-108966"}',
                                '{"product_id":"FUR-CH-10000454","order_id":"CA-2016-152156"}',
                                '{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156"}',
                                '{"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688"}',
                            ]
                        },
                        "2024-03-14 06:53:39",
                    ),
                ],
                "target_query_dq_output": [
                    (
                        "your_product_96bb003e-e1cf-11ee-9a59-ae03267c3340",
                        "your_product",
                        "dq_spark_local.customer_order",
                        "product_missing_count_threshold",
                        "product_id",
                        "source_f1",
                        "_target_dq",
                        {
                            "source_f1": [
                                '{"product_id":"FUR-TA-10000577","order_id":"US-2015-108966"}',
                                '{"product_id":"OFF-ST-10000760","order_id":"US-2015-108966"}',
                                '{"product_id":"FUR-CH-10000454","order_id":"CA-2016-152156"}',
                                '{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156"}',
                                '{"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688"}',
                            ]
                        },
                        "2024-03-14 06:53:39",
                    ),
                    (
                        "your_product_96bb003e-e1cf-11ee-9a59-ae03267c3340",
                        "your_product",
                        "dq_spark_local.customer_order",
                        "product_missing_count_threshold",
                        "product_id",
                        "target_f1",
                        "_target_dq",
                        {
                            "target_f1": [
                                '{"product_id":"FUR-TA-10000577","order_id":"US-2015-108966"}',
                                '{"product_id":"FUR-CH-10000454","order_id":"CA-2016-152156"}',
                                '{"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156"}',
                                '{"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688"}',
                            ]
                        },
                        "2024-03-14 06:53:39",
                    ),
                ],
            },
            {
                "product_id": "product_1",
                "table_name": "dq_spark_local.customer_order",
                "rule": "product_missing_count_threshold",
                "column_name": "product_id",
                "rule_type": "query_dq",
                "source_expectations": "((select count(*) from (select distinct product_id,order_id from order_source) a) - (select count(*) from (select distinct product_id,order_id from order_target) b) ) < 3",
                "source_dq_status": "pass",
                "source_dq_actual_result": 5,
                "source_dq_row_count": 5,
                "target_expectations": "((select count(*) from (select distinct product_id,order_id from order_source) a) - (select count(*) from (select distinct product_id,order_id from order_target) b) ) < 3",
                "target_dq_status": "pass",
                "target_dq_actual_result": 5,
                "target_dq_row_count": 4,
            },
            "query_dq",
            {
                "mode": "append",
                "format": "bigquery",
                "partitionBy": [],
                "bucketBy": {},
                "sortBy": [],
                "options": {"mergeSchema": "true"},
            },
        ),
    ],
)
def test_write_detailed_stats(
    input_record,
    expected_result,
    dq_check,
    writer_config,
) -> None:
    """
    This functions writes the detailed stats for all rule type into the detailed stats table

    Args:
        config: Provide the config to write the dataframe into the table

    Returns:
        None:


    """
    _mock_context = Mock(spec=SparkExpectationsContext)
    _mock_context.get_dq_rules_params = {"env": "test_env"}

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
        "get_source_agg_dq_status",
        input_record.get("source_agg_dq_status"),
    )
    setattr(
        _mock_context,
        "get_final_agg_dq_status",
        input_record.get("final_agg_dq_status"),
    )
    setattr(
        _mock_context,
        "get_query_dq_detailed_stats_status",
        input_record.get("query_dq_detailed_stats_status"),
    )
    setattr(
        _mock_context,
        "get_source_query_dq_status",
        input_record.get("source_query_dq_status"),
    )
    setattr(
        _mock_context,
        "get_final_query_dq_status",
        input_record.get("final_query_dq_status"),
    )
    setattr(_mock_context, "get_row_dq_status", input_record.get("row_dq_status"))
    setattr(
        _mock_context,
        "get_summarized_row_dq_res",
        input_record.get("summarised_row_dq_res"),
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
    setattr(_mock_context, "get_run_id", input_record.get("run_id"))
    setattr(_mock_context, "product_id", "product_1")
    setattr(_mock_context, "get_table_name", "dq_spark_local.customer_order")
    setattr(_mock_context, "get_input_count", input_record.get("input_count"))
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
        setattr(_mock_context, "get_detailed_stats_table_writer_config", writer_config)

    _mock_context.spark = spark
    _mock_context.product_id = "product1"

    _fixture_writer = SparkExpectationsWriter(_mock_context)

    if writer_config and writer_config["format"] == "bigquery":
        patcher = patch("pyspark.sql.DataFrameWriter.save")
        mock_bq = patcher.start()
        setattr(_mock_context, "get_se_streaming_stats_dict", {"se.streaming.enable": False})
        _fixture_writer.write_detailed_stats()
        mock_bq.assert_called_with()

    else:
        setattr(_mock_context, "get_se_streaming_stats_dict", {"se.streaming.enable": True})
        _fixture_writer.write_detailed_stats()

        stats_table = spark.sql(f"select * from test_dq_detailed_stats_table where rule_type = '{dq_check}'")
        assert stats_table.count() == 1
        row = stats_table.first()
        assert row.product_id == expected_result.get("product_id")
        assert row.table_name == "dq_spark_local.customer_order"
        assert row.rule_type == expected_result.get("rule_type")
        assert row.rule == expected_result.get("rule")
        assert row.source_expectations == expected_result.get("source_expectations")
        assert row.source_dq_status == expected_result.get("source_dq_status")
        assert row.source_dq_actual_outcome == expected_result.get("source_dq_actual_result")
        assert row.source_dq_row_count == expected_result.get("source_dq_row_count")
        assert row.target_expectations == expected_result.get("target_expectations")
        assert row.target_dq_status == expected_result.get("target_dq_status")
        assert row.target_dq_actual_outcome == expected_result.get("target_dq_actual_result")
        assert row.target_dq_row_count == expected_result.get("target_dq_row_count")


def test_write_detailed_stats_exception() -> None:
    """
    This functions writes the detailed stats for all rule type into the detailed stats table

    Args:
        config: Provide the config to write the dataframe into the table

    Returns:
        None:


    """
    _mock_context = Mock(spec=SparkExpectationsContext)
    _mock_context.get_dq_rules_params = {"env": "test_env"}
    setattr(
        _mock_context,
        "get_rules_execution_settings_config",
        {
            "row_dq": True,
            "source_agg_dq": True,
            "source_query_dq": True,
            "target_agg_dq": True,
            "target_query_dq": True,
        },
    )
    setattr(_mock_context, "get_agg_dq_detailed_stats_status", True)
    setattr(_mock_context, "get_source_agg_dq_status", "Passed")

    _mock_context.spark = spark
    _mock_context.product_id = "product1"

    _fixture_writer = SparkExpectationsWriter(_mock_context)

    with pytest.raises(
        SparkExpectationsMiscException,
        match=r"error occurred while saving the data into the stats table .*",
    ):
        _fixture_writer.write_detailed_stats()


# @pytest.mark.parametrize("table_name, rule_type", [("test_error_table", "row_dq")])
# @patch(
#     "spark_expectations.sinks.utils.writer.SparkExpectationsWriter.save_df_as_table",
#     autospec=True,
#     spec_set=True,
# )
# def test_write_error_records_final_dependent(
#     save_df_as_table,
#     table_name,
#     rule_type,
#     _fixture_dq_dataset,
#     _fixture_expected_error_dataset,
#     _fixture_writer,
# ):
#     # invoke the write_error_records_final method with the test fixtures as arguments
#     result, _df = _fixture_writer.write_error_records_final(_fixture_dq_dataset, table_name, rule_type)

#     # assert that the returned value is the expected number of rows in the error table
#     assert result == 3

#     # Assert
#     save_df_args = save_df_as_table.call_args
#     assert save_df_args[0][0] == _fixture_writer
#     assert (
#         save_df_args[0][1].orderBy("id").collect()
#         == _fixture_expected_error_dataset.withColumn("meta_dq_run_date", lit("2022-12-27 10:39:44"))
#         .orderBy("id")
#         .collect()
#     )
#     assert save_df_args[0][2] == table_name
#     save_df_as_table.assert_called_once_with(
#         _fixture_writer,
#         save_df_args[0][1],
#         table_name,
#         _fixture_writer._context.get_target_and_error_table_writer_config,
#     )


# @pytest.mark.parametrize("table_name, rule_type", [("test_error_table", "row_dq")])
# def test_write_error_records_final(
#     table_name,
#     rule_type,
#     _fixture_dq_dataset,
#     _fixture_expected_dq_dataset,
#     _fixture_writer,
# ):
#     config = WrappedDataFrameWriter().mode("overwrite").format("delta").build()

#     setattr(_fixture_writer._context, "get_target_and_error_table_writer_config", config)
#     # invoke the write_error_records_final method with the test fixtures as arguments
#     result, _df = _fixture_writer.write_error_records_final(_fixture_dq_dataset, table_name, rule_type)
#     # error_df = spark.table("test_error_table")

#     # assert that the returned value is the expected number of rows in the error table
#     _df = _df.withColumn("meta_dq_run_date", to_timestamp(lit("2022-12-27 10:39:44")))
#     assert result == 3
#     assert _df.count() == 4
#     assert _df.orderBy("id").collect() == _fixture_expected_dq_dataset.orderBy("id").collect()


# @pytest.mark.parametrize("table_name, rule_type", [("test_error_table", "row_dq")])
# @patch(
#     "spark_expectations.sinks.utils.writer.SparkExpectationsWriter.save_df_as_table",
#     autospec=True,
#     spec_set=True,
# )
# def test_write_error_records_final_dependent(
#     save_df_as_table,
#     table_name,
#     rule_type,
#     _fixture_dq_dataset,
#     _fixture_expected_error_dataset,
#     _fixture_writer,
# ):
#     # invoke the write_error_records_final method with the test fixtures as arguments
#     result, _df = _fixture_writer.write_error_records_final(_fixture_dq_dataset, table_name, rule_type)

#     # assert that the returned value is the expected number of rows in the error table
#     assert result == 3

#     # Assert
#     save_df_args = save_df_as_table.call_args
#     assert save_df_args[0][0] == _fixture_writer
#     assert (
#         save_df_args[0][1].orderBy("id").collect()
#         == _fixture_expected_error_dataset.withColumn("meta_dq_run_date", lit("2022-12-27 10:39:44"))
#         .orderBy("id")
#         .collect()
#     )
#     assert save_df_args[0][2] == table_name
#     save_df_as_table.assert_called_once_with(
#         _fixture_writer,
#         save_df_args[0][1],
#         table_name,
#         _fixture_writer._context.get_target_and_error_table_writer_config,
#     )


# @pytest.mark.parametrize(
#     "test_data, expected_result",
#     [
#         (
#             [
#                 {
#                     "meta_row_dq_results": [
#                         {
#                             "rule_type": "row_dq",
#                             "rule": "rule1",
#                             "description": "col1 should not be null",
#                             "column_name":"col1",
#                             "tag": "validity",
#                             "action_if_failed": "ignore",
#                         },
#                         {
#                             "rule_type": "row_dq",
#                             "rule": "rule2",
#                             "description": "col2 should start with A",
#                             "column_name": "col2",
#                             "tag": "validity",
#                             "action_if_failed": "ignore",
#                         },
#                     ],
#                     "meta_dq_run_id": "run_id",
#                     "meta_dq_run_date": "2022-12-27 10:39:44",
#                 },
#                 {
#                     "meta_row_dq_results": [
#                         {
#                             "rule_type": "row_dq",
#                             "rule": "rule1",
#                             "description": "col1 should not be null",
#                             "column_name": "col1",
#                             "tag": "validity",
#                             "action_if_failed": "ignore",
#                         }
#                     ],
#                     "meta_dq_run_id": "run_id",
#                     "meta_dq_run_date": "2022-12-27 10:39:44",
#                 },
#                 {
#                     "meta_row_dq_results": [
#                         {
#                             "rule_type": "row_dq",
#                             "rule": "rule2",
#                             "description": "col2 should start with A",
#                             "column_name": "col2",
#                             "tag": "validity",
#                             "action_if_failed": "ignore",
#                         }
#                     ],
#                     "meta_dq_run_id": "run_id",
#                     "meta_dq_run_date": "2022-12-27 10:39:44",
#                 },
#             ],
#             [
#                 {
#                     "rule_type": "row_dq",
#                     "rule": "rule1",
#                     "description": "col1 should not be null",
#                     "column_name": "col1",
#                     "tag": "validity",
#                     "action_if_failed": "ignore",
#                     "failed_row_count": 2,
#                 },
#                 {
#                     "rule_type": "row_dq",
#                     "rule": "rule2",
#                     "description": "col2 should start with A",
#                     "column_name": "col2",
#                     "tag": "validity",
#                     "action_if_failed": "ignore",
#                     "failed_row_count": 2,
#                 },
#             ],
#         ),
#         (
#             [
#                 {
#                     "meta_row_dq_results": [
#                         {
#                             "rule_type": "row_dq",
#                             "rule": "rule1",
#                             "description": "col1 should not be null",
#                             "column_name":"col1",
#                             "tag": "validity",
#                             "action_if_failed": "ignore",
#                         }
#                     ],
#                     "meta_dq_run_id": "run_id",
#                     "meta_dq_run_date": "2022-12-27 10:39:44",
#                 },
#                 {
#                     "meta_row_dq_results": [
#                         {
#                             "rule_type": "row_dq",
#                             "rule": "rule1",
#                             "description": "col1 should not be null",
#                             "column_name": "col1",
#                             "tag": "validity",
#                             "action_if_failed": "ignore",
#                         }
#                     ],
#                     "meta_dq_run_id": "run_id",
#                     "meta_dq_run_date": "2022-12-27 10:39:44",
#                 },
#             ],
#             [
#                 {
#                     "rule_type": "row_dq",
#                     "rule": "rule1",
#                     "description": "col1 should not be null",
#                     "column_name": "col1",
#                     "tag": "validity",
#                     "action_if_failed": "ignore",
#                     "failed_row_count": 2,
#                 },
#                 {
#                     "rule_type": "row_dq",
#                     "rule": "rule2",
#                     "description": "col2 should start with A",
#                     "column_name": "col2",
#                     "tag": "validity",
#                     "action_if_failed": "ignore",
#                     "failed_row_count": 0,
#                 },
#             ],
#         ),
#     ],
# )
# def test_generate_summarized_row_dq_res(test_data, expected_result, _fixture_context):
#     writer = SparkExpectationsWriter(_fixture_context)
#     # Create test DataFrame
#     test_df = spark.createDataFrame(test_data)
#     writer.generate_summarized_row_dq_res(test_df, "row_dq")
#     assert writer._context.set_summarized_row_dq_res.call_count == 1
#     writer._context.set_summarized_row_dq_res.assert_called_once_with(expected_result)


@pytest.mark.parametrize(
    "dq_rules, summarized_row_dq, expected_result",
    [
        (
            {
                "row_dq_rules": [
                    {
                        "rule": "rule1",
                        "enable_error_drop_alert": True,
                        "action_if_failed": "drop",
                        "description": "description1",
                        "rule_type": "row_dq",
                        "column_name": "col1",
                        "error_drop_threshold": "10",
                    },
                    {
                        "rule": "rule2",
                        "enable_error_drop_alert": True,
                        "action_if_failed": "drop",
                        "description": "description1",
                        "column_name": "col1",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "10",
                    },
                ]
            },
            [
                {"rule": "rule1", "failed_row_count": 10},
            ],
            [
                {
                    "rule_name": "rule1",
                    "action_if_failed": "drop",
                    "description": "description1",
                    "column_name": "col1",
                    "rule_type": "row_dq",
                    "error_drop_threshold": "10",
                    "error_drop_percentage": "10.0",
                }
            ],
        ),
        (
            {
                "row_dq_rules": [
                    {
                        "rule": "rule3",
                        "enable_error_drop_alert": True,
                        "action_if_failed": "ignore",
                        "column_name": "col1",
                        "description": "description3",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "10",
                    }
                ]
            },
            [{"rule": "rule3", "failed_row_count": 10}],
            [
                {
                    "rule_name": "rule3",
                    "action_if_failed": "ignore",
                    "description": "description3",
                    "column_name": "col1",
                    "rule_type": "row_dq",
                    "error_drop_threshold": "10",
                    "error_drop_percentage": "10.0",
                }
            ],
        ),
        (
            {
                "row_dq_rules": [
                    {
                        "rule": "rule4",
                        "enable_error_drop_alert": True,
                        "action_if_failed": "fail",
                        "column_name": "col1",
                        "description": "description4",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "10",
                    }
                ]
            },
            [{"rule": "rule4", "failed_row_count": 10}],
            [
                {
                    "rule_name": "rule4",
                    "action_if_failed": "fail",
                    "description": "description4",
                    "column_name": "col1",
                    "rule_type": "row_dq",
                    "error_drop_threshold": "10",
                    "error_drop_percentage": "10.0",
                }
            ],
        ),
        (
            {
                "row_dq_rules": [
                    {
                        "rule": "rule1",
                        "enable_error_drop_alert": True,
                        "action_if_failed": "drop",
                        "column_name": "col1",
                        "description": "description1",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "10",
                    },
                    {
                        "rule": "rule2",
                        "enable_error_drop_alert": True,
                        "action_if_failed": "drop",
                        "column_name": "col1",
                        "description": "description2",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "10",
                    },
                ]
            },
            [
                {"rule": "rule1", "failed_row_count": 10},
                {"rule": "rule2", "failed_row_count": 20},
            ],
            [
                {
                    "rule_name": "rule1",
                    "action_if_failed": "drop",
                    "description": "description1",
                    "rule_type": "row_dq",
                    "column_name": "col1",
                    "error_drop_threshold": "10",
                    "error_drop_percentage": "10.0",
                },
                {
                    "rule_name": "rule2",
                    "action_if_failed": "drop",
                    "description": "description2",
                    "column_name": "col1",
                    "rule_type": "row_dq",
                    "error_drop_threshold": "10",
                    "error_drop_percentage": "20.0",
                },
            ],
        ),
        (
            {
                "row_dq_rules": [
                    {
                        "rule": "rule1",
                        "enable_error_drop_alert": True,
                        "action_if_failed": "drop",
                        "description": "description1",
                        "column_name": "col1",
                        "rule_type": "row_dq",
                        "error_drop_threshold": "10",
                    },
                ]
            },
            None,
            None,
        ),
    ],
)
def test_generate_rules_exceeds_threshold(
    dq_rules,
    summarized_row_dq,
    expected_result,
):
    _context = SparkExpectationsContext("product1", spark)
    _writer = SparkExpectationsWriter(_context)
    _context.set_summarized_row_dq_res(summarized_row_dq)
    _context.set_input_count(100)

    # Check the results
    _writer.generate_rules_exceeds_threshold(dq_rules)
    assert _context.get_rules_exceeds_threshold == expected_result


# @pytest.mark.parametrize(
#     "test_data",
#     [
#         (
#             [
#                 {"row_dq_results": [{"rule": "rule1"}, {"rule": "rule2"}]},
#                 {"row_dq_results": [{"rule": "rule1"}, {"rule": "rule2"}]},
#             ]
#         ),
#     ],
# )
# def test_generate_summarized_row_dq_res_exception(test_data, _fixture_writer):
#     # Create test DataFrame
#     test_df = spark.createDataFrame(test_data)

#     with pytest.raises(
#         SparkExpectationsMiscException,
#         match=r"error occurred created summarized row dq statistics .*",
#     ):
#         # Call the function under test
#         _fixture_writer.generate_summarized_row_dq_res(test_df, "row_dq")


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


# @pytest.mark.parametrize(
#     "dbr_version,env,expected_options",
#     [
#         (
#             13.3,
#             "prod",
#             {
#                 "kafka.bootstrap.servers": "test-server-url",
#                 "kafka.security.protocol": "SASL_SSL",
#                 "kafka.sasl.mechanism": "OAUTHBEARER",
#                 "kafka.sasl.jaas.config": """kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="test-client-id" clientSecret="test-token";""",
#                 "kafka.sasl.oauthbearer.token.endpoint.url": "test-endpoint",
#                 "kafka.sasl.login.callback.handler.class": "kafkashaded.org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler",
#                 "topic": "test-topic",
#             },
#         ),
#         (
#             12,
#             "local",
#             {
#                 "kafka.bootstrap.servers": "localhost:9092",
#                 "topic": "dq-sparkexpectations-stats",
#                 "failOnDataLoss": "true",
#             },
#         ),
#         (
#             12,
#             "prod",
#             {
#                 "kafka.bootstrap.servers": "test-server-url",
#                 "kafka.security.protocol": "SASL_SSL",
#                 "kafka.sasl.mechanism": "OAUTHBEARER",
#                 "kafka.sasl.jaas.config": """kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.client.id='test-client-id'  oauth.client.secret='test-token' oauth.token.endpoint.uri='test-endpoint'; """,
#                 "kafka.sasl.login.callback.handler.class": "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler",
#                 "topic": "test-topic",
#             },
#         ),
#     ],
# )
# def test_get_kafka_write_options(dbr_version, env, expected_options):
#     """Test the Kafka write options generation for different environments and configurations"""
#     context = SparkExpectationsContext("product1", spark)
#     context._env = env

#     # Mock runtime environment check and secrets handler
#     with (
#         patch(
#             "spark_expectations.core.context.SparkExpectationsContext.get_dbr_version",
#             new_callable=Mock(return_value=dbr_version),
#         ),
#         patch(
#             "spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret"
#         ) as mock_get_secret,
#         patch(
#             "spark_expectations.core.context.SparkExpectationsContext.get_server_url_key",
#             new_callable=Mock(return_value="test-server-url"),
#         ),
#         patch(
#             "spark_expectations.core.context.SparkExpectationsContext.get_client_id",
#             new_callable=Mock(return_value="test-client-id"),
#         ),
#         patch(
#             "spark_expectations.core.context.SparkExpectationsContext.get_token",
#             new_callable=Mock(return_value="test-token"),
#         ),
#         patch(
#             "spark_expectations.core.context.SparkExpectationsContext.get_token_endpoint_url",
#             new_callable=Mock(return_value="test-endpoint"),
#         ),
#         patch(
#             "spark_expectations.core.context.SparkExpectationsContext.get_topic_name",
#             new_callable=Mock(return_value="test-topic"),
#         ),
#     ):
#         # Configure mock to return the value passed to get_secret
#         mock_get_secret.side_effect = lambda x: x

#         writer = SparkExpectationsWriter(context)
#         actual_options = writer.get_kafka_write_options(
#             {}
#         )  # Empty dict since we mock everything
#         assert actual_options == expected_options
