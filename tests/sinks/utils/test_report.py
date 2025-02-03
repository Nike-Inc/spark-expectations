import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession

from spark_expectations.core.expectations import DataFrame
from spark_expectations.sinks.utils.report import SparkExpectationsReport
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.core import get_spark_session


def test_dq_obs_report_data_insert():
    spark = get_spark_session()
    _context = SparkExpectationsContext(product_id="product_1", spark=spark)
    report = SparkExpectationsReport(_context)
    print("report being called")
    dq_obs_rpt_gen_status_flag, df_1 = report.dq_obs_report_data_insert()
    assert type(dq_obs_rpt_gen_status_flag) is bool
    assert  type(df_1) is DataFrame