import os
from unittest import mock
from unittest.mock import patch
from pyspark.sql.session import SparkSession
from spark_expectations.core import get_spark_session
from spark_expectations.core.__init__ import current_dir


@patch("spark_expectations.core.__init__.current_dir", autospec=True, spec_set=True)
def test_get_spark_session(_mock_os):
    spark = get_spark_session()
    assert isinstance(spark, SparkSession)

    # Add additional assertions as needed to test the SparkSession configuration
    assert "io.delta.sql.DeltaSparkSessionExtension" in spark.sparkContext.getConf().get("spark.sql.extensions")
    assert "org.apache.spark.sql.delta.catalog.DeltaCatalog" in spark.sparkContext.getConf().get(
        "spark.sql.catalog.spark_catalog"
    )

    # Test that the warehouse and derby directories are properly configured
    assert "/tmp/hive/warehouse" in spark.sparkContext.getConf().get("spark.sql.warehouse.dir")
    assert "-Dderby.system.home=/tmp/derby" in spark.sparkContext.getConf().get("spark.driver.extraJavaOptions")

    assert (
        spark.conf.get("spark.jars") == f"{current_dir}/../../jars/spark-sql-kafka-0-10_2.12-3.0.0.jar,"
        f"{current_dir}/../../jars/kafka-clients-3.0.0.jar,"
        f"{current_dir}/../../jars/commons-pool2-2.8.0.jar,"
        f"{current_dir}/../../jars/spark-token-provider-kafka-0-10_2.12-3.0.0.jar"
    )

    # Add more assertions to test any other desired SparkSession configuration options


@mock.patch.dict(os.environ, {"UNIT_TESTING_ENV": "disable", "SPARKEXPECTATIONS_ENV": "disable"})
def test_get_spark_active_session():
    spark = SparkSession.builder.getOrCreate()

    # Now try to get the active session as we disabled unittest flags for this test
    active = get_spark_session()
    assert active == spark
