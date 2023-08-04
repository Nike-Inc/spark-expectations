import os
import pandas as pd
import numpy as np
import pkg_resources
from pyspark.sql import DataFrame
from spark_expectations.core import get_spark_session

spark = get_spark_session()


def spark_expectations_read_employee_dataset() -> DataFrame:
    _df: DataFrame = pd.read_csv(
        pkg_resources.resource_filename(
            "spark_expectations", "examples/resources/employee.csv"
        )
    ).replace({np.nan: None})

    return spark.createDataFrame(_df)


def spark_expectations_read_order_dataset() -> DataFrame:
    _df: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv("file://" + os.path.join(os.path.dirname(__file__), "resources/order.csv"))
    )
    return _df


def spark_expectations_read_product_dataset() -> DataFrame:
    _df: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(
            "file://" + os.path.join(os.path.dirname(__file__), "resources/product.csv")
        )
    )
    return _df


def spark_expectations_read_customer_dataset() -> DataFrame:
    _df: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(
            "file://"
            + os.path.join(os.path.dirname(__file__), "resources/customer.csv")
        )
    )
    return _df
