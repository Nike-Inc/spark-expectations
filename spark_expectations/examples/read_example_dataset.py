import pkg_resources
from pyspark.sql import DataFrame
from spark_expectations.core import get_spark_session
from typing import Optional

spark = get_spark_session()


def spark_expectations_read_employee_dataset(prepend: Optional[str] = None) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "DROPMALFORMED")
        .csv(prepend_csv_path(csv_filename="employee.csv", prepend=prepend))
    )


def spark_expectations_read_order_dataset(prepend: Optional[str] = None) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "DROPMALFORMED")
        .csv(prepend_csv_path(csv_filename="order.csv", prepend=prepend))
    )


def spark_expectations_read_product_dataset(prepend: Optional[str] = None) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "DROPMALFORMED")
        .csv(prepend_csv_path(csv_filename="product.csv", prepend=prepend))
    )


def spark_expectations_read_customer_dataset(prepend: Optional[str] = None) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "DROPMALFORMED")
        .csv(prepend_csv_path(csv_filename="customer.csv", prepend=prepend))
    )


def prepend_csv_path(csv_filename: str, prepend: Optional[str] = None) -> str:
    """
    Prepend a string to the given path if prepend is not None.
    Args:
        csv_filename (str): The CSV file name.
        prepend (str): The string to prepend.
    Returns:
        str: The modified path.
    Example:
        >>> prepend_csv_path("employee.csv", "file:")
        'file:/Users/developer/spark-expectations/spark_expectations/examples/resources/employee.csv'
    """
    path = pkg_resources.resource_filename("spark_expectations", f"examples/resources/{csv_filename}")
    return path if prepend is None else f"{prepend}{path}"
