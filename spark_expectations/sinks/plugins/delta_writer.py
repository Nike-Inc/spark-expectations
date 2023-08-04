from typing import Dict, Union
from pyspark.sql import DataFrame
from spark_expectations import _log
from spark_expectations.sinks.plugins.base_writer import (
    SparkExpectationsSinkWriter,
    spark_expectations_writer_impl,
)
from spark_expectations.core import get_spark_session
from spark_expectations.core.exceptions import SparkExpectationsMiscException


class SparkExpectationsDeltaWritePluginImpl(SparkExpectationsSinkWriter):
    """
    function implements/supports data into the delta table
    """

    @spark_expectations_writer_impl
    def writer(
        self, _write_args: Dict[Union[str], Union[str, bool, Dict[str, str], DataFrame]]
    ) -> None:
        """
        Args:
            _write_args:

        Returns:
        """
        try:
            _log.info("started writing data into delta stats table")
            df: DataFrame = _write_args.get("stats_df")
            df.write.saveAsTable(
                name=f"{_write_args.get('table_name')}",
                **{"mode": "append", "format": "delta", "mergeSchema": "true"},
            )
            get_spark_session().sql(
                f"ALTER TABLE {_write_args.get('table_name')} "
                f"SET TBLPROPERTIES ('product_id' = '{_write_args.get('product_id')}')"
            )
            _log.info("ended writing data into delta stats table")
        except Exception as e:
            raise SparkExpectationsMiscException(
                f"error occurred while saving data into delta stats table {e}"
            )
