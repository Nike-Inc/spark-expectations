from typing import Dict, Union
from pyspark.sql import DataFrame
import pluggy

SPARK_EXPECTATIONS_WRITER_PLUGIN = "spark_expectations_writer_plugins"

writer_plugin_spec = pluggy.HookspecMarker(SPARK_EXPECTATIONS_WRITER_PLUGIN)
spark_expectations_writer_impl = pluggy.HookimplMarker(SPARK_EXPECTATIONS_WRITER_PLUGIN)


class SparkExpectationsSinkWriter:
    @writer_plugin_spec
    def writer(
        self, _write_args: Dict[Union[str], Union[str, bool, Dict[str, str], DataFrame]]
    ) -> None:
        """
        function consist signature to write data into kafka etc. which will be implemented in the child class
        Args:
            _write_args:

        Returns:

        """

        pass
