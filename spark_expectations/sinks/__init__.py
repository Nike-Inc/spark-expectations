import functools
import pluggy
from spark_expectations import _log
from spark_expectations.sinks.plugins.base_writer import (
    SparkExpectationsSinkWriter,
    SPARK_EXPECTATIONS_WRITER_PLUGIN,
)

from spark_expectations.sinks.plugins.delta_writer import (
    SparkExpectationsDeltaWritePluginImpl,
)
from spark_expectations.sinks.plugins.nsp_writer import (
    SparkExpectationsNspWritePluginImpl,
)


@functools.lru_cache
def get_sink_hook() -> pluggy.PluginManager:
    """
    function provides pluggy hook manager to write data into delta and nsp
    Returns:
        PluginManager: pluggy Manager object

    """
    pm = pluggy.PluginManager(SPARK_EXPECTATIONS_WRITER_PLUGIN)
    pm.add_hookspecs(SparkExpectationsSinkWriter)
    pm.register(
        SparkExpectationsDeltaWritePluginImpl(), "spark_expectations_delta_write"
    )
    pm.register(SparkExpectationsNspWritePluginImpl(), "spark_expectations_nsp_write")
    for name, plugin_instance in pm.list_name_plugin():
        _log.info(
            "Loaded plugin with name: %s and class: %s",
            name,
            plugin_instance.__class__.__name__,
        )
    return pm


_sink_hook = get_sink_hook().hook
