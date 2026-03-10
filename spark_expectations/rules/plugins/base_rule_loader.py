from __future__ import annotations

from typing import Dict, Optional

import pluggy

from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession

SPARK_EXPECTATIONS_RULE_LOADER_PLUGIN = "spark_expectations_rule_loader_plugins"

rule_loader_plugin_spec = pluggy.HookspecMarker(SPARK_EXPECTATIONS_RULE_LOADER_PLUGIN)
spark_expectations_rule_loader_impl = pluggy.HookimplMarker(SPARK_EXPECTATIONS_RULE_LOADER_PLUGIN)


class SparkExpectationsRuleLoader:
    """Base hook spec for rule loader plugins.

    Each plugin should check whether it can handle the given format/path
    and return a DataFrame with the standard rules schema, or None if
    it cannot handle the request.
    """

    @rule_loader_plugin_spec(firstresult=True)
    def load_rules(
        self,
        path: str,
        format: str,
        options: Dict[str, str],
        spark: Optional[SparkSession] = None,
    ) -> Optional[DataFrame]:
        """Load rules from *path* and return a Spark DataFrame.

        Args:
            path: File path readable by Python (local, DBFS fuse, mounted volume).
            format: Requested format (``yaml``, ``json``, or ``auto``).
            options: Extra loader-specific options forwarded by the caller.
            spark: Optional SparkSession; if ``None`` the active session is used.

        Returns:
            A Spark DataFrame with the standard rules schema, or ``None``
            when this plugin cannot handle the requested format.
        """
