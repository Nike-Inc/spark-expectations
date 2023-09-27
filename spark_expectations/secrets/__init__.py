from __future__ import annotations

import functools
from dataclasses import dataclass
from typing import Optional, Dict
import pluggy
from pyspark.sql.session import SparkSession
from spark_expectations import _log
from spark_expectations.config.user_config import Constants as UserConfig


SPARK_EXPECTATIONS_SECRETS_BACKEND = "spark_expectations_secrets_backend"

spark_expectations_secrets_plugin_spec = pluggy.HookspecMarker(
    SPARK_EXPECTATIONS_SECRETS_BACKEND
)


class SparkExpectationsSecretPluginSpec:
    @staticmethod
    @spark_expectations_secrets_plugin_spec(firstresult=True)
    def get_secret_value(
        secret_key_path: str, secret_dict: Dict[str, str]
    ) -> Optional["str"]:
        """Custom execute method that is able to be plugged in."""


@functools.lru_cache
def get_spark_expectations_tasks_hook() -> SparkExpectationsSecretPluginSpec:
    pm = pluggy.PluginManager(SPARK_EXPECTATIONS_SECRETS_BACKEND)
    pm.add_hookspecs(SparkExpectationsSecretPluginSpec)
    pm.load_setuptools_entrypoints(SPARK_EXPECTATIONS_SECRETS_BACKEND)
    pm.register(CerberusSparkExpectationsSecretPluginImpl())
    pm.register(DatabricksSecretsSparkExpectationsSecretPluginImpl())
    for name, plugin_instance in pm.list_name_plugin():
        _log.info(
            "Loaded plugin with name: %s and class: %s",
            name,
            plugin_instance.__class__.__name__,
        )
    return pm.hook


spark_expectations_secrets_backend_plugin_impl = pluggy.HookimplMarker(
    SPARK_EXPECTATIONS_SECRETS_BACKEND
)


class CerberusSparkExpectationsSecretPluginImpl(SparkExpectationsSecretPluginSpec):
    @staticmethod
    @spark_expectations_secrets_backend_plugin_impl
    def get_secret_value(
        secret_key_path: str, secret_dict: Dict[str, str]
    ) -> Optional[str]:
        """
        This function implemented to get secret value from cerberus
        Args:
            secret_key_path: str which accepts url with secret key
            secret_dict: dict which contains params for fetch secrets
        Returns:
            str | none : returns secret value in string or none
        """

        from cerberus.client import CerberusClient

        if secret_dict[UserConfig.secret_type].lower() == "cerberus":
            _client = CerberusClient(secret_dict[UserConfig.cbs_url])
            data = _client.get_secrets_data(secret_key_path)
            return data

        return None


class DatabricksSecretsSparkExpectationsSecretPluginImpl(
    SparkExpectationsSecretPluginSpec
):
    @staticmethod
    @spark_expectations_secrets_backend_plugin_impl
    def get_secret_value(
        secret_key_path: str, secret_dict: Dict[str, str]
    ) -> Optional[str]:
        """
         # pragma: no cover
        This function implemented to get secret value from databricks scope
        Args:
            secret_key_path: str which accepts url with secret key
            secret_dict: dict which contains params for fetch secrets
        Returns:
            str | none : returns secret value in string or none
        """
        if secret_dict[UserConfig.secret_type].lower() == "databricks":
            try:
                from pyspark.dbutils import DBUtils

                spark = SparkSession.getActiveSession()  # pragma: no cover
                dbutils = DBUtils(spark)  # pragma: no cover
            except ImportError:
                raise ImportError(
                    "You must install databricks to use the databricks secrets backend, "
                    "please try pip install databricks"
                )

            data = dbutils.secrets.get(
                scope=secret_dict[UserConfig.dbx_secret_scope], key=secret_key_path
            )  # pragma: no cover
            return data  # pragma: no cover
        return None  # pragma: no cover


@dataclass
class SparkExpectationsSecretsBackend:
    secret_dict: Dict[str, str]

    def get_secret(self, secret_key: Optional[str]) -> str | None:
        """
        This function implemented to get secret value
        Args:
            secret_key: Optional[str] which accepts url with secret key
        Returns:
            str | none : returns secret value in string or none
        """
        return get_spark_expectations_tasks_hook().get_secret_value(
            secret_key_path=secret_key, secret_dict=self.secret_dict
        )
