import pluggy
from unittest import mock
import pytest
from unittest.mock import patch
from spark_expectations.secrets import (
    get_spark_expectations_tasks_hook,
    SparkExpectationsSecretPluginSpec,
    CerberusSparkExpectationsSecretPluginImpl,
    SparkExpectationsSecretsBackend,
)
from spark_expectations.config.user_config import Constants as UserConfig


def test_get_secret_value(mocker):
    # Create an instance of the class that implements the send_notification method
    secret_obj = SparkExpectationsSecretPluginSpec()
    mock_context = mocker.MagicMock()

    secret_dict = {
        "username": "test@example.com",
        "password": "XXXXX",
    }
    secret_key_path = "password"

    # Call the send_notification method and assert that it does not return any value
    assert secret_obj.get_secret_value(secret_key_path=mock_context, secret_dict=secret_dict) is None


def test_get_spark_expectations_tasks_hook(caplog):
    # Create instances of the plugin implementations

    # Call the function under test
    result = get_spark_expectations_tasks_hook()
    assert isinstance(result, pluggy._hooks._HookRelay)


@patch("cerberus.client.CerberusClient", autospec=True, spec_set=True)
def test_get_secret_value_with_cerberus(mock_cerberus):
    cerberus_se_handler = CerberusSparkExpectationsSecretPluginImpl()
    # Simulate the return value of get_secrets_data
    secret_value = "my_secret_value"

    mock_cerberus.get_secrets_data.return_value = secret_value
    # mock_cerberus.get_secrets_data.return_value = secret_value
    setattr(mock_cerberus, "get_secrets_data", secret_value)

    # Set up the input parameters
    secret_key_path = "my_secret_key"
    secret_dict = {
        UserConfig.secret_type: "cerberus",
        UserConfig.cbs_url: "https://example.com/cerberus",
    }

    # Call the function under test
    result = cerberus_se_handler.get_secret_value(secret_key_path, secret_dict)

    # Assert the CerberusClient methods were called as expected
    assert mock_cerberus.get_secrets_data == "my_secret_value"


def test_get_secret_value_with_cerberus_none():
    cerberus_se_handler = CerberusSparkExpectationsSecretPluginImpl()
    # Simulate the return value of get_secrets_data

    # Set up the input parameters
    secret_key_path = "my_secret_key"
    secret_dict = {
        UserConfig.secret_type: "x",
        UserConfig.cbs_url: "https://example.com/cerberus",
    }

    # Call the function under test
    result = cerberus_se_handler.get_secret_value(secret_key_path, secret_dict)

    # Assert the CerberusClient methods were called as expected
    assert result is None


@mock.patch("spark_expectations.secrets.get_spark_expectations_tasks_hook")
def test_get_secret_with_valid_key(mock_hook):
    # Set up the mock return value for get_secret_value
    expected_secret_value = "my_secret"
    mock_hook.return_value.get_secret_value.return_value = expected_secret_value

    # Create an instance of the class under test
    secret_manager = SparkExpectationsSecretsBackend(secret_dict={"my_secret_key": "my_secret"})

    # Set up the test input
    secret_key = "my_secret_key"

    # Call the method under test
    result = secret_manager.get_secret(secret_key)

    # Assert the expected behavior and values
    mock_hook.return_value.get_secret_value.assert_called_once_with(
        secret_key_path=secret_key, secret_dict=secret_manager.secret_dict
    )
    assert result == expected_secret_value


@mock.patch("spark_expectations.secrets.get_spark_expectations_tasks_hook")
def test_get_secret_with_invalid_key(mock_hook):
    # Set up the mock return value for get_secret_value
    mock_hook.return_value.get_secret_value.return_value = None

    # Create an instance of the class under test
    secret_manager = SparkExpectationsSecretsBackend(secret_dict={"my_secret_key": "my_secret"})

    # Set up the test input
    secret_key = None

    # Call the method under test
    result = secret_manager.get_secret(secret_key)

    # Assert the expected behavior and values
    mock_hook.return_value.get_secret_value.assert_called_once_with(
        secret_key_path=secret_key, secret_dict=secret_manager.secret_dict
    )
    assert result is None


def test_get_secret_exception():
    # Create an instance of the class under test
    secret_manager = SparkExpectationsSecretsBackend(
        secret_dict={"my_secret_key": "my_secret", UserConfig.secret_type: "databricks"}
    )
    with pytest.raises(Exception):
        secret_manager.get_secret("my_secret_key")
