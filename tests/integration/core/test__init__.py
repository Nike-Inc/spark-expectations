import json
import os
import pytest
from unittest import mock
from unittest.mock import patch, mock_open
from typing import Any
import yaml
from pyspark.sql.session import SparkSession
from spark_expectations.core import get_spark_session, load_configurations, get_config_dict, infer_safe_cast
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
        spark.conf.get("spark.jars") == f"{current_dir}/../../jars/spark-sql-kafka-0-10_2.13-4.0.0.jar,"
        f"{current_dir}/../../jars/kafka-clients-3.7.0.jar,"
        f"{current_dir}/../../jars/commons-pool2-2.12.0.jar,"
        f"{current_dir}/../../jars/spark-token-provider-kafka-0-10_2.13-4.0.0.jar"
    )

    # Add more assertions to test any other desired SparkSession configuration options


@mock.patch.dict(os.environ, {"UNIT_TESTING_ENV": "disable", "SPARKEXPECTATIONS_ENV": "disable"})
def test_get_spark_active_session():
    spark = SparkSession.builder.getOrCreate()

    # Now try to get the active session as we disabled unittest flags for this test
    active = get_spark_session()
    assert active == spark

@patch("spark_expectations.core.__init__.current_dir", new=os.path.dirname(__file__)+"/../../../")
def test_load_configurations_valid_file():
    spark = SparkSession.builder.getOrCreate()
    streaming_dict, notification_dict = load_configurations(spark)

    # Check that the configurations are returned correctly
    assert streaming_dict is not None
    assert notification_dict is not None

    # Check that the streaming and notification configurations are loaded correctly
    assert streaming_dict["se.streaming.dbx.secret.scope"] == "secret_scope"
    assert notification_dict["spark.expectations.notifications.email.subject"] == "spark-expectations-testing"
    assert notification_dict["spark.expectations.notifications.email.from"] == ""
    assert streaming_dict["se.streaming.enable"] is True

@patch("spark_expectations.core.__init__.current_dir", new=os.path.dirname(__file__) + "/../../../")
def test_load_configurations_invalid_file():
    """Test that load_configurations raises RuntimeError for invalid YAML content."""
    spark = SparkSession.builder.getOrCreate()

    # Test with malformed YAML - unclosed bracket
    invalid_yaml_content = "key:[value, another"

    with patch("builtins.open", mock_open(read_data=invalid_yaml_content)):
        with pytest.raises(RuntimeError) as exception_info:
            load_configurations(spark)

        # Verify the error message contains expected text
        assert "Error parsing Spark config YAML configuration file" in str(exception_info.value)

@patch("spark_expectations.core.__init__.current_dir", new=os.path.dirname(__file__) + "/../../../")
def test_load_configurations_empty_file():
    """Test that load_configurations handles empty files correctly."""
    spark = SparkSession.builder.getOrCreate()

    # Mock the open function to return empty data
    with patch("builtins.open", mock_open(read_data="")):
        streaming_dict, notification_dict = load_configurations(spark)

        # Check that the default values are returned correctly (empty dicts)
        assert streaming_dict == {}
        assert notification_dict == {}

@pytest.fixture
def spark_session():
    """Fixture to provide a SparkSession for testing."""
    return SparkSession.builder.getOrCreate()

@pytest.fixture
def mock_config_data():
    """Fixture providing mock configuration data from spark-expectations default config."""
    return {
        'streaming': {
            'se.streaming.enable': True,
            'se.streaming.secret.type': 'databricks',
            'se.streaming.dbx.workspace.url': 'https://workspace.cloud.databricks.com',
            'se.streaming.dbx.secret.scope': 'secret_scope',
            'se.streaming.dbx.topic.name': 'se_streaming_topic_name'
        },
        'notification': {
            'spark.expectations.notifications.email.enabled': False,
            'spark.expectations.notifications.slack.enabled': False,
            'spark.expectations.notifications.teams.enabled': False,
            'spark.expectations.notifications.zoom.enabled': False,
            'spark.expectations.notifications.on.start': False,
            'spark.expectations.notifications.on.completion': True,
            'spark.expectations.notifications.on.fail': True,
            'spark.expectations.notifications.error.drop.threshold': 100,
            'spark.expectations.notifications.email.subject': 'spark-expectations-testing',
            'spark.expectations.notifications.smtp.creds.dict': { 
                'se.streaming.secret.type': 'cerberus',
                "se.streaming.cerberus.url": 'https://cerberus.example.com',
                'se.streaming.cerberus.sdb.path': 'your_sdb_path',
                'spark.expectations.notifications.cerberus.smtp.password': 'your_smtp_password'
            }
        }
    }

# Tests for get_config_dict function

@patch("spark_expectations.core.load_configurations")
def test_get_config_dict_basic_functionality(mock_load_configs, mock_config_data: dict[str, dict[str, Any]]):
    """Test basic functionality of get_config_dict."""
    spark = SparkSession.builder.getOrCreate()

    # Mock load_configurations to return the config dictionaries
    mock_load_configs.return_value = (mock_config_data['streaming'], mock_config_data['notification'])

    # Call the function
    notification_dict, streaming_dict = get_config_dict(spark)

    # Verify load_configurations was called
    mock_load_configs.assert_called_once_with(spark)

    # Verify return types and basic content
    assert isinstance(notification_dict, dict)
    assert isinstance(streaming_dict, dict)
    assert streaming_dict['se.streaming.enable'] is True
    assert notification_dict['spark.expectations.notifications.email.enabled'] is False

@patch("spark_expectations.core.load_configurations")
def test_get_config_dict_with_user_conf(mock_load_configs, mock_config_data: dict[str, dict[str, Any]]):
    """Test get_config_dict with user configuration override."""
    spark = SparkSession.builder.getOrCreate()

    # Mock load_configurations to return the config dictionaries
    mock_load_configs.return_value = (mock_config_data['streaming'], mock_config_data['notification'])

    # User configuration to override defaults
    user_conf = {
        'se.streaming.enable': False,
        'spark.expectations.notifications.email.enabled': True
    }

    # Call the function with user config
    notification_dict, streaming_dict = get_config_dict(spark, user_conf)

    # Verify user overrides are applied
    assert streaming_dict['se.streaming.enable'] is False
    assert notification_dict['spark.expectations.notifications.email.enabled'] is True

@patch("spark_expectations.core.load_configurations")
def test_get_config_dict_with_partial_user_conf(mock_load_configs, mock_config_data: dict[str, dict[str, Any]]):
    """Test get_config_dict with partial user configuration."""
    spark = SparkSession.builder.getOrCreate()

    # Mock load_configurations to return the config dictionaries
    mock_load_configs.return_value = (mock_config_data['streaming'], mock_config_data['notification'])

    # User configuration with only some keys
    user_conf = {
        'se.streaming.enable': False
        # email.enabled not specified, should use default
    }

    # Call the function with partial user config
    notification_dict, streaming_dict = get_config_dict(spark, user_conf)

    # Verify user override is applied and default is used for unspecified key
    assert streaming_dict['se.streaming.enable'] is False
    assert notification_dict['spark.expectations.notifications.email.enabled'] is False  # from default

@patch("spark_expectations.core.load_configurations")
def test_get_config_dict_no_user_conf_uses_defaults(mock_load_configs, mock_config_data: dict[str, dict[str, Any]]):
    """Test get_config_dict without user_conf uses defaults directly."""
    spark = SparkSession.builder.getOrCreate()

    # Mock load_configurations to return the config dictionaries
    mock_load_configs.return_value = (mock_config_data['streaming'], mock_config_data['notification'])

    # Call the function without user_conf (should use defaults)
    notification_dict, streaming_dict = get_config_dict(spark, None)

    # Verify default values are used
    assert streaming_dict['se.streaming.enable'] is True  # from mock_config_data
    assert notification_dict['spark.expectations.notifications.email.enabled'] is False  # from mock_config_data

@patch("spark_expectations.core.load_configurations")
def test_get_config_dict_serverless_mode(mock_load_configs, mock_config_data: dict[str, dict[str, Any]]):
    """Test get_config_dict with serverless mode enabled."""
    spark = SparkSession.builder.getOrCreate()

    # Mock load_configurations to return the config dictionaries
    mock_load_configs.return_value = (mock_config_data['streaming'], mock_config_data['notification'])

    # User configuration with serverless mode enabled
    user_conf = {
        'spark.expectations.is.serverless': True,
        'se.streaming.enable': False,
        'spark.expectations.notifications.email.enabled': True
    }

    # Call the function with serverless mode
    notification_dict, streaming_dict = get_config_dict(spark, user_conf)

    # Verify serverless mode uses user_conf values without accessing spark.conf
    assert streaming_dict['se.streaming.enable'] is False
    assert notification_dict['spark.expectations.notifications.email.enabled'] is True

@patch("spark_expectations.core.load_configurations")
def test_get_config_dict_empty_configs(mock_load_configs):
    """Test get_config_dict with empty configuration dictionaries."""
    spark = SparkSession.builder.getOrCreate()

    # Mock load_configurations to return empty dictionaries
    mock_load_configs.return_value = ({}, {})

    # Call the function
    notification_dict, streaming_dict = get_config_dict(spark)

    # Verify empty dictionaries are returned
    assert notification_dict == {}
    assert streaming_dict == {}

@patch("spark_expectations.core.load_configurations")
def test_get_config_dict_load_configurations_error(mock_load_configs):
    """Test get_config_dict handles load_configurations errors properly."""
    spark = SparkSession.builder.getOrCreate()

    # Make load_configurations raise an exception
    mock_load_configs.side_effect = RuntimeError("Configuration load failed")

    # Verify RuntimeError is propagated and wrapped
    with pytest.raises(RuntimeError) as exc_info:
        get_config_dict(spark)

    error_message = str(exc_info.value)
    assert "Error retrieving configuration: Configuration load failed" in error_message

# Tests for infer_safe_cast function

@pytest.mark.parametrize("input_value,expected", [
    # Test integers
    ("123", 123),
    ("0", 0),
    ("-456", -456),
    # Test floats
    ("3.14", 3.14),
    ("0.0", 0.0),
    ("-2.5", -2.5),
    # Test booleans
    ("true", True),
    ("false", False),
    ("TRUE", True),
    ("FALSE", False),
    # Test strings
    ("hello", "hello"),
    ("  spaced  ", "spaced"),
    ("", ""),
    ("not_a_number", "not_a_number"),
])
def test_infer_safe_cast_basic_types(input_value, expected):
    """Test basic type inference for common values."""
    result = infer_safe_cast(input_value)
    if isinstance(expected, bool):
        assert result is expected
    else:
        assert result == expected

@pytest.mark.parametrize("input_value,expected", [
    (None, None),
    ("none", None),
    ("null", None),
    ("NONE", None),
    ("NULL", None),
])
def test_infer_safe_cast_none_handling(input_value, expected):
    """Test None value handling."""
    assert infer_safe_cast(input_value) == expected

@pytest.mark.parametrize("input_value,expected_type", [
    (42, int),
    (3.14, float),
    (True, bool),
    (False, bool),
    ({"key": "value"}, dict),
    ([1, 2, 3], list),
])
def test_infer_safe_cast_existing_types(input_value, expected_type):
    """Test that existing types are preserved."""
    result = infer_safe_cast(input_value)
    assert result == input_value
    assert isinstance(result, expected_type)

@pytest.mark.parametrize("dict_str,expected", [
    ("{'key': 'value', 'num': 42}", {'key': 'value', 'num': 42}),
    ("{}", {}),
    ("{invalid dict}", "{invalid dict}"),
    ("{'nested': {'key': 'value'}}", {'nested': {'key': 'value'}}),
])
def test_infer_safe_cast_dictionary_strings(dict_str, expected):
    """Test dictionary string conversion."""
    result = infer_safe_cast(dict_str)
    if isinstance(expected, dict):
        assert isinstance(result, dict)
        assert result == expected
    else:
        assert result == expected
