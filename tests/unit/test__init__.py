import logging
import pytest
import yaml
from unittest.mock import Mock, patch, mock_open
from spark_expectations import get_default_log_handler, setup_logger


@pytest.fixture(name="_fixture_logger_handler")
def fixture_logger_handler():
    return get_default_log_handler()


def test_logger_handler_type(_fixture_logger_handler):
    assert isinstance(_fixture_logger_handler, logging.Handler)


def test_logger_handler_formatter(_fixture_logger_handler):
    formatter = _fixture_logger_handler.formatter
    assert isinstance(formatter, logging.Formatter)
    assert (
        formatter._fmt == "[%(asctime)s] [%(levelname)s] [spark_expectations]"
        " {%(module)s.py:%(funcName)s:%(lineno)d} - %(message)s"
    )


@pytest.mark.parametrize("pkg_name", ["foo", "bar"])
def test_logger_handler_pkg_name(pkg_name):
    logger_handler = get_default_log_handler(pkg_name)
    formatter = logger_handler.formatter
    assert pkg_name in formatter._fmt


@pytest.fixture(name="_fixture_logger_name")
def fixture_logger_name():
    return "test_logger"


@pytest.fixture(name="_fixture_logger")
def fixture_logger(_fixture_logger_name):
    return setup_logger(_fixture_logger_name)


@pytest.mark.parametrize("name", [None, "custom_name"])
def test_setup_logger(name, _fixture_logger_name, _fixture_logger):
    log = setup_logger(name)
    assert log.name == name or _fixture_logger_name
    assert log.level == logging.INFO
    assert log.propagate is False
    assert len(log.handlers) == 1


def test_infer_safe_cast_comprehensive_coverage():
    """Test infer_safe_cast to cover all missing branches and improve coverage"""
    from spark_expectations.core import infer_safe_cast
    
    # Test malformed dictionary string that triggers ValueError/SyntaxError (line 223)
    malformed_dict = "{'key': invalid_syntax without quotes}"
    result = infer_safe_cast(malformed_dict)
    assert result == malformed_dict  # Should return original string when dict parsing fails
    
    # Test another malformed dictionary that triggers SyntaxError
    malformed_dict2 = "{'unclosed_dict': 'value'"
    result2 = infer_safe_cast(malformed_dict2)
    assert result2 == malformed_dict2  # Should return original string
    
    # Test edge cases for comprehensive coverage
    assert infer_safe_cast(None) is None
    assert infer_safe_cast("") == ""
    assert infer_safe_cast(" ") == ""
    assert infer_safe_cast("  null  ") is None
    assert infer_safe_cast("TRUE") is True
    assert infer_safe_cast("FALSE") is False


def test_load_configurations_error_handling():
    """Test load_configurations error handling - covers lines 50-55"""
    from spark_expectations.core import load_configurations
    import tempfile
    import os
    
    mock_spark = Mock()
    mock_spark.conf.set = Mock()
    
    # Test FileNotFoundError (line 50)
    with patch('builtins.open', side_effect=FileNotFoundError("Config file not found")):
        with pytest.raises(RuntimeError, match="Spark config YAML file not found"):
            load_configurations(mock_spark)
    
    # Test YAMLError (line 52)  
    with patch('builtins.open', mock_open(read_data="invalid: yaml: content: [")):
        with patch('yaml.safe_load', side_effect=yaml.YAMLError("Invalid YAML")):
            with pytest.raises(RuntimeError, match="Error parsing Spark config YAML configuration file"):
                load_configurations(mock_spark)
    
    # Test general Exception (line 54)
    with patch('builtins.open', side_effect=Exception("Unexpected error")):
        with pytest.raises(RuntimeError, match="An unexpected error occurred while loading spark configurations"):
            load_configurations(mock_spark)


def test_get_config_dict_error_conditions():
    """Test get_config_dict error handling - covers lines 140-142"""
    from spark_expectations.core import get_config_dict
    import json
    
    mock_spark = Mock()
    
    # Test JSONDecodeError (line 140)
    with patch('spark_expectations.core.load_configurations'):
        mock_spark.conf.get.side_effect = lambda key, default=None: {
            "default_notification_dict": "invalid json {",
            "default_streaming_dict": "{}"
        }.get(key, default)
        
        user_conf = {"spark.expectations.is.serverless": False}
        
        with pytest.raises(RuntimeError, match="Error parsing configuration JSON"):
            get_config_dict(mock_spark, user_conf)
    
    # Test general Exception (line 142)
    with patch('spark_expectations.core.load_configurations', side_effect=Exception("Config error")):
        user_conf = {"spark.expectations.is.serverless": False}
        
        with pytest.raises(RuntimeError, match="Error retrieving configuration"):
            get_config_dict(mock_spark, user_conf)
