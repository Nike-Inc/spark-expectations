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
    
    # Test valid dictionary parsing that should hit line 220 (return parsed)
    valid_dict_str = "{'key': 'value', 'number': 42}"
    result_dict = infer_safe_cast(valid_dict_str)
    expected_dict = {'key': 'value', 'number': 42}
    assert result_dict == expected_dict
    assert isinstance(result_dict, dict)
    
    # Test another valid dictionary
    valid_dict_str2 = "{'nested': {'inner': 'value'}}"
    result_dict2 = infer_safe_cast(valid_dict_str2)
    assert result_dict2 == {'nested': {'inner': 'value'}}
    assert isinstance(result_dict2, dict)
    
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
    """Test get_config_dict error handling"""
    from spark_expectations.core import get_config_dict
    
    mock_spark = Mock()
    
    # Test general Exception
    with patch('spark_expectations.core.load_configurations', side_effect=Exception("Config error")):
        user_conf = {"spark.expectations.is.serverless": False}
        
        with pytest.raises(RuntimeError, match="Error retrieving configuration"):
            get_config_dict(mock_spark, user_conf)


def test_load_configurations_success_scenarios():
    """Test load_configurations with valid config data - covers lines 33-47"""
    from spark_expectations.core import load_configurations
    import json
    
    mock_spark = Mock()
    mock_spark.conf.set = Mock()
    
    # Test with None config (line 34-35)
    with patch('builtins.open', mock_open(read_data="")):
        with patch('yaml.safe_load', return_value=None):
            load_configurations(mock_spark)
            # Should handle None config gracefully
    
    # Test with invalid config type (line 36-37) 
    with patch('builtins.open', mock_open(read_data="invalid_list_config")):
        with patch('yaml.safe_load', return_value=["invalid", "config"]):
            with pytest.raises(RuntimeError, match="Error parsing Spark config YAML configuration file"):
                load_configurations(mock_spark)
    
    # Test valid config processing (lines 38-47)
    valid_config = {
        "se.streaming.topic.name": "test_topic",
        "spark.expectations.notifications.email.enabled": True,
        "spark.sql.adaptive.enabled": True
    }
    
    with patch('builtins.open', mock_open()):
        with patch('yaml.safe_load', return_value=valid_config):
            streaming_dict, notification_dict = load_configurations(mock_spark)
            
            # Verify config processing
            mock_spark.conf.set.assert_any_call("spark.sql.adaptive.enabled", "True")
            # Verify the returned dictionaries
            assert streaming_dict == {"se.streaming.topic.name": "test_topic"}
            assert notification_dict == {"spark.expectations.notifications.email.enabled": True}


def test_build_config_dict_comprehensive():
    """Test _build_config_dict internal function comprehensively"""
    from spark_expectations.core import get_config_dict
    
    mock_spark = Mock()
    # Mock spark.conf.get to return the default value (second argument) when called
    mock_spark.conf.get = Mock(side_effect=lambda key, default=None: default)
    
    # Test with user_conf that overrides defaults
    with patch('spark_expectations.core.load_configurations') as mock_load_configs:
        mock_load_configs.return_value = (
            {},
            {"spark.expectations.notifications.email.enabled": True}
        )
        user_conf_with_overrides = {
            "spark.expectations.notifications.email.enabled": False,
            "custom.config": "test_value"
        }
        
        notification_dict, streaming_dict = get_config_dict(mock_spark, user_conf_with_overrides)
        
        # Verify user_conf overrides defaults
        assert notification_dict["spark.expectations.notifications.email.enabled"] == False
        assert streaming_dict == {}  # Empty from load_configurations
    
    # Test with user_conf containing partial configs
    # Note: _build_config_dict only includes keys from default_dict, not from user_conf
    with patch('spark_expectations.core.load_configurations') as mock_load_configs:
        mock_load_configs.return_value = (
            {"se.streaming.enable": False},
            {
                "spark.expectations.notifications.email.enabled": True,
                "spark.expectations.notifications.slack.enabled": False
            }
        )
        
        user_conf_partial = {
            "spark.expectations.notifications.slack.enabled": True
        }
        
        notification_dict, streaming_dict = get_config_dict(mock_spark, user_conf_partial)
        
        # Verify user_conf partial values merge with defaults
        # email.enabled is not in user_conf, so it gets default value from load_configurations
        assert notification_dict["spark.expectations.notifications.email.enabled"] == True  # from defaults
        # slack.enabled is in user_conf, so it overrides the default
        assert notification_dict["spark.expectations.notifications.slack.enabled"] == True  # from user_conf
        assert streaming_dict["se.streaming.enable"] == False  # from defaults
    
    # Test user_conf is None - should use defaults directly
    with patch('spark_expectations.core.load_configurations') as mock_load_configs:
        mock_load_configs.return_value = (
            {},
            {"spark.expectations.notifications.email.enabled": False}
        )
        
        notification_dict, streaming_dict = get_config_dict(mock_spark, None)
        
        # Verify None user_conf uses defaults
        assert notification_dict["spark.expectations.notifications.email.enabled"] == False
        assert isinstance(notification_dict, dict)
        assert isinstance(streaming_dict, dict)


def test_build_config_dict_no_user_conf_uses_defaults():
    """Test _build_config_dict when user_conf is None - should use defaults directly"""
    from spark_expectations.core import get_config_dict
    
    mock_spark = Mock()
    # Mock spark.conf.get to return the default value (second argument) when called
    mock_spark.conf.get = Mock(side_effect=lambda key, default=None: default)
    
    # Mock scenario where user_conf is None and we use defaults
    with patch('spark_expectations.core.load_configurations') as mock_load_configs:
        mock_load_configs.return_value = (
            {"se.streaming.enable": True},
            {"spark.expectations.notifications.email.enabled": False}
        )
        
        # When user_conf is None, should use defaults directly
        notification_dict, streaming_dict = get_config_dict(mock_spark, None)
        
        # Verify defaults are used
        assert notification_dict["spark.expectations.notifications.email.enabled"] == False
        assert streaming_dict["se.streaming.enable"] == True
        assert isinstance(notification_dict, dict)
        assert isinstance(streaming_dict, dict)


def test_get_spark_session_local_environment():
    """Test get_spark_session with UNIT_TESTING_ENV set - covers lines 143-168"""
    from spark_expectations.core import get_spark_session
    import os
    
    # Test UNIT_TESTING_ENV branch (lines 143-168)
    with patch.dict(os.environ, {"UNIT_TESTING_ENV": "spark_expectations_unit_testing_on_github_actions"}):
        with patch('spark_expectations.core.SparkSession.builder') as mock_builder:
            mock_session = Mock()
            mock_chain = Mock()
            
            # Setup the builder chain
            mock_builder.config.return_value = mock_chain
            mock_chain.config.return_value = mock_chain
            mock_chain.getOrCreate.return_value = mock_session
            
            with patch('spark_expectations.core.load_configurations') as mock_load_config:
                result = get_spark_session()
                
                # Verify the SparkSession builder was configured with Delta Lake settings
                mock_builder.config.assert_called_with("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                # Verify load_configurations was called
                mock_load_config.assert_called_once_with(mock_session)
                assert result == mock_session


def test_get_spark_session_sparkexpectations_env():
    """Test get_spark_session with SPARKEXPECTATIONS_ENV set - covers lines 143-168"""
    from spark_expectations.core import get_spark_session
    import os
    
    # Test SPARKEXPECTATIONS_ENV branch (lines 143-168)
    with patch.dict(os.environ, {"SPARKEXPECTATIONS_ENV": "local"}, clear=True):
        with patch('spark_expectations.core.SparkSession.builder') as mock_builder:
            mock_session = Mock()
            mock_chain = Mock()
            
            # Setup the builder chain
            mock_builder.config.return_value = mock_chain
            mock_chain.config.return_value = mock_chain
            mock_chain.getOrCreate.return_value = mock_session
            
            with patch('spark_expectations.core.load_configurations') as mock_load_config:
                result = get_spark_session()
                
                # Verify JAR configuration was set (lines 154-160)
                jar_calls = [call for call in mock_chain.config.call_args_list if 'spark.jars' in str(call)]
                assert len(jar_calls) > 0  # Verify JAR config was called
                
                # Verify other configurations (lines 161-165)
                config_calls = [str(call) for call in mock_chain.config.call_args_list]
                assert any('spark.sql.shuffle.partitions' in call for call in config_calls)
                assert any('spark.ui.enabled' in call for call in config_calls)
                
                mock_load_config.assert_called_once_with(mock_session)
                assert result == mock_session


def test_get_spark_session_active_session_branch():
    """Test get_spark_session else branch using getActiveSession - covers lines 169-171"""
    from spark_expectations.core import get_spark_session
    import os
    
    # Test else branch (lines 169-171)
    with patch.dict(os.environ, {}, clear=True):  # Clear environment variables
        with patch('spark_expectations.core.SparkSession.getActiveSession') as mock_get_active:
            mock_session = Mock()
            mock_get_active.return_value = mock_session
            
            with patch('spark_expectations.core.load_configurations') as mock_load_config:
                result = get_spark_session()
                
                # Verify getActiveSession was called (line 170)
                mock_get_active.assert_called_once()
                mock_load_config.assert_called_once_with(mock_session)
                assert result == mock_session


def test_infer_safe_cast_string_fallback():
    """Test infer_safe_cast final string fallback - covers lines 219-220"""
    from spark_expectations.core import infer_safe_cast
    
    # Test final fallback to string (lines 219-220)
    # Use strings that can't be converted to int, float, bool, or dict
    test_strings = [
        "this is just a regular string that can't be converted",
        "not_a_number_or_bool",
        "random_text_123_abc",
        "hello world!",
        "some-special-chars@#$%"
    ]
    
    for test_str in test_strings:
        result = infer_safe_cast(test_str)
        # Should return the cleaned string (line 220)
        assert result == test_str
        assert isinstance(result, str)
    
    # Test with string that has whitespace - should be stripped
    whitespace_string = "   some string with spaces   "
    result2 = infer_safe_cast(whitespace_string)
    assert result2 == "some string with spaces"  # Stripped
    
    # Test with non-string input that falls back to string
    class CustomObject:
        def __str__(self):
            return "custom_object_string"
    
    custom_obj = CustomObject()
    result3 = infer_safe_cast(custom_obj)
    assert result3 == "custom_object_string"
    assert isinstance(result3, str)
    
    # Test with complex object that becomes string
    test_tuple = (1, 2, 3)
    result4 = infer_safe_cast(test_tuple)
    assert result4 == "(1, 2, 3)"
    assert isinstance(result4, str)


def test_infer_safe_cast_complete_fallback_coverage():
    """Ensure we hit all branches including the dictionary parsing exception - covers lines 220-225"""
    from spark_expectations.core import infer_safe_cast
    
    # Test various inputs that will definitely hit the final fallback
    test_cases = [
        "definitely_not_a_number",
        "not_true_or_false", 
        "not{a:valid}dict",
        "random text with symbols !@#$%^&*()",
        "   whitespace_test   ",  # This will be stripped
        "{'incomplete': dict without closing brace",  # Should trigger SyntaxError in ast.literal_eval
        "[1, 2, 3",  # Another syntax error case
        "{'key': 'value'} extra text",  # Valid dict but with extra text
    ]
    
    for test_input in test_cases:
        result = infer_safe_cast(test_input)
        expected = str(test_input).strip()
        assert result == expected
        assert isinstance(result, str)
        
    # Test with a bytes object to ensure it converts to string
    bytes_input = b"bytes_string"
    result_bytes = infer_safe_cast(bytes_input)
    assert result_bytes == "b'bytes_string'"
    assert isinstance(result_bytes, str)
    
    # Specifically test the dictionary parsing exception path
    malformed_dict_cases = [
        "{'key': value_without_quotes}",  # Should trigger ValueError in ast.literal_eval
        "{key: 'value'}",  # Invalid dict format 
        "{'nested': {'incomplete': }",  # Nested incomplete dict
    ]
    
    for malformed_dict in malformed_dict_cases:
        result = infer_safe_cast(malformed_dict)
        # Should fall back to string due to ValueError/SyntaxError in dict parsing
        assert result == malformed_dict
        assert isinstance(result, str)
