import logging
import pytest
from unittest.mock import Mock, patch, mock_open
from spark_expectations import get_default_log_handler, setup_logger
from spark_expectations.core import load_configurations, get_config_dict, infer_safe_cast
from spark_expectations.config.user_config import Constants as user_config


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


def test_infer_safe_cast_serverless_types():
    """Test infer_safe_cast with serverless configuration types"""
    # Test serverless boolean flags
    assert infer_safe_cast("true") is True
    assert infer_safe_cast("false") is False
    assert infer_safe_cast("True") is True
    assert infer_safe_cast("False") is False
    
    # Test serverless configuration values
    assert infer_safe_cast("25") == 25  # SMTP port
    assert infer_safe_cast("587") == 587  # Alternative SMTP port
    assert infer_safe_cast("localhost") == "localhost"  # SMTP server
    
    # Test None/null values common in serverless configs
    assert infer_safe_cast("null") is None
    assert infer_safe_cast("None") is None
    assert infer_safe_cast("none") is None


def test_get_config_dict_serverless_mode():
    """Test get_config_dict in serverless mode"""
    mock_spark = Mock()
    
    # Test serverless configuration
    user_conf = {
        "spark.expectations.is.serverless": True,
        user_config.se_notifications_enable_email: False,
        user_config.se_enable_obs_dq_report_result: False
    }
    
    notification_dict, streaming_dict = get_config_dict(mock_spark, user_conf)
    
    # Verify serverless defaults are applied
    assert notification_dict["spark.expectations.notifications.email.enabled"] is False
    assert notification_dict["spark.expectations.notifications.slack.enabled"] is False
    assert notification_dict["spark.expectations.notifications.teams.enabled"] is False
    assert notification_dict["spark.expectations.notifications.on.completion"] is True
    assert notification_dict["spark.expectations.notifications.on.fail"] is True
    
    # Streaming dict should be empty in serverless mode
    assert streaming_dict == {}


def test_load_configurations_serverless_safe():
    """Test load_configurations handles serverless environment safely"""
    mock_spark = Mock()
    mock_spark.conf.set = Mock()
    
    # Mock minimal config file for serverless environment
    config_content = """
    spark.sql.adaptive.enabled: true
    se.streaming.enable: false
    spark.expectations.notifications.email.enabled: false
    """
    
    config_dict = {
        "spark.sql.adaptive.enabled": True,
        "se.streaming.enable": False,
        "spark.expectations.notifications.email.enabled": False
    }
    
    with patch('builtins.open', mock_open(read_data=config_content)):
        with patch('yaml.safe_load', return_value=config_dict):
            # Should complete without errors even in serverless-like environment
            load_configurations(mock_spark)
            
            # Verify config setting was attempted
            assert mock_spark.conf.set.call_count >= 1
