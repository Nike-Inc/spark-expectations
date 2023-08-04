import logging
import pytest
from spark_expectations import get_default_log_handler, setup_logger


@pytest.fixture(name="_fixture_logger_handler")
def fixture_logger_handler():
    return get_default_log_handler()


def test_logger_handler_type(_fixture_logger_handler):
    assert isinstance(_fixture_logger_handler, logging.Handler)


def test_logger_handler_formatter(_fixture_logger_handler):
    formatter = _fixture_logger_handler.formatter
    assert isinstance(formatter, logging.Formatter)
    assert formatter._fmt == "[%(asctime)s] [%(levelname)s] [spark_expectations]" \
                             " {%(module)s.py:%(funcName)s:%(lineno)d} - %(message)s"


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
