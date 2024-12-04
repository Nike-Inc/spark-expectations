import sys
import logging
from typing import Optional


def get_default_log_handler(pkg_name: str = "spark_expectations") -> logging.Handler:
    logger_handler = logging.StreamHandler(stream=sys.stdout)
    logger_handler.setFormatter(
        logging.Formatter(
            f"[%(asctime)s] [%(levelname)s] [{pkg_name}] "
            "{%(module)s.py:%(funcName)s:%(lineno)d} - %(message)s"
        )
    )
    return logger_handler


def setup_logger(name: Optional[str] = None) -> logging.Logger:
    log = logging.getLogger(name or __name__)  # Logger
    log.setLevel(logging.INFO)
    logger_handler = get_default_log_handler()
    log.propagate = False
    if log.hasHandlers():
        log.handlers.clear()
    log.addHandler(logger_handler)
    return log


_log = setup_logger()
