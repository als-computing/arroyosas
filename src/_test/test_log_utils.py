"""Tests for arroyosas.log_utils"""

import logging

from arroyosas.log_utils import setup_logger


def test_setup_logger_default_level():
    logger = logging.getLogger("test_log_utils_default")
    logger.handlers.clear()
    setup_logger(logger)
    assert logger.level == logging.INFO


def test_setup_logger_debug_level():
    logger = logging.getLogger("test_log_utils_debug")
    logger.handlers.clear()
    setup_logger(logger, "DEBUG")
    assert logger.level == logging.DEBUG


def test_setup_logger_warning_level():
    logger = logging.getLogger("test_log_utils_warning")
    logger.handlers.clear()
    setup_logger(logger, "WARNING")
    assert logger.level == logging.WARNING


def test_setup_logger_adds_handler():
    logger = logging.getLogger("test_log_utils_handler")
    logger.handlers.clear()
    setup_logger(logger)
    assert len(logger.handlers) >= 1
    assert isinstance(logger.handlers[0], logging.StreamHandler)


def test_setup_logger_case_insensitive():
    logger = logging.getLogger("test_log_utils_case")
    logger.handlers.clear()
    setup_logger(logger, "debug")
    assert logger.level == logging.DEBUG


def test_setup_logger_formatter():
    logger = logging.getLogger("test_log_utils_fmt")
    logger.handlers.clear()
    setup_logger(logger)
    handler = logger.handlers[0]
    assert handler.formatter is not None
    fmt = handler.formatter._fmt
    assert "%(levelname)s" in fmt
    assert "%(name)s" in fmt
    assert "%(message)s" in fmt
