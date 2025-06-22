"""Logging utilities for the ingestion framework.

This module provides standardized logging configuration for the framework,
with support for both console and file-based logging with rotation.
"""

import logging
import os
from logging.handlers import RotatingFileHandler
from sys import stdout

# Log format
FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")


def set_logger(name: str, filename: str = "ingestion.log") -> logging.Logger:
    """Configure and return a logger with file and console handlers.

    Creates a logger with the specified name and configures it with both a rotating
    file handler and a console handler. The file handler automatically rotates log
    files when they reach 5MB, keeping up to 10 backup files.

    Args:
        name: The name for the logger, typically the module name
        filename: Path to the log file (defaults to "ingestion.log")

    Returns:
        A configured Logger instance ready to use

    Example:
        ```python
        logger = set_logger(__name__)
        logger.info("Processing started")
        logger.error("An error occurred: %s", error_message)
        ```
    """

    log_level: str | None = os.getenv("FLINT_LOG_LEVEL")
    if not log_level:
        log_level = os.getenv("LOG_LEVEL", "INFO")

    logger = logging.getLogger(name)

    # Add rotating log handler
    rotating_handler = RotatingFileHandler(
        filename=filename,
        maxBytes=5 * 1024 * 1024,  # 5MB
        backupCount=10,  # Max 10 log files before replacing the oldest
    )
    rotating_handler.setLevel(log_level)
    rotating_handler.setFormatter(FORMATTER)
    logger.addHandler(rotating_handler)

    # Add console stream handler
    console_handler = logging.StreamHandler(stream=stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(FORMATTER)
    logger.addHandler(console_handler)

    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get an existing logger instance by name.

    Args:
        name (str): Name of the logger to retrieve.

    Returns:
        logging.Logger: The requested logger instance.
    """
    return logging.getLogger(name)
