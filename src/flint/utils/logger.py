"""Logging utilities for the ingestion framework.

This module provides standardized logging configuration for the framework,
with support for both console and file-based logging with rotation.
"""

import logging
import os
from logging.handlers import RotatingFileHandler
from sys import stdout

FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")


def set_logger(name: str | None = None, filename: str = "flint.log", level: str | None = None) -> logging.Logger:
    """
    Configure and return a logger with file and console handlers.

    Args:
        name: The name for the logger, or None for the root logger.
        filename: Path to the log file (defaults to "flint.log").
        level: The logging level as a string (e.g., "INFO", "DEBUG"). If not provided, uses environment
            variables or defaults to "INFO".

    Returns:
        logging.Logger: A configured Logger instance ready to use.
    """
    level = level or os.getenv("FLINT_LOG_LEVEL") or os.getenv("LOG_LEVEL") or "INFO"
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        rotating_handler = RotatingFileHandler(
            filename=filename,
            maxBytes=5 * 1024 * 1024,
            backupCount=10,
        )
        rotating_handler.setFormatter(FORMATTER)
        rotating_handler.setLevel(level)
        logger.addHandler(rotating_handler)

        console_handler = logging.StreamHandler(stream=stdout)
        console_handler.setLevel(level)
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
