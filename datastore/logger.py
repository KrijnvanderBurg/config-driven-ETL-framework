"""
Logger Implementation.

This module provides functionality for configuring and obtaining logger instances.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

import logging
from logging.handlers import RotatingFileHandler
from sys import stdout

# Log format
FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")


def set_logger(
    name: str,
    filename: str = "ingestion.log",
    level=logging.INFO,
) -> logging.Logger:
    """
    Configure the logging settings.

    Args:
        name (str): Logger name.
        filename (str): Name of the log file, defaults to "ingestion.log" (optional).
        level (enum): Logging level (default is INFO).

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)

    # Add rotating log handler
    rotating_handler = RotatingFileHandler(
        filename=filename,
        maxBytes=5 * 1024 * 1024,  # 5MB
        backupCount=10,  # Max 10 log files before replacing the oldest
    )
    rotating_handler.setLevel(level)
    rotating_handler.setFormatter(FORMATTER)
    logger.addHandler(rotating_handler)

    # Add console stream handler
    console_handler = logging.StreamHandler(stream=stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(FORMATTER)
    logger.addHandler(console_handler)

    return logger


def get_logger(name) -> logging.Logger:
    """
    Get logger instance by name.

    Returns:
        logging.Logger: logger instance.
    """
    return logging.getLogger(name)
