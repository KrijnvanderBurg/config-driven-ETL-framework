"""
Logger implementation.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""


import logging
from logging.handlers import RotatingFileHandler
from sys import stdout

FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")


def set_logger(
    name: str,
    log_filename: str = "ingestion.log",
    level=logging.INFO,
) -> logging.Logger:
    """
    Set logging configuration.

    Args:
        name (str): Logger name.
        log_filename (str): name of logfile, defaults to ingestion.log (optional).
        level (enum): Logging level (default is INFO).

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)

    # add rotating log handler
    rotating_handler = RotatingFileHandler(
        filename=log_filename,
        maxBytes=5 * 1024 * 1024,  # 5MB
        backupCount=10,  # max 10 log files before replacing the oldest
    )
    rotating_handler.setLevel(level)
    rotating_handler.setFormatter(FORMATTER)
    logger.addHandler(rotating_handler)

    # add console stream handler
    console_handler = logging.StreamHandler(stream=stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(FORMATTER)
    logger.addHandler(console_handler)

    return logger


def get_logger(name):
    """Get logger instance."""
    return logging.getLogger(name)
