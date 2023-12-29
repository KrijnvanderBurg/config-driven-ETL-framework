"""
Logger class tests.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""


from logging import Logger

from datastore.logger import get_logger, set_logger


def test_set_logger_default():
    """Test if logging instance is created."""
    logger = set_logger("test_logger")
    assert isinstance(logger, Logger)


def test_set_logger_with_source_name():
    """Test if logging instance is created with given name."""
    source_logger = set_logger("test_logger")
    assert isinstance(source_logger, Logger)

    # Ensure the filename of the RotatingFileHandler matches the expected format
    rotating_file_handler = source_logger.handlers[0]  # first handler must be rotating file handler
    expected_log_filename = "ingestion.log"
    assert rotating_file_handler.baseFilename.endswith(expected_log_filename)


def test_get_logger():
    """Test if logger is instance of logging."""
    logger = get_logger("test_logger")
    assert isinstance(logger, Logger)
