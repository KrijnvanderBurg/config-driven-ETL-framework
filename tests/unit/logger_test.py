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

# ============ Tests ===============


def test__set_logger() -> None:
    """
    Assert that logging instance is created.
    """
    # Act
    logger = set_logger("test_logger")

    # Assert
    assert isinstance(logger, Logger)
    assert logger.name == "test_logger"


def test__get_logger() -> None:
    """
    Assert that logger is an instance of logging.
    """
    # Act
    logger = get_logger("test_logger")

    # Assert
    assert isinstance(logger, Logger)
    assert logger.name == "test_logger"


def test__set_logger__with_source_name() -> None:
    """
    Assert that logging instance is created with the given name.
    """
    # Act
    source_logger = set_logger("test_logger")
    rotating_file_handler = source_logger.handlers[0]  # the first handler must be a rotating file handler
    expected_log_filename = "ingestion.log"

    # Assert
    assert rotating_file_handler.baseFilename.endswith(expected_log_filename)  # type: ignore[attr-defined]
