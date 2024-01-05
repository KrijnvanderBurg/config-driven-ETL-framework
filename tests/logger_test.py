"""
Logger class tests.

# | ✓ | Tests
# |---|-----------------------------------
# | ✓ | Set logger object is logging type.
# | ✓ | Get logger object is logging type.
# | ✓ | Logger rotateFileHandler filename.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from logging import Logger

from datastore.logger import get_logger, set_logger

# ==================================
# ====== Logger Class Tests ========
# ==================================


def test_set_logger_creation() -> None:
    """
    Assert that logging instance is created.
    """
    # Act
    logger = set_logger("test_logger")

    # Assert
    assert isinstance(logger, Logger)


def test_get_logger() -> None:
    """
    Assert that logger is an instance of logging.
    """
    # Act
    logger = get_logger("test_logger")

    # Assert
    assert isinstance(logger, Logger)


# When adding return type None (-> None) then mypy errors: "Handler" has no attribute "baseFilename"
def test_set_logger_with_source_name():
    """
    Assert that logging instance is created with the given name.
    """
    # Arrange
    source_logger = set_logger("test_logger")

    # Act
    rotating_file_handler = source_logger.handlers[0]  # the first handler must be a rotating file handler
    expected_log_filename = "ingestion.log"

    # Assert
    assert rotating_file_handler.baseFilename.endswith(expected_log_filename)
