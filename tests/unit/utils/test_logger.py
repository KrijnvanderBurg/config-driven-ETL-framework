from logging import Logger

from ingestion_framework.utils.logger import get_logger, set_logger


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
