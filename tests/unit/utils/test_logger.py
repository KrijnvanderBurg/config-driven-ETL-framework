from logging import Logger

from flint.utils.logger import get_logger, set_logger


class TestLogger:
    """Unit tests for the logging utility functions."""

    def test_set_logger_creates_logger(self) -> None:
        """
        Test set_logger returns a configured logger instance.

        This test checks if the set_logger function correctly creates and
        configures a logger instance with the given name.
        """
        # Act
        logger = set_logger("test_logger")

        # Assert
        assert isinstance(logger, Logger)
        assert logger.name == "test_logger"

    def test_get_logger(self) -> None:
        """
        Test get_logger returns the correct logger instance.

        This test verifies that the get_logger function returns a logger
        instance that has been previously configured with set_logger.
        """
        # Act
        logger = get_logger("test_logger")

        # Assert
        assert isinstance(logger, Logger)
        assert logger.name == "test_logger"

    def test_set_logger_file_output(self, tmp_path) -> None:
        """
        Test set_logger writes logs to the specified file.

        This test ensures that the logs are being written to the correct
        file when a logger is created with set_logger.
        """
        # Act
        source_logger = set_logger("test_logger")
        rotating_file_handler = source_logger.handlers[0]  # the first handler must be a rotating file handler
        expected_log_filename = "ingestion.log"

        # Assert
        assert rotating_file_handler.baseFilename.endswith(expected_log_filename)  # type: ignore[attr-defined]
