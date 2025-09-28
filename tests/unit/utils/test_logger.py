# """unit tests for logging utility functions."""

# from logging import DEBUG, WARNING, Logger

# from pytest import MonkeyPatch

# from flint.utils.logger import get_logger, set_logger


# class TestLogger:
#     """Unit tests for the logging utility functions."""

#     def test_set_logger_creates_logger(self) -> None:
#         """
#         Test set_logger returns a configured logger instance.

#         This test checks if the set_logger function correctly creates and
#         configures a logger instance with the given name.
#         """
#         # Act
#         logger = set_logger("test_logger")

#         # Assert
#         assert isinstance(logger, Logger)
#         assert logger.name == "test_logger"

#     def test_get_logger(self) -> None:
#         """
#         Test get_logger returns the correct logger instance.

#         This test verifies that the get_logger function returns a logger
#         instance that has been previously configured with set_logger.
#         """
#         # Act
#         logger = get_logger("test_logger")

#         # Assert
#         assert isinstance(logger, Logger)
#         assert logger.name == "test_logger"

#     def test_set_logger_file_output(self) -> None:
#         """
#         Test set_logger writes logs to the specified file.

#         This test ensures that the logs are being written to the correct
#         file when a logger is created with set_logger.
#         """
#         # Act
#         source_logger = set_logger("test_logger")
#         rotating_file_handler = source_logger.handlers[0]  # the first handler must be a rotating file handler
#         expected_log_filename = "flint.log"

#         # Assert
#         assert rotating_file_handler.baseFilename.endswith(expected_log_filename)  # type: ignore[attr-defined]

#     def test_set_logger_respects_flint_log_level(self, monkeypatch: MonkeyPatch) -> None:
#         """
#         Test that set_logger sets the log level from FLINT_LOG_LEVEL.
#         """
#         monkeypatch.setenv("FLINT_LOG_LEVEL", "DEBUG")
#         monkeypatch.delenv("LOG_LEVEL", raising=False)
#         logger = set_logger("test_logger_env1")
#         for handler in logger.handlers:
#             assert handler.level == DEBUG

#     def test_set_logger_respects_log_level(self, monkeypatch: MonkeyPatch) -> None:
#         """
#         Test that set_logger sets the log level from LOG_LEVEL if FLINT_LOG_LEVEL is not set.
#         """
#         monkeypatch.delenv("FLINT_LOG_LEVEL", raising=False)
#         monkeypatch.setenv("LOG_LEVEL", "WARNING")
#         logger = set_logger("test_logger_env2")
#         for handler in logger.handlers:
#             assert handler.level == WARNING

#     def test_set_logger_env_priority(self, monkeypatch: MonkeyPatch) -> None:
#         """
#         Test that FLINT_LOG_LEVEL takes precedence over LOG_LEVEL.
#         """
#         monkeypatch.setenv("FLINT_LOG_LEVEL", "WARNING")
#         monkeypatch.setenv("LOG_LEVEL", "DEBUG")
#         logger = set_logger("test_logger_priority")
#         for handler in logger.handlers:
#             assert handler.level == WARNING
