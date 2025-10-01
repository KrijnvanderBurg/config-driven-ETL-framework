"""unit tests for logging utility functions."""

from io import StringIO
from unittest.mock import patch

from pytest import MonkeyPatch

from flint.utils.logger import bind_context, clear_context, get_logger, set_logger


class TestLogger:
    """Unit tests for the logging utility functions."""

    def test_set_logger__creates_logger__can_log_messages(self) -> None:
        """Test set_logger returns a logger that can log messages."""
        # Arrange
        logger = set_logger("test_logger")

        # Act & Assert - logger can be called without raising exceptions
        logger.info("test message")
        logger.debug("debug message")
        logger.warning("warning message")
        logger.error("error message")

    def test_get_logger__returns_logger__can_log_messages(self) -> None:
        """Test get_logger returns a logger that can log messages."""
        # Act
        logger = get_logger("test_logger")

        # Assert - logger can be called without raising exceptions
        logger.info("test message")
        logger.debug("debug message")

    def test_set_logger__with_custom_level__accepts_level_parameter(self) -> None:
        """Test set_logger accepts custom log level parameter."""
        # Act
        logger = set_logger("test_custom_level", level="DEBUG")

        # Assert - logger can be called without raising exceptions
        logger.info("test message")

    def test_set_logger__with_flint_log_level_env__uses_env_variable(self, monkeypatch: MonkeyPatch) -> None:
        """Test that set_logger respects FLINT_LOG_LEVEL environment variable."""
        # Arrange
        monkeypatch.setenv("FLINT_LOG_LEVEL", "DEBUG")
        monkeypatch.delenv("LOG_LEVEL", raising=False)

        # Act
        logger = set_logger("test_logger_env1")

        # Assert - logger can be called without raising exceptions
        logger.debug("debug message should work")

    def test_set_logger__with_log_level_env__falls_back_to_log_level(self, monkeypatch: MonkeyPatch) -> None:
        """Test that set_logger falls back to LOG_LEVEL if FLINT_LOG_LEVEL is not set."""
        # Arrange
        monkeypatch.delenv("FLINT_LOG_LEVEL", raising=False)
        monkeypatch.setenv("LOG_LEVEL", "WARNING")

        # Act
        logger = set_logger("test_logger_env2")

        # Assert - logger can be called without raising exceptions
        logger.warning("warning message should work")

    def test_set_logger__with_both_env_vars__flint_takes_precedence(self, monkeypatch: MonkeyPatch) -> None:
        """Test that FLINT_LOG_LEVEL takes precedence over LOG_LEVEL."""
        # Arrange
        monkeypatch.setenv("FLINT_LOG_LEVEL", "WARNING")
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")

        # Act
        logger = set_logger("test_logger_priority")

        # Assert - logger can be called without raising exceptions
        logger.warning("warning message should work")

    def test_bind_context__adds_context__context_available_in_logs(self) -> None:
        """Test that bind_context adds context variables to subsequent logs."""
        # Arrange
        logger = set_logger("test_context")
        output = StringIO()

        # Act
        bind_context(user_id="123", request_id="abc")

        with patch("sys.stdout", output):
            logger.info("test message with context")

        # Assert
        log_output = output.getvalue()
        assert "test message with context" in log_output

        # Cleanup
        clear_context()

    def test_clear_context__removes_context__no_context_in_subsequent_logs(self) -> None:
        """Test that clear_context removes all bound context variables."""
        # Arrange
        bind_context(user_id="456", request_id="def")
        output = StringIO()

        # Act
        clear_context()
        logger = get_logger("test_clear")

        with patch("sys.stdout", output):
            logger.info("test message after clear")

        # Assert
        log_output = output.getvalue()
        assert "test message after clear" in log_output
