"""Unit tests for logging utility functions."""

import pytest

from flint.utils.logger import bind_context, clear_context, get_logger, set_logger


class TestSetLogger:
    """Test set_logger function."""

    def test_creates_logger_with_default_name(self) -> None:
        """Test that set_logger creates a logger instance."""
        # Act
        logger = set_logger("test_logger")

        # Assert
        assert logger is not None
        logger.info("test message")

    def test_accepts_custom_log_level(self) -> None:
        """Test set_logger accepts custom log level parameter."""
        # Act
        logger = set_logger("test_custom_level", level="DEBUG")

        # Assert
        assert logger is not None
        logger.debug("debug message")

    def test_respects_flint_log_level_env_variable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that set_logger uses FLINT_LOG_LEVEL environment variable."""
        # Arrange
        monkeypatch.setenv("FLINT_LOG_LEVEL", "DEBUG")
        monkeypatch.delenv("LOG_LEVEL", raising=False)

        # Act
        logger = set_logger("test_logger_env1")

        # Assert
        assert logger is not None
        logger.debug("debug message")

    def test_falls_back_to_log_level_env_variable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that set_logger falls back to LOG_LEVEL when FLINT_LOG_LEVEL is not set."""
        # Arrange
        monkeypatch.delenv("FLINT_LOG_LEVEL", raising=False)
        monkeypatch.setenv("LOG_LEVEL", "WARNING")

        # Act
        logger = set_logger("test_logger_env2")

        # Assert
        assert logger is not None
        logger.warning("warning message")

    def test_flint_log_level_takes_precedence_over_log_level(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that FLINT_LOG_LEVEL takes precedence over LOG_LEVEL."""
        # Arrange
        monkeypatch.setenv("FLINT_LOG_LEVEL", "WARNING")
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")

        # Act
        logger = set_logger("test_logger_priority")

        # Assert
        assert logger is not None
        logger.warning("warning message")


class TestGetLogger:
    """Test get_logger function."""

    def test_returns_logger_instance(self) -> None:
        """Test get_logger returns a logger that can log messages."""
        # Act
        logger = get_logger("test_logger")

        # Assert
        assert logger is not None
        logger.info("test message")


class TestContextBinding:
    """Test context binding and clearing functions."""

    def test_bind_context_adds_context_variables(self) -> None:
        """Test that bind_context adds context variables to subsequent logs."""
        # Arrange
        logger = set_logger("test_context")

        # Act
        bind_context(user_id="123", request_id="abc")
        logger.info("test message with context")

        # Cleanup
        clear_context()

    def test_clear_context_removes_bound_variables(self) -> None:
        """Test that clear_context removes all bound context variables."""
        # Arrange
        bind_context(user_id="456", request_id="def")

        # Act
        clear_context()

        # Assert
        logger = get_logger("test_clear")
        logger.info("test message after clear")
