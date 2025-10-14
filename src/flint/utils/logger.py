"""Structured logging utilities for the Samara framework."""

import logging
import os
from typing import Any

import structlog


def set_logger(name: str | None = None, level: str | None = None) -> structlog.BoundLogger:
    """Configure and return a structured logger with console output only."""
    # Get log level from environment variables with fallback
    log_level = level or os.environ.get("FLINT_LOG_LEVEL") or os.environ.get("LOG_LEVEL") or "INFO"

    # Configure structlog only once
    if not structlog.is_configured():
        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.processors.TimeStamper(fmt="ISO"),
                structlog.processors.add_log_level,
                structlog.dev.ConsoleRenderer(colors=True),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(log_level),
            logger_factory=structlog.PrintLoggerFactory(),
            cache_logger_on_first_use=True,
        )

    return structlog.get_logger(name)


def get_logger(name: str) -> logging.Logger:
    """Get a structured logger instance."""
    return structlog.get_logger(name)


def bind_context(**context: Any) -> None:
    """Bind context variables to all subsequent log messages."""
    structlog.contextvars.bind_contextvars(**context)


def clear_context() -> None:
    """Clear all bound context variables."""
    structlog.contextvars.clear_contextvars()
