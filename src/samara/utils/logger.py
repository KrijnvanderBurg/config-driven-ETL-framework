"""Logging utilities for the Samara framework.

This module provides a centralized logging configuration for the Samara framework,
using structlog for structured logging. It integrates seamlessly with OpenTelemetry
for distributed tracing and observability.

When an OTLP (OpenTelemetry Protocol) logs endpoint is configured via environment
variables or settings, all logs are automatically exported with full structured
attributes, enabling powerful querying and analysis in backends like Loki/Grafana,
Datadog, or any OpenTelemetry-compatible observability platform.

Key Features:
    - Centralized structured logging with structlog
    - Automatic OpenTelemetry integration for distributed tracing
    - Rich structured logging attributes for enhanced observability
    - Familiar logging interface with structured context support

Typical Usage:
    >>> from samara.utils.logger import set_logger, get_logger
    >>> set_logger(level="INFO")
    >>> logger = get_logger(__name__)
    >>> logger.info("Application started", version="1.0.0")
"""

import logging

import structlog


def set_logger(level: str = "INFO") -> None:
    """Configure structlog with standard library integration.

    This function initializes structlog with processors that provide structured
    logging with consistent formatting. It integrates with Python's standard
    logging library to ensure third-party libraries and OpenTelemetry handlers
    continue to work correctly.

    The configuration includes:
    - Context variables merging for request-scoped logging
    - Automatic log level and logger name addition
    - ISO-formatted timestamps
    - Stack trace and exception formatting
    - Integration with Python's logging module for OTLP export

    Args:
        level: The logging level threshold as a string. Valid values are:
            - "DEBUG": Detailed diagnostic information (most verbose)
            - "INFO": Confirmation that things are working as expected
            - "WARNING": Indication of potential issues
            - "ERROR": Serious problems that prevented a function from completing
            - "CRITICAL": Very serious errors that may prevent program execution
            Defaults to "INFO" if not specified.

    Note:
        - This configures both structlog and the root Python logger.
        - When OpenTelemetry is configured, all logs will automatically include
          trace context (trace_id, span_id) for correlation with distributed traces.
        - The configuration can be called multiple times for reconfiguration.

    Examples:
        Basic usage with INFO level:
            >>> from samara.utils.logger import set_logger, get_logger
            >>> set_logger(level="INFO")
            >>> logger = get_logger(__name__)
            >>> logger.info("Application initialized successfully")

        Enable debug logging for development:
            >>> set_logger(level="DEBUG")
            >>> logger = get_logger(__name__)
            >>> logger.debug("Detailed variable state", x=x)

        Configure from environment variable:
            >>> import os
            >>> log_level = os.getenv("LOG_LEVEL", "INFO")
            >>> set_logger(level=log_level)

    See Also:
        get_logger: Create logger instances for specific modules
    """
    # Configure standard logging for third-party libraries and OTLP handler
    logging.basicConfig(
        format="%(message)s",
        level=level,
        force=True,
    )

    # Configure structlog
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Create and return a structlog logger instance for the specified module or component.

    This function creates a structlog BoundLogger with the given name, following
    Python's hierarchical logging convention. The logger inherits configuration
    from `set_logger()`, including log level and processors.

    When OpenTelemetry is properly configured with an OTLP logs endpoint, all logs
    emitted through this logger are automatically enriched with trace context and
    exported to the configured observability backend (e.g., Grafana Loki, Datadog,
    Jaeger, or any OpenTelemetry-compatible system).

    Args:
        name: The name of the logger, which should follow Python's hierarchical
            naming convention. Typically, use `__name__` to automatically use the
            module's fully qualified name (e.g., "samara.workflow.controller").
            This enables fine-grained control over logging configuration by module.

    Returns:
        structlog.stdlib.BoundLogger: A configured structlog logger instance that:
            - Inherits the log level from the root logger
            - Automatically includes OpenTelemetry trace context when available
            - Supports all standard logging methods (debug, info, warning, error, critical)
            - Provides structured logging with keyword arguments
            - Exports logs to OTLP endpoint when configured

    Note:
        - Always call `set_logger()` before using `get_logger()` to ensure proper
          configuration of structlog processors and levels.
        - Logger names are hierarchical: "samara.workflow" is the parent of
          "samara.workflow.controller", allowing granular level control.
        - The returned logger is cached by structlog, so multiple calls with the
          same name return the same logger instance.
        - When OpenTelemetry tracing is active, logs automatically include
          trace_id and span_id fields for correlation with distributed traces.

    Examples:
        Standard usage with module name:
            >>> from samara.utils.logger import get_logger
            >>> logger = get_logger(__name__)
            >>> logger.info("Processing started")

        Using different log levels:
            >>> logger = get_logger(__name__)
            >>> logger.debug("Variable state", count=count)
            >>> logger.info("Operation completed successfully")
            >>> logger.warning("Deprecated feature used")
            >>> logger.error("Failed to process record", exc_info=True)

        Adding structured context as keyword arguments:
            >>> logger = get_logger(__name__)
            >>> logger.info(
            ...     "User action performed",
            ...     user_id=123,
            ...     action="login",
            ...     ip="192.168.1.1"
            ... )

        Creating logger for specific component:
            >>> logger = get_logger("samara.workflow.spark_executor")
            >>> logger.info("Spark job submitted", job_id="job-001")

    See Also:
        set_logger: Configure structlog before creating loggers
        structlog.stdlib.BoundLogger: Structlog's BoundLogger documentation
    """
    return structlog.get_logger(name)
