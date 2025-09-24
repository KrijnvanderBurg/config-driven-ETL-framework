"""Custom exceptions for the ingestion framework.

This module defines specialized exception classes used throughout the framework
to provide more detailed error information and improved error handling.

Custom exceptions help with:
- Providing more context about errors
- Enabling specific error handling for different error types
- Improving debugging by identifying the exact cause of failures
- Associating exceptions with appropriate exit codes
"""

import enum
from typing import Any, TypeVar

K = TypeVar("K")  # Key type


class ExitCode(enum.IntEnum):
    """Exit codes for the application.

    These codes follow common Unix/Linux conventions:
    - 0: Success
    - 1-63: Application-specific error codes
    - 64-127: Command-specific error codes

    References:
        - https://tldp.org/LDP/abs/html/exitcodes.html
        - https://www.freebsd.org/cgi/man.cgi?query=sysexits&sektion=3
    """

    SUCCESS = 0
    INVALID_ARGUMENTS = 10
    IO_ERROR = 20
    CONFIGURATION_ERROR = 30
    ALERT_CONFIGURATION_ERROR = 31
    RUNTIME_CONFIGURATION_ERROR = 32
    VALIDATION_ERROR = 40
    ALERT_TEST_ERROR = 41
    JOB_ERROR = 50
    KEYBOARD_INTERRUPT = 98
    UNEXPECTED_ERROR = 99


class FlintError(Exception):
    """Base exception for all Flint-specific exceptions.

    Provides common functionality for all Flint exceptions, including
    association with an exit code. Use with Python's built-in exception
    chaining by raising with the `from` keyword.

    The exit_code attribute serves two key purposes:
    1. It allows the CLI to map exceptions to appropriate system exit codes
    2. It provides semantic meaning to different error types

    This design separates error detection (exceptions) from error handling (exit codes),
    following the separation of concerns principle.

    Attributes:
        exit_code: The exit code associated with this exception

    Example:
        ```python
        try:
            # Some operation that might fail
            process_data(file_path)
        except IOError as e:
            # Chain the exception to preserve the original cause
            raise ConfigurationError(f"Cannot process configuration: {file_path}") from e
        ```
    """

    def __init__(self, message: str, exit_code: ExitCode) -> None:
        """Initialize FlintException.

        Args:
            message: The exception message
            exit_code: The exit code associated with this exception
        """
        self.exit_code = exit_code
        super().__init__(message)


class FlintIOError(FlintError):
    """Exception raised for I/O errors."""

    def __init__(self, message: str) -> None:
        """Initialize ValidationError.

        Args:
            message: The exception message
        """
        super().__init__(message=message, exit_code=ExitCode.IO_ERROR)


class FlintAlertConfigurationError(FlintError):
    """Exception raised for configuration-related errors."""

    def __init__(self, message: str) -> None:
        """Initialize ConfigurationError.

        Args:
            message: The exception message
        """
        super().__init__(message=message, exit_code=ExitCode.CONFIGURATION_ERROR)


class FlintRuntimeConfigurationError(FlintError):
    """Exception raised for configuration-related errors."""

    def __init__(self, message: str) -> None:
        """Initialize ConfigurationError.

        Args:
            message: The exception message
        """
        super().__init__(message=message, exit_code=ExitCode.CONFIGURATION_ERROR)


class FlintConfigurationKeyError(FlintRuntimeConfigurationError):
    """Exception raised when a key is missing from configuration dictionaries.

    This exception provides more detailed context about missing keys in configuration
    dictionaries, showing all available keys to help diagnose configuration issues.

    Attributes:
        key: The key that was not found
        available_keys: List of keys that are available in the dictionary
    """

    def __init__(self, key: K, dict_: dict[K, Any]) -> None:
        """Initialize ConfigurationKeyError with the missing key and available keys.

        Args:
            key: The key that was not found
            dict_: The dictionary that was being accessed
        """
        self.key = key
        self.available_keys = list(dict_.keys())
        super().__init__(f"Missing configuration key: '{key}'. Available keys: {self.available_keys}")


class FlintValidationError(FlintError):
    """Exception raised for validation failures."""

    def __init__(self, message: str) -> None:
        """Initialize ValidationError.

        Args:
            message: The exception message
        """
        super().__init__(message=message, exit_code=ExitCode.VALIDATION_ERROR)


class FlintAlertTestError(FlintError):
    """Exception raised for validation failures."""

    def __init__(self, message: str) -> None:
        """Initialize ValidationError.

        Args:
            message: The exception message
        """
        super().__init__(message=message, exit_code=ExitCode.ALERT_TEST_ERROR)


class FlintJobError(FlintError):
    """Exception raised for errors related to the ETL process."""

    def __init__(self, message: str) -> None:
        """Initialize E.

        Args:
            message: The exception message
        """
        super().__init__(message=message, exit_code=ExitCode.JOB_ERROR)
