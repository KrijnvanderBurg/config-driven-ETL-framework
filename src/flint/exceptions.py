"""Custom exceptions for the ingestion framework.

This module defines specialized exception classes used throughout the framework
to provide more detailed error information and improved error handling.

Custom exceptions help with:
- Providing more context about errors
- Enabling specific error handling for different error types
- Improving debugging by identifying the exact cause of failures
- Associating exceptions with appropriate exit codes
"""

from typing import Any, TypeVar

from flint.types import ExitCode

K = TypeVar("K")  # Key type


class FlintException(Exception):
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


class ConfigurationError(FlintException):
    """Exception raised for configuration-related errors."""

    def __init__(self, message: str) -> None:
        """Initialize ConfigurationError.

        Args:
            message: The exception message
        """
        super().__init__(message=message, exit_code=ExitCode.CONFIGURATION_ERROR)


class ConfigurationKeyError(ConfigurationError):
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


class ValidationError(FlintException):
    """Exception raised for validation failures."""

    def __init__(self, message: str) -> None:
        """Initialize ValidationError.

        Args:
            message: The exception message
        """
        super().__init__(message=message, exit_code=ExitCode.VALIDATION_ERROR)


class ExtractError(FlintException):
    """Exception raised for data extraction failures."""

    def __init__(self, message: str) -> None:
        """Initialize ExtractError.

        Args:
            message: The exception message
        """
        super().__init__(message=message, exit_code=ExitCode.EXTRACT_ERROR)


class TransformError(FlintException):
    """Exception raised for data transformation failures."""

    def __init__(self, message: str) -> None:
        """Initialize TransformError.

        Args:
            message: The exception message
        """
        super().__init__(message=message, exit_code=ExitCode.TRANSFORM_ERROR)


class LoadError(FlintException):
    """Exception raised for data loading failures."""

    def __init__(self, message: str) -> None:
        """Initialize LoadError.

        Args:
            message: The exception message
        """
        super().__init__(message=message, exit_code=ExitCode.LOAD_ERROR)
