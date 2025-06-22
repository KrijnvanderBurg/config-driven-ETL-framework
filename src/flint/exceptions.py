"""Custom exceptions for the ingestion framework.

This module defines specialized exception classes used throughout the framework
to provide more detailed error information and improved error handling.

Custom exceptions help with:
- Providing more context about errors
- Enabling specific error handling for different error types
- Improving debugging by identifying the exact cause of failures
"""

from typing import Any, TypeVar

K = TypeVar("K")  # Key type


class DictKeyError(KeyError):
    """Exception raised when a key is not found in a dictionary.

    This exception extends KeyError to provide a more informative error message
    that includes not just the missing key but also lists all available keys
    in the dictionary, making debugging easier.

    Attributes:
        key: The key that was not found
        dict_: The dictionary that was being accessed
    """

    def __init__(self, key: K, dict_: dict[K, Any]) -> None:
        """
        Initialize DictKeyError with the missing key and the dictionary.

        Args:
            key: The key that was not found
            dict_: The dictionary that was being accessed
        """
        self.key = key
        self.dict_ = dict_
        super().__init__(f"Key '{key}' not found. Available keys: {list(dict_.keys())}")
