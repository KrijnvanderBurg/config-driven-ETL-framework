"""File channel for writing alert notifications to files.

This module implements the file alert channel that writes notifications
to log files or other file destinations. It supports configurable file
paths and failure handling for file system operations.

The FileChannel follows the Flint framework patterns for configuration-driven
initialization and implements the BaseChannel interface.
"""

import logging
from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import ConfigurationKeyError
from flint.utils.alert.channels import BaseConfig
from flint.utils.logger import get_logger

FILE_PATH: Final[str] = "file_path"

logger: logging.Logger = get_logger(__name__)


@dataclass
class FileConfig(BaseConfig):
    """File config for file-based notifications.

    This class implements file alerting functionality for writing notifications
    to log files or other file destinations. It supports configurable file
    paths and handles file system errors appropriately.

    Attributes:
        file_path: Path to the file where alerts should be written
        failure_handling: Configuration for handling channel failures and retries
    """

    file_path: str

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a FileChannel instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing file channel configuration with keys:
                  - file_path: Path to the alert log file
                  - failure_handling: Failure handling configuration

        Returns:
            A FileChannel instance configured from the dictionary

        Examples:
            >>> config = {
            ...     "file_path": "alerts.log",
            ...     "failure_handling": {...}
            ... }
            >>> file_channel = FileChannel.from_dict(config)
        """
        logger.debug("Creating FileChannel from configuration dictionary")
        try:
            file_path = dict_[FILE_PATH]
        except KeyError as e:
            raise ConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(file_path=file_path)

    def send_alert(self, message: str, title: str) -> None:
        """Send an alert message to a file (not implemented yet)."""
        raise NotImplementedError("send_alert not implemented for FileChannel.")
