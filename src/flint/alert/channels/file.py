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

from flint.exceptions import FlintConfigurationKeyError
from flint.utils.alert.channels.base import BaseConfig
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
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(file_path=file_path)

    def _alert(self, title: str, body: str) -> None:
        """Send an alert message to a file.

        Appends the alert to the configured file, creating it if it does not exist.

        Args:
            body: The alert message content.
            title: The alert title.

        Raises:
            OSError: If writing to the file fails.
        """
        try:
            with open(self.file_path, "a", encoding="utf-8") as file:
                file.write(f"{title} - {body}\n")
            logger.info("Alert written to file: %s", self.file_path)
        except OSError as exc:
            logger.error("Failed to write alert to file %s: %s", self.file_path, exc)
            raise
