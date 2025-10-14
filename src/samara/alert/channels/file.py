"""File channel for writing alert messages to files.

This module implements the file alert channel that writes alerts
to log files or other file destinations. It supports configurable file
paths and failure handling for file system operations.

The FileAlertChannel follows the Samara framework patterns for configuration-driven
initialization and implements the ChannelModel interface.
"""

import logging
from pathlib import Path
from typing import Literal

from pydantic import Field
from samara.alert.channels.base import ChannelModel
from samara.utils.logger import get_logger
from typing_extensions import override

logger: logging.Logger = get_logger(__name__)


class FileChannel(ChannelModel):
    """File alert channel for file-based alerts.

    This class implements file alerting functionality for writing alerts
    to log files or other file destinations. It supports configurable file
    paths and handles file system errors appropriately.

    Attributes:
        channel_type: Always "file" for file channels
        file_path: Path to the file where alerts should be written
    """

    channel_type: Literal["file"] = Field(..., description="Channel type discriminator")
    file_path: Path = Field(..., description="Path to the file where alerts should be written")

    @override
    def _alert(self, title: str, body: str) -> None:
        """Send an alert message to a file.

        Appends the alert to the configured file with timestamp, creating it if it does not exist.

        Args:
            title: The alert title.
            body: The alert message content.

        Raises:
            OSError: If writing to the file fails.
        """
        try:
            with open(self.file_path, "a", encoding="utf-8") as file:
                file.write(f"{title}: {body}\n")
            logger.info("Alert written to file: %s", self.file_path)
        except OSError as e:
            logger.error("Failed to write alert to file %s: %s", self.file_path, e)
            raise
