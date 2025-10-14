"""Trigger rules for alert channel selection and filtering.

This module defines the trigger rules system that determines which channels
should receive specific alerts based on flexible rule conditions.

The trigger system supports composable rule evaluation with various criteria
to enable sophisticated alert trigger logic using the same pattern as
transform functions.
"""

import logging

from pydantic import Field
from samara import BaseModel
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class AlertTemplate(BaseModel):
    """Configuration for alert message templates and formatting.

    This class manages the template configuration for formatting alert messages,
    including prefixes and suffixes for both titles and message content.

    Attributes:
        prepend_title: Text to prepend to alert titles
        append_title: Text to append to alert titles
        prepend_body: Text to prepend to alert messages
        append_body: Text to append to alert messages
    """

    prepend_title: str = Field(..., description="Text to prepend to alert titles")
    append_title: str = Field(..., description="Text to append to alert titles")
    prepend_body: str = Field(..., description="Text to prepend to alert messages")
    append_body: str = Field(..., description="Text to append to alert messages")

    def format_body(self, message: str) -> str:
        """Format a message with prepend and append templates.

        Args:
            message: The raw message to format

        Returns:
            The formatted message with templates applied
        """
        return f"{self.prepend_body}{message}{self.append_body}"

    def format_title(self, title: str) -> str:
        """Format a title with prepend and append templates.

        Args:
            title: The raw title to format

        Returns:
            The formatted title with templates applied
        """
        return f"{self.prepend_title}{title}{self.append_title}"
