"""Template configuration for alert message formatting.

This module defines the Templates dataclass that handles the configuration
and formatting of alert messages. It provides standardized templates for
prepending and appending content to alert titles and messages.

The Templates class follows the Flint framework pattern of using from_dict
classmethods for configuration-driven initialization.
"""

import logging
from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import ConfigurationKeyError
from flint.models import Model
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

PREPEND_TITLE: Final[str] = "prepend_title"
APPEND_TITLE: Final[str] = "append_title"
PREPEND_MESSAGE: Final[str] = "prepend_message"
APPEND_MESSAGE: Final[str] = "append_message"


@dataclass
class Templates(Model):
    """Configuration for alert message templates and formatting.

    This class manages the template configuration for formatting alert messages,
    including prefixes and suffixes for both titles and message content.

    Attributes:
        prepend_title: Text to prepend to alert titles
        append_title: Text to append to alert titles
        prepend_message: Text to prepend to alert messages
        append_message: Text to append to alert messages
    """

    prepend_title: str
    append_title: str
    prepend_message: str
    append_message: str

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a Templates instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing template configuration with keys:
                  - prepend_title: Text to prepend to alert titles
                  - append_title: Text to append to alert titles
                  - prepend_message: Text to prepend to alert messages
                  - append_message: Text to append to alert messages

        Returns:
            A Templates instance configured from the dictionary

        Raises:
            ConfigurationKeyError: If a required key is missing.

        Examples:
            >>> config = {
            ...     "prepend_title": "ETL Pipeline Alert",
            ...     "append_title": "Alert Notification",
            ...     "prepend_message": "Attention: ETL Pipeline Alert",
            ...     "append_message": "Please take necessary actions."
            ... }
            >>> templates = Templates.from_dict(config)
        """
        logger.debug("Creating Templates from configuration dictionary")

        try:
            prepend_title = dict_[PREPEND_TITLE]
            append_title = dict_[APPEND_TITLE]
            prepend_message = dict_[PREPEND_MESSAGE]
            append_message = dict_[APPEND_MESSAGE]
        except KeyError as e:
            raise ConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            prepend_title=prepend_title,
            append_title=append_title,
            prepend_message=prepend_message,
            append_message=append_message,
        )
