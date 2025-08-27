"""Channel wrapper for alert channel configuration.

This module defines the Channel class that represents a complete channel
configuration including type, name, config, and failure handling. This
maps directly to the JSON structure.
"""

import logging
from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import FlintConfigurationKeyError
from flint.job.models import Model
from flint.utils.alert.channels import EmailConfig, FileConfig, HttpConfig
from flint.utils.alert.channels.base import BaseConfig
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

TYPE: Final[str] = "type"
NAME: Final[str] = "name"
CONFIG: Final[str] = "config"


@dataclass
class AlertChannel(Model):
    """Channel configuration wrapper that maps directly to JSON structure.

    This class represents a complete channel configuration including the
    channel type, name, configuration, and failure handling settings.

    Attributes:
        type: The type of channel (email, http, file)
        name: The unique name of the channel
        config: The actual channel implementation instance
        retry: Failure handling configuration
    """

    type: str
    name: str
    config: BaseConfig

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a Channel instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing channel configuration with keys:
                  - type: Channel type (email, http, file)
                  - name: Channel name
                  - config: Channel-specific configuration
                  - retry: Failure handling configuration

        Returns:
            A Channel instance configured from the dictionary

        Raises:
            FlintConfigurationKeyError: If required configuration keys are missing
            ValueError: If an unknown channel type is specified

        Examples:
            >>> config = {
            ...     "type": "email",
            ...     "name": "custom-email-alert-name",
            ...     "config": {...},
            ...     "retry": {...}
            ... }
            >>> channel = Channel.from_dict(config)
        """
        logger.debug("Creating Channel from configuration dictionary")

        try:
            type_ = dict_[TYPE]
            name = dict_[NAME]
            config_dict = dict_[CONFIG]
        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        # Create the appropriate channel instance
        config: BaseConfig
        if type_ == "email":
            config = EmailConfig.from_dict(config_dict)
        elif type_ == "http":
            config = HttpConfig.from_dict(config_dict)
        elif type_ == "file":
            config = FileConfig.from_dict(config_dict)
        else:
            raise ValueError(f"Unknown channel type: {type_}")

        return cls(
            type=type_,
            name=name,
            config=config,
        )

    def send_alert(self, title: str, body: str) -> None:
        """Send an alert message through this channel.

        Args:
            title: The alert title.
            body: The alert message to send.
        """
        logger.debug("Sending alert through channel: %s", self.name)
        self.config.alert(title=title, body=body)
        logger.info("Alert sent through %s channel", self.name)
