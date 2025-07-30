"""Alert manager for handling notification configurations and trigger.

This module provides the main AlertManager class that orchestrates alert
processing and trigger based on configuration. It serves as the root object
for the alert system, managing templates, channels, and trigger rules.

The AlertManager uses the from_dict classmethod pattern consistent with other
components in the Flint framework to create instances from configuration data.
"""

import logging
from dataclasses import dataclass
from typing import Any, Final, Self

from flint.models import Model
from flint.utils.alert.channels.base import BaseChannel
from flint.utils.alert.channels.email import EmailChannel
from flint.utils.alert.channels.file import FileChannel
from flint.utils.alert.channels.http import HttpChannel
from flint.utils.alert.template import Templates
from flint.utils.alert.triggers import Triggers
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

TEMPLATES: Final[str] = "templates"
CHANNELS: Final[str] = "channels"
triggers: Final[str] = "triggers"
TYPE: Final[str] = "type"
NAME: Final[str] = "name"
CHANNEL: Final[str] = "channel"
FAILURE_HANDLING: Final[str] = "failure_handling"


@dataclass
class Channel(Model):
    """Manager for alert channel configurations and coordination.

    This class manages all configured alert channels and provides a unified
    interface for channel initialization and access. It supports multiple
    channel instances of each type and handles their configuration from dictionary data.

    Attributes:
        channels: Dictionary mapping channel names to their instances
    """

    @classmethod
    def from_dict(cls, dict_: list[dict[str, Any]]) -> Self:
        """Create a Channels instance from a list of channel configurations.

        Args:
            dict_: List of channel configuration dicts. Each channel has 'type', 'name', and 'channel' fields:
                [
                    {"type": "email", "name": "ops-email", "channel": {...}},
                    {"type": "http", "name": "slack-webhook", "channel": {...}},
                    {"type": "file", "name": "error-log", "channel": {...}}
                ]

        Returns:
            A Channels instance configured from the list
        """
        logger.debug("Creating Channels from configuration list")

        channels: dict[str, BaseChannel] = {}
        for channel_config in dict_:
            type = channel_config[TYPE]
            name = channel_config[NAME]
            channel_dict = channel_config[CHANNEL]

            # Create the appropriate channel instance
            channel: BaseChannel
            if type == "email":
                channel = EmailChannel.from_dict(channel_dict)
            elif type == "http":
                channel = HttpChannel.from_dict(channel_dict)
            elif type == "file":
                channel = FileChannel.from_dict(channel_dict)
            else:
                raise ValueError(f"Unknown channel type: {type}")

            channels[name] = channel
            logger.debug("Added %s channel '%s' to configuration", type, name)

        return cls(channels=channels)


@dataclass
class AlertManager(Model):
    """Main alert manager that coordinates alert processing and trigger.

    This class serves as the root object for the alert system, managing the
    configuration and coordination of templates, channels, and trigger rules.
    It implements the Model interface to support configuration-driven initialization.

    Attributes:
        templates: Template configuration for formatting alert messages
        channels: Channel manager for handling different notification channels
        triggers: Rules for determining which channels to use for specific alerts
    """

    templates: Templates
    channels: Channel
    triggers: Triggers

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create an AlertManager instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing alert configuration with keys:
                  - templates: Template configuration for message formatting
                  - channels: Channel configurations for notifications
                  - triggers: Rules for alert trigger

        Returns:
            An AlertManager instance configured from the dictionary

        Raises:
            ConfigurationKeyError: If required configuration keys are missing

        Examples:
            >>> config = {
            ...     "templates": {...},
            ...     "channels": {...},
            ...     "triggers": [...]
            ... }
            >>> manager = AlertManager.from_dict(config)
        """
        logger.debug("Creating AlertManager from configuration dictionary")

        templates = Templates.from_dict(dict_[TEMPLATES])
        channels = Channel.from_dict(dict_[CHANNELS])
        triggers = Triggers.from_dict(dict_[triggers])

        return cls(templates=templates, channels=channels, triggers=triggers)

    def send_alert(self, message: str, title: str | None = None) -> None:
        """Send an alert to all channels as defined by enabled trigger rules.

        Args:
            message: The alert message to send.
            title: Optional alert title.
        """
        for rule in self.triggers.rules:
            if not rule.enabled:
                continue
            for channel_name in rule.channel_names:
                # Find the channel by name
                if channel_name in self.channels.channels:
                    # Send alert through the channel instance
                    self.channels.channels[channel_name].send_alert(message, title)
