"""Alert manager for handling notification configurations and routing.

This module provides the main AlertManager class that orchestrates alert
processing and routing based on configuration. It serves as the root object
for the alert system, managing templates, channels, and routing rules.

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
from flint.utils.alert.routing_rules import RoutingRules
from flint.utils.alert.template import Templates
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

TEMPLATES: Final[str] = "templates"
CHANNELS: Final[str] = "channels"
ROUTING_RULES: Final[str] = "routing_rules"
TYPE: Final[str] = "type"
NAME: Final[str] = "name"
CHANNEL: Final[str] = "channel"
FAILURE_HANDLING: Final[str] = "failure_handling"


@dataclass
class Channels(Model):
    """Manager for alert channel configurations and coordination.

    This class manages all configured alert channels and provides a unified
    interface for channel initialization and access. It supports multiple
    channel instances of each type and handles their configuration from dictionary data.

    Attributes:
        channels: Dictionary mapping channel names to their instances
    """

    channels: dict[str, BaseChannel]

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
            channel_type = channel_config[TYPE]
            channel_name = channel_config[NAME]
            channel_config_dict = channel_config[CHANNEL]

            # Create the appropriate channel instance
            channel: BaseChannel
            if channel_type == "email":
                channel = EmailChannel.from_dict(channel_config_dict)
            elif channel_type == "http":
                channel = HttpChannel.from_dict(channel_config_dict)
            elif channel_type == "file":
                channel = FileChannel.from_dict(channel_config_dict)
            else:
                raise ValueError(f"Unknown channel type: {channel_type}")

            channels[channel_name] = channel
            logger.debug("Added %s channel '%s' to configuration", channel_type, channel_name)

        return cls(channels=channels)


@dataclass
class AlertManager(Model):
    """Main alert manager that coordinates alert processing and routing.

    This class serves as the root object for the alert system, managing the
    configuration and coordination of templates, channels, and routing rules.
    It implements the Model interface to support configuration-driven initialization.

    Attributes:
        templates: Template configuration for formatting alert messages
        channels: Channel manager for handling different notification channels
        routing_rules: Rules for determining which channels to use for specific alerts
    """

    templates: Templates
    channels: Channels
    routing_rules: RoutingRules

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create an AlertManager instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing alert configuration with keys:
                  - templates: Template configuration for message formatting
                  - channels: Channel configurations for notifications
                  - routing_rules: Rules for alert routing

        Returns:
            An AlertManager instance configured from the dictionary

        Raises:
            ConfigurationKeyError: If required configuration keys are missing

        Examples:
            >>> config = {
            ...     "templates": {...},
            ...     "channels": {...},
            ...     "routing_rules": [...]
            ... }
            >>> manager = AlertManager.from_dict(config)
        """
        logger.debug("Creating AlertManager from configuration dictionary")

        templates = Templates.from_dict(dict_[TEMPLATES])
        channels = Channels.from_dict(dict_[CHANNELS])
        routing_rules = RoutingRules.from_dict(dict_[ROUTING_RULES])

        return cls(templates=templates, channels=channels, routing_rules=routing_rules)

    def send_alert(self, message: str, title: str | None = None) -> None:
        """Send an alert to all channels as defined by enabled routing rules.

        Args:
            message: The alert message to send.
            title: Optional alert title.
        """
        for rule in self.routing_rules.rules:
            if not rule.enabled:
                continue
            for channel_name in rule.channel_names:
                # Find the channel by name
                if channel_name in self.channels.channels:
                    # Send alert through the channel instance
                    self.channels.channels[channel_name].send_alert(message, title)
