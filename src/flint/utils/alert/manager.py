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


@dataclass
class Channels(Model):
    """Manager for alert channel configurations and coordination.

    This class manages all configured alert channels and provides a unified
    interface for channel initialization and access. It supports multiple
    channel instances of each type and handles their configuration from dictionary data.

    Attributes:
        channels: Dictionary mapping channel names to channel instances
    """

    channels: dict[str, BaseChannel]

    @classmethod
    def from_dict(cls, dict_: Any) -> Self:
        """Create a ChannelManager instance from a dictionary or list configuration.

        Args:
            dict_: Either a dictionary containing a list of channel configurations under 'channels' key,
                  or directly a list of channel configurations.
                  Each channel has 'type', 'name', and 'config' fields:
                  [
                      {"type": "email", "name": "ops-email", "config": {...}},
                      {"type": "http", "name": "slack-webhook", "config": {...}},
                      {"type": "file", "name": "error-log", "config": {...}}
                  ]

        Returns:
            A ChannelManager instance configured from the dictionary
        """
        logger.debug("Creating ChannelManager from configuration")

        channels: dict[str, BaseChannel] = {}

        # Handle both direct list and dictionary with 'channels' key
        channels_list = dict_["channels"] if "channels" in dict_ else dict_

        for channel_config in channels_list:
            channel_type = channel_config["type"]
            channel_name = channel_config["name"]
            config = channel_config["config"]

            channel: BaseChannel
            if channel_type == "email":
                channel = EmailChannel.from_dict(config)
            elif channel_type == "http":
                channel = HttpChannel.from_dict(config)
            elif channel_type == "file":
                channel = FileChannel.from_dict(config)
            else:
                raise ValueError(f"Unknown channel type: {channel_type} in channel config: {channel_config}")

            channels[channel_name] = channel
            logger.debug("Created %s channel '%s' from configuration", channel_type, channel_name)

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
