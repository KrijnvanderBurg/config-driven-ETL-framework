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
from flint.utils.alert.channel import AlertChannel
from flint.utils.alert.trigger import AlertTrigger
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

TEMPLATES: Final[str] = "templates"
CHANNELS: Final[str] = "channels"
TRIGGERS: Final[str] = "triggers"


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

    channels: list[AlertChannel]
    triggers: list[AlertTrigger]

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
            ValueError: If an unknown channel type is specified

        Examples:
            >>> config = {
            ...     "templates": {...},
            ...     "channels": {...},
            ...     "triggers": [...]
            ... }
            >>> manager = AlertManager.from_dict(config)
        """
        logger.debug("Creating AlertManager from configuration dictionary")

        channels: list[AlertChannel] = []
        for channel_config in dict_[CHANNELS]:
            channel = AlertChannel.from_dict(channel_config)
            channels.append(channel)
            logger.debug("Added %s channel '%s' to configuration", channel.type, channel.name)

        triggers: list[AlertTrigger] = []
        for trigger_dict in dict_[TRIGGERS]:
            trigger = AlertTrigger.from_dict(trigger_dict)
            triggers.append(trigger)

        return cls(channels=channels, triggers=triggers)

    def send_alert(self, message: str, title: str, exception: Exception) -> None:
        """Send an alert to all channels as defined by enabled trigger rules.

        Args:
            message: The alert message to send.
            title: The alert title.
            exception: The exception that triggered the alert.
        """

        for trigger in self.triggers:
            if trigger.is_fire(exception=exception):
                logger.debug("Trigger '%s' conditions met; processing alert", trigger.name)

                formatted_message = trigger.template.format_message(message)
                formatted_title = trigger.template.format_title(title)

                for channel_name in trigger.channel_names:
                    # Find the channel by name
                    for channel in self.channels:
                        if channel.name == channel_name:
                            # Send alert through the channel instance
                            channel.send_alert(message=formatted_message, title=formatted_title)
                            logger.debug("Sent alert to channel '%s'", channel.name)
                            break
