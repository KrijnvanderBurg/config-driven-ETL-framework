"""Alert manager for handling notification configurations and trigger.

This module provides the main AlertManager class that orchestrates alert
processing and trigger based on configuration. It serves as the root object
for the alert system, managing templates, channels, and trigger rules.

The AlertManager uses the from_dict classmethod pattern consistent with other
components in the Flint framework to create instances from configuration data.
"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Self

from flint.job.models import Model
from flint.utils.alert.channel import AlertChannel
from flint.utils.alert.trigger import AlertTrigger
from flint.utils.file import FileHandlerContext
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

ALERT: Final[str] = "alert"
CHANNELS: Final[str] = "channels"
TRIGGERS: Final[str] = "triggers"


@dataclass
class Alert(Model):
    """Main alert manager that coordinates alert processing and trigger.

    This class serves as the root object for the alert system, managing the
    configuration and coordination of templates, channels, and trigger rules.
    It implements the Model interface to support configuration-driven initialization.

    Attributes:
        channels: Channel manager for handling different notification channels
        triggers: Rules for determining which channels to use for specific alerts
    """

    channels: list[AlertChannel]
    triggers: list[AlertTrigger]

    @classmethod
    def from_file(cls, filepath: Path) -> Self:
        """Create a Alert instance from a configuration file.

        Loads and parses a configuration file to create a Alert instance.

        Args:
            filepath: Path to the configuration file.

        Returns:
            A fully configured Alert instance.
        """
        logger.info("Creating Alert from file: %s", filepath)

        handler = FileHandlerContext.from_filepath(filepath=filepath)
        dict_: dict[str, Any] = handler.read()

        alert = cls.from_dict(dict_=dict_)
        logger.info("Successfully created Alert from JSON file: %s", filepath)
        return alert

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create an AlertManager instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing alert configuration with keys:
                  - channels: Channel configurations for notifications
                  - triggers: Rules for alert trigger

        Returns:
            An AlertManager instance configured from the dictionary

        Raises:
            FlintConfigurationKeyError: If required configuration keys are missing
            ValueError: If an unknown channel type is specified

        Examples:
            >>> config = {
            ...     "channels": {...},
            ...     "triggers": [...]
            ... }
            >>> manager = AlertManager.from_dict(config)
        """
        logger.debug("Creating AlertManager from configuration dictionary")
        alert_dict = dict_[ALERT]

        channels: list[AlertChannel] = []
        for channel_config in alert_dict[CHANNELS]:
            channel = AlertChannel.from_dict(channel_config)
            channels.append(channel)
            logger.debug("Added %s channel '%s' to configuration", channel.type, channel.name)

        triggers: list[AlertTrigger] = []
        for trigger_dict in alert_dict[TRIGGERS]:
            trigger = AlertTrigger.from_dict(trigger_dict)
            triggers.append(trigger)

        return cls(channels=channels, triggers=triggers)

    def trigger_if_conditions_met(self, title: str, body: str, exception: Exception) -> None:
        """Send an alert to all channels as defined by enabled trigger rules.

        Args:
            title: The alert title.
            body: The alert message to send.
            exception: The exception that triggered the alert.
        """

        for trigger in self.triggers:
            if trigger.is_fire(exception=exception):
                logger.debug("Trigger '%s' conditions met; processing alert", trigger.name)

                formatted_title = trigger.template.format_title(title)
                formatted_body = trigger.template.format_body(body)

                for channel_name in trigger.channel_names:
                    # Find the channel by name
                    for channel in self.channels:
                        if channel.name == channel_name:
                            # Send alert through the channel instance
                            channel.send_alert(title=formatted_title, body=formatted_body)
                            logger.debug("Sent alert to channel '%s'", channel.name)
                            break
