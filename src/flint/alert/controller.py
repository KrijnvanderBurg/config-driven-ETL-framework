"""Alert manager for handling notification configurations and trigger.

This module provides the main AlertController class that orchestrates alert
processing and trigger based on configuration. It serves as the root object
for the alert system, managing templates, channels, and trigger rules.

The AlertController uses the from_dict classmethod pattern consistent with other
components in the Flint framework to create instances from configuration data.
"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Self

from flint.alert.channel import AlertChannel
from flint.alert.trigger import AlertTrigger
from flint.etl.models import Model
from flint.utils.file import FileHandlerContext
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

ALERT: Final[str] = "alert"
TRIGGERS: Final[str] = "triggers"
CHANNELS: Final[str] = "channels"


@dataclass
class AlertController(Model):
    """Main alert manager that coordinates alert processing and triggering.

    This class serves as the root object for the alert system, managing the
    configuration and coordination of templates, channels, and trigger rules.
    It implements the Model interface to support configuration-driven initialization.

    Attributes:
        triggers: Rules for determining which channels to use for specific alerts
        channels: List of alert channels for handling different alert destinations
    """

    triggers: list[AlertTrigger]
    channels: list[AlertChannel]

    @classmethod
    def from_file(cls, filepath: Path) -> Self:
        """Create an AlertController instance from a configuration file.

        Loads and parses a configuration file to create an AlertController instance.

        Args:
            filepath: Path to the configuration file.

        Returns:
            A fully configured AlertController instance.
        """
        logger.info("Creating AlertController from file: %s", filepath)

        handler = FileHandlerContext.from_filepath(filepath=filepath)
        dict_: dict[str, Any] = handler.read()

        alert = cls.from_dict(dict_=dict_)
        logger.info("Successfully created AlertController from JSON file: %s", filepath)
        return alert

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create an AlertController instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing alert configuration with keys:
                  - channels: Channel configurations for notifications
                  - triggers: Rules for alert trigger

        Returns:
            An AlertController instance configured from the dictionary

        Raises:
            FlintConfigurationKeyError: If required configuration keys are missing
            ValueError: If an unknown channel type is specified

        Examples:
            >>> config = {
            ...     "channels": {...},
            ...     "triggers": [...]
            ... }
            >>> manager = AlertController.from_dict(config)
        """
        logger.debug("Creating AlertController from configuration dictionary")
        alert_dict = dict_[ALERT]

        triggers: list[AlertTrigger] = []
        for trigger_dict in alert_dict[TRIGGERS]:
            trigger = AlertTrigger.from_dict(trigger_dict)
            triggers.append(trigger)

        channels: list[AlertChannel] = []
        for channel_config in alert_dict[CHANNELS]:
            channel = AlertChannel.from_dict(channel_config)
            channels.append(channel)
            logger.debug("Added %s channel '%s' to configuration", channel.type, channel.name)

        return cls(triggers=triggers, channels=channels)

    def evaluate_trigger_and_alert(self, title: str, body: str, exception: Exception) -> None:
        """Process and send an alert to all channels as defined by enabled trigger rules.

        Args:
            title: The alert title.
            body: The alert message to send.
            exception: The exception that triggered the alert.
        """

        for trigger in self.triggers:
            if trigger.should_fire(exception=exception):
                logger.debug("Trigger '%s' conditions met; processing alert", trigger.name)

                formatted_title = trigger.template.format_title(title)
                formatted_body = trigger.template.format_body(body)

                for channel_name in trigger.channel_names:
                    # Find the channel by name
                    for channel in self.channels:
                        if channel.name == channel_name:
                            formatted_title = trigger.template.format_title(title)
                            formatted_body = trigger.template.format_body(body)

                            # Send alert through the channel instance
                            channel.trigger(title=formatted_title, body=formatted_body)
                            logger.debug("Sent alert to channel '%s'", channel.name)
                            break
