"""Alert manager for handling notification configurations and trigger.

This module provides the main AlertManager class that orchestrates alert
processing and trigger based on configuration. It serves as the root object
for the alert system, managing templates, channels, and trigger rules.

The AlertManager uses the from_dict classmethod pattern consistent with other
components in the Flint framework to create instances from configuration data.
"""

import logging
from pathlib import Path
from typing import Any, Final, Self

from pydantic import Field, ValidationError

from flint import BaseModel
from flint.alert.channels import channel_union
from flint.alert.trigger import AlertTrigger
from flint.exceptions import FlintAlertConfigurationError, FlintIOError
from flint.utils.file import FileHandlerContext
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

ALERT: Final = "alert"


class AlertController(BaseModel):
    """Main alert manager that coordinates alert processing and triggering.

    This class serves as the root object for the alert system, managing the
    configuration and coordination of templates, channels, and trigger rules.
    It implements the Model interface to support configuration-driven initialization.

    Attributes:
        channels: List of alert channels for handling different alert destinations
        triggers: Rules for determining which channels to use for specific alerts
    """

    channels: list[channel_union] = Field(..., description="List of configured channels")
    triggers: list[AlertTrigger] = Field(..., description="List of alert trigger rules")

    @classmethod
    def from_file(cls, filepath: Path) -> Self:
        """Create an AlertManager instance from a configuration file.

        Loads and parses a configuration file to create an AlertManager instance.

        Args:
            filepath: Path to the configuration file.

        Returns:
            A fully configured AlertManager instance.

        Raises:
            FlintIOError: If there are file I/O related issues (file not found, permission denied, etc.)
            FlintAlertConfigurationError: If there are configuration parsing or validation issues
        """
        logger.info("Creating AlertManager from file: %s", filepath)

        try:
            handler = FileHandlerContext.from_filepath(filepath=filepath)
            dict_: dict[str, Any] = handler.read()
        except (OSError, ValueError) as e:
            logger.error("Failed to read alert configuration file: %s", e)
            raise FlintIOError(f"Cannot load alert configuration from '{filepath}': {e}") from e

        try:
            alert = cls(**dict_[ALERT])
            logger.info("Successfully created AlertManager from configuration file: %s", filepath)
            return alert
        except KeyError as e:
            raise FlintAlertConfigurationError(f"Missing 'alert' section in configuration file '{filepath}'") from e
        except ValidationError as e:
            raise FlintAlertConfigurationError(f"Invalid alert configuration in file '{filepath}': {e}") from e

    def evaluate_trigger_and_alert(self, title: str, body: str, exception: Exception) -> None:
        """Process and send an alert to all channels as defined by enabled trigger rules.

        Args:
            title: The alert title.
            body: The alert message to send.
            exception: The exception that triggered the alert.
        """

        for trigger in self.triggers:
            if trigger.should_fire(exception=exception):
                logger.debug("Trigger '%s' conditions met; processing alert", trigger.id_)

                formatted_title = trigger.template.format_title(title)
                formatted_body = trigger.template.format_body(body)

                for channel_id in trigger.channel_ids:
                    # Find the channel by id
                    for channel in self.channels:
                        if channel.id_ == channel_id:
                            formatted_title = trigger.template.format_title(title)
                            formatted_body = trigger.template.format_body(body)

                            # Send alert through the channel instance
                            channel.alert(title=formatted_title, body=formatted_body)
                            logger.debug("Sent alert to channel '%s'", channel.id_)
                            break
