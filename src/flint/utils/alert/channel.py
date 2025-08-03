"""Channel wrapper for alert channel configuration.

This module defines the Channel class that represents a complete channel
configuration including type, name, config, and failure handling. This
maps directly to the JSON structure.
"""

import logging
from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import ConfigurationKeyError
from flint.models import Model
from flint.utils.alert.channels import BaseConfig, EmailConfig, FileConfig, HttpConfig
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

TYPE: Final[str] = "type"
NAME: Final[str] = "name"
CONFIG: Final[str] = "config"
FAILURE_HANDLING: Final[str] = "failure_handling"


ERROR_ON_ALERT_FAILURE: Final[str] = "error_on_alert_failure"
RETRY_ATTEMPTS: Final[str] = "retry_attempts"
RETRY_DELAY_SECONDS: Final[str] = "retry_delay_seconds"


@dataclass
class FailureHandling(Model):
    """Configuration for handling channel failures and retries.

    This class defines how alert channels should behave when failures occur,
    including retry logic and error escalation settings.

    Attributes:
        error_on_alert_failure: Whether to raise errors when alert sending fails
        retry_attempts: Number of retry attempts for failed alerts
        retry_delay_seconds: Delay between retry attempts in seconds
    """

    error_on_alert_failure: bool
    retry_attempts: int
    retry_delay_seconds: int

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a FailureHandling instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing failure handling configuration

        Returns:
            A FailureHandling instance configured from the dictionary

        Examples:
            >>> config = {
            ...     "error_on_alert_failure": True,
            ...     "retry_attempts": 3,
            ...     "retry_delay_seconds": 30
            ... }
            >>> failure_handling = FailureHandling.from_dict(config)
        """
        logger.debug("Creating FailureHandling from configuration dictionary")
        try:
            error_on_alert_failure = dict_[ERROR_ON_ALERT_FAILURE]
            retry_attempts = dict_[RETRY_ATTEMPTS]
            retry_delay_seconds = dict_[RETRY_DELAY_SECONDS]
        except KeyError as e:
            raise ConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            error_on_alert_failure=error_on_alert_failure,
            retry_attempts=retry_attempts,
            retry_delay_seconds=retry_delay_seconds,
        )


@dataclass
class Channel(Model):
    """Channel configuration wrapper that maps directly to JSON structure.

    This class represents a complete channel configuration including the
    channel type, name, configuration, and failure handling settings.

    Attributes:
        type: The type of channel (email, http, file)
        name: The unique name of the channel
        config: The actual channel implementation instance
        failure_handling: Failure handling configuration
    """

    type: str
    name: str
    config: BaseConfig
    failure_handling: FailureHandling

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a Channel instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing channel configuration with keys:
                  - type: Channel type (email, http, file)
                  - name: Channel name
                  - config: Channel-specific configuration
                  - failure_handling: Failure handling configuration

        Returns:
            A Channel instance configured from the dictionary

        Raises:
            ConfigurationKeyError: If required configuration keys are missing
            ValueError: If an unknown channel type is specified

        Examples:
            >>> config = {
            ...     "type": "email",
            ...     "name": "custom-email-alert-name",
            ...     "config": {...},
            ...     "failure_handling": {...}
            ... }
            >>> channel = Channel.from_dict(config)
        """
        logger.debug("Creating Channel from configuration dictionary")

        try:
            type_ = dict_[TYPE]
            name = dict_[NAME]
            config_dict = dict_[CONFIG]
            failure_handling = FailureHandling.from_dict(dict_[FAILURE_HANDLING])
        except KeyError as e:
            raise ConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        # Merge config and failure_handling for the channel creation
        merged_config = {**config_dict, "failure_handling": dict_[FAILURE_HANDLING]}

        # Create the appropriate channel instance
        config: BaseConfig
        if type_ == "email":
            config = EmailConfig.from_dict(merged_config)
        elif type_ == "http":
            config = HttpConfig.from_dict(merged_config)
        elif type_ == "file":
            config = FileConfig.from_dict(merged_config)
        else:
            raise ValueError(f"Unknown channel type: {type_}")

        return cls(
            type=type_,
            name=name,
            config=config,
            failure_handling=failure_handling,
        )

    def send_alert(self, message: str, title: str) -> None:
        """Send an alert message through this channel.

        Args:
            message: The alert message to send.
            title: The alert title.
        """
        self.config.send_alert(message=message, title=title)
