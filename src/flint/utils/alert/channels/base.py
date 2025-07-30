"""Base classes for alert channels.

This module provides the abstract base classes and common interfaces for
implementing different types of alert channels. It defines the common
structure and failure handling configuration that all channels share.

The base classes follow the Flint framework patterns for configuration-driven
initialization and provide a consistent interface for all channel types.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import ConfigurationKeyError
from flint.models import Model
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

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
class BaseChannel(Model, ABC):
    """Abstract base class for all alert channels.

    This class provides the common interface and shared functionality
    for all types of alert channels. Concrete channel implementations
    must inherit from this class and implement the abstract methods.
    """

    @classmethod
    @abstractmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a channel instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing channel configuration

        Returns:
            A channel instance configured from the dictionary

        Raises:
            NotImplementedError: When not implemented by subclasses
        """
        raise NotImplementedError

    @abstractmethod
    def send_alert(self, message: str, title: str | None = None) -> None:
        """Send an alert message through this channel.

        Args:
            message: The alert message to send.
            title: Optional alert title.

        Raises:
            NotImplementedError: Must be implemented by subclasses.
        """
        raise NotImplementedError("send_alert must be implemented by subclasses.")
