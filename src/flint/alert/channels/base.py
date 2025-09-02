"""Alert Channels for the Flint ETL framework.

This package provides different alert channels for the alert system,
including email, HTTP webhooks, and file-based alerts. Each channel
implements a common interface for consistent configuration and usage.

Available Channels:
- EmailAlertChannel: SMTP-based email alerts
- HttpAlertChannel: HTTP webhook alerts
- FileAlertChannel: File-based logging alerts
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass

from flint.etl.models import Model
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


@dataclass
class BaseAlertChannel(Model, ABC):
    """Base configuration for alert channels.

    This class serves as a base for all alert channel configurations, providing
    common attributes and methods for channel implementations.
    """

    def alert(self, title: str, body: str) -> None:
        """Send an alert message through this channel.

        Args:
            title: Optional alert title.
            body: The alert message to send.

        Raises:
            NotImplementedError: Must be implemented by subclasses.
        """
        logger.debug("Sending alert through channel: %s", self.__class__.__name__)
        self._alert(title=title, body=body)
        logger.info("Alert sent through %s channel", self.__class__.__name__)

    @abstractmethod
    def _alert(self, title: str, body: str) -> None:
        """Internal method to handle alert sending logic.

        This method should implement the actual logic for sending alerts
        through the channel. It is expected to be implemented by subclasses.

        Args:
            message: The alert message to send.
            title: Optional alert title.
        """
        raise NotImplementedError("_alert must be implemented by subclasses.")
