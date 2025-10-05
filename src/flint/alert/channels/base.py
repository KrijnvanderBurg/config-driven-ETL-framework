"""Alert Channels for the Flint ETL framework.

This package provides different alert channels for the alert system,
including email, HTTP webhooks, and file-based alerts. Each channel
implements a common interface for consistent configuration and usage.

Available Channels:
- EmailChannel: SMTP-based email alerts
- HttpChannel: HTTP webhook alerts
- FileChannel: File-based logging alerts
"""

import logging
from abc import ABC, abstractmethod

from pydantic import Field

from flint import BaseModel
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class ChannelModel(BaseModel, ABC):
    """Base configuration for alert channels.

    Each concrete implementation must define a channel_type field with a specific
    Literal value to ensure type safety and proper discrimination.
    """

    id: str = Field(..., description="Unique identifier for the alert channel", min_length=1)
    description: str = Field(..., description="Description of the alert channel")
    enabled: bool = Field(..., description="Whether this channel is enabled")

    def alert(self, title: str, body: str) -> None:
        """Send an alert message through this channel."""
        logger.debug("Sending alert through channel: %s", self.id)
        self._alert(title=title, body=body)
        logger.info("Alert sent through %s channel", self.id)

    @abstractmethod
    def _alert(self, title: str, body: str) -> None:
        """Internal method to handle alert sending logic."""
