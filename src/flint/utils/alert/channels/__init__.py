"""Alert Configs for the Flint ETL framework.

This package provides different notification Configs for the alert system,
including email, HTTP webhooks, and file-based notifications. Each Config
implements a common interface for consistent configuration and usage.

Available Configs:
- EmailConfig: SMTP-based email notifications
- HttpConfig: HTTP webhook notifications
- FileConfig: File-based logging notifications
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass

from flint.models import Model
from flint.utils.alert.channels.email import EmailConfig
from flint.utils.alert.channels.file import FileConfig
from flint.utils.alert.channels.http import HttpConfig

__all__ = ["EmailConfig", "HttpConfig", "FileConfig", "BaseConfig"]


@dataclass
class BaseConfig(Model, ABC):
    """Base configuration for alert channels.

    This class serves as a base for all channel configurations, providing
    common attributes and methods for channel implementations.
    """

    @abstractmethod
    def send_alert(self, message: str, title: str) -> None:
        """Send an alert message through this channel.

        Args:
            message: The alert message to send.
            title: Optional alert title.

        Raises:
            NotImplementedError: Must be implemented by subclasses.
        """
        raise NotImplementedError("send_alert must be implemented by subclasses.")
