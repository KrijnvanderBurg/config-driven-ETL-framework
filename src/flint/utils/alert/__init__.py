"""Alert system for the Flint ETL framework.

This package provides a comprehensive alert system for notifying users about
ETL job status, errors, and other important events. The system supports
multiple notification channels and flexible trigger rules.

Key components:
- AlertManager: Main coordinator for alert processing
- ChannelManager: Manages different notification channels
- Templates: Message formatting and templating
- Triggers: Defines rules for trigger alerts to channels
"""

from flint.utils.alert.channel import Channel, FailureHandling
from flint.utils.alert.channels import BaseConfig, EmailConfig, FileConfig, HttpConfig
from flint.utils.alert.manager import AlertManager
from flint.utils.alert.trigger import Conditions, Trigger

__all__ = [
    "Channel",
    "AlertManager",
    "Trigger",
    "Trigger",
    "Conditions",
    "FailureHandling",
    "BaseConfig",
    "EmailConfig",
    "HttpConfig",
    "FileConfig",
]
