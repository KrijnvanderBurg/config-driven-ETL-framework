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

from flint.utils.alert.channel import AlertChannel, Retry
from flint.utils.alert.channels import BaseConfig, EmailConfig, FileConfig, HttpConfig
from flint.utils.alert.manager import Alert
from flint.utils.alert.trigger import AlertConditions, AlertTrigger

__all__ = [
    "AlertChannel",
    "Alert",
    "AlertTrigger",
    "AlertTrigger",
    "AlertConditions",
    "Retry",
    "BaseConfig",
    "EmailConfig",
    "HttpConfig",
    "FileConfig",
]
