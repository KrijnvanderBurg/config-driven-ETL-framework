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

from flint.alert.channel import AlertChannel
from flint.alert.channels import EmailConfig, FileConfig, HttpConfig
from flint.alert.manager import Alert
from flint.alert.trigger import AlertConditions, AlertTrigger

__all__ = [
    "Alert",
    "AlertChannel",
    "AlertTrigger",
    "AlertConditions",
    "EmailConfig",
    "HttpConfig",
    "FileConfig",
]
