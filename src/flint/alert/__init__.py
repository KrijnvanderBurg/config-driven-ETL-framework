"""Alert system for the Flint ETL framework.

This package provides a comprehensive alert system for notifying users about
ETL job status, errors, and other important events. The system supports
multiple notification channels and flexible trigger rules.

Key components:
- AlertController: Main coordinator for alert processing
- ChannelManager: Manages different notification channels
- Templates: Message formatting and templating
- Triggers: Defines rules for trigger alerts to channels
"""

from flint.alert.channel import AlertChannel
from flint.alert.channels import EmailAlertChannel, FileAlertChannel, HttpAlertChannel
from flint.alert.controller import AlertController
from flint.alert.trigger import AlertConditions, AlertTrigger

__all__ = [
    "AlertController",
    "AlertChannel",
    "AlertTrigger",
    "AlertConditions",
    "EmailAlertChannel",
    "HttpAlertChannel",
    "FileAlertChannel",
]
