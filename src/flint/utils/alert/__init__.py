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

from flint.utils.alert.channel import Channel
from flint.utils.alert.channels import Channel, EmailChannel, FailureHandling, FileChannel, HttpChannel
from flint.utils.alert.manager import AlertManager
from flint.utils.alert.template import Templates
from flint.utils.alert.triggers import Conditions, RoutingRule, Triggers

__all__ = [
    "Channel",
    "AlertManager",
    "Templates",
    "Triggers",
    "RoutingRule",
    "Conditions",
    "Channel",
    "FailureHandling",
    "EmailChannel",
    "HttpChannel",
    "FileChannel",
]
