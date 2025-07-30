"""Alert system for the Flint ETL framework.

This package provides a comprehensive alert system for notifying users about
ETL job status, errors, and other important events. The system supports
multiple notification channels and flexible routing rules.

Key components:
- AlertManager: Main coordinator for alert processing
- ChannelManager: Manages different notification channels
- Templates: Message formatting and templating
- RoutingRules: Defines rules for routing alerts to channels
"""

from flint.utils.alert.manager import AlertManager, Channels
from flint.utils.alert.template import Templates

__all__ = ["AlertManager", "Channels", "Templates"]
