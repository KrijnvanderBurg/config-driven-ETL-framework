"""Alert Channels for the Flint ETL framework.

This package provides different alert channels for the alert system,
including email, HTTP webhooks, and file-based alerts. Each channel
implements a common interface for consistent configuration and usage.

Available Channels:
- EmailAlertChannel: SMTP-based email alerts
- HttpAlertChannel: HTTP webhook alerts
- FileAlertChannel: File-based logging alerts
"""

from flint.alert.channels.email import EmailAlertChannel
from flint.alert.channels.file import FileAlertChannel
from flint.alert.channels.http import HttpAlertChannel

__all__ = ["EmailAlertChannel", "HttpAlertChannel", "FileAlertChannel"]
