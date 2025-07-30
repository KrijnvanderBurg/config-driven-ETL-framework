"""Alert channels for the Flint ETL framework.

This package provides different notification channels for the alert system,
including email, HTTP webhooks, and file-based notifications. Each channel
implements a common interface for consistent configuration and usage.

Available channels:
- EmailChannel: SMTP-based email notifications
- HttpChannel: HTTP webhook notifications
- FileChannel: File-based logging notifications
"""

from flint.utils.alert.channels.base import BaseChannel, FailureHandling
from flint.utils.alert.channels.email import EmailChannel
from flint.utils.alert.channels.file import FileChannel
from flint.utils.alert.channels.http import HttpChannel

__all__ = ["BaseChannel", "FailureHandling", "EmailChannel", "HttpChannel", "FileChannel"]
