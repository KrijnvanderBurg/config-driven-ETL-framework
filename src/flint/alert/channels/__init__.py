"""Alert Configs for the Flint ETL framework.

This package provides different notification Configs for the alert system,
including email, HTTP webhooks, and file-based notifications. Each Config
implements a common interface for consistent configuration and usage.

Available Configs:
- EmailConfig: SMTP-based email notifications
- HttpConfig: HTTP webhook notifications
- FileConfig: File-based logging notifications
"""

from flint.alert.channels.email import EmailConfig
from flint.alert.channels.file import FileConfig
from flint.alert.channels.http import HttpConfig

__all__ = ["EmailConfig", "HttpConfig", "FileConfig"]
