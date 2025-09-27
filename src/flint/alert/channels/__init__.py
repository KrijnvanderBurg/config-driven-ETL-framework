"""Alert Channels for the Flint ETL framework.

This package provides different alert channels for the alert system,
including email, HTTP webhooks, and file-based alerts. Each channel
implements a common interface for consistent configuration and usage.

Available Channels:
- EmailChannel: SMTP-based email alerts
- HttpChannel: HTTP webhook alerts
- FileChannel: File-based logging alerts
"""

import logging
from typing import Annotated

from pydantic import Discriminator

from flint.alert.channels.base import ChannelModel
from flint.alert.channels.email import EmailChannel
from flint.alert.channels.file import FileChannel
from flint.alert.channels.http import HttpChannel
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

__all__ = ["ChannelModel", "ChannelUnion"]

ChannelUnion = Annotated[
    EmailChannel | HttpChannel | FileChannel,
    Discriminator("channel_id"),
]
