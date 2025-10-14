"""HTTP channel for sending alert messages via HTTP requests.

This module implements the HTTP alert channel that sends alerts
through HTTP endpoints like webhooks. It supports custom headers, different
HTTP methods, and configurable timeouts and failure handling.

The HttpAlertChannel follows the Samara framework patterns for configuration-driven
initialization and implements the ChannelModel interface.
"""

import logging
from typing import Literal

from pydantic import Field
from samara.alert.channels.base import ChannelModel
from samara.utils.http import HttpBase
from samara.utils.logger import get_logger
from typing_extensions import override

logger: logging.Logger = get_logger(__name__)


class HttpChannel(HttpBase, ChannelModel):
    """HTTP alert channel for webhook-based alerts.

    This class implements HTTP alerting functionality for sending alerts
    to webhooks or HTTP endpoints. It inherits HTTP functionality from HttpBase
    and channel functionality from ChannelModel.

    Attributes:
        channel_type: Always "http" for HTTP channels
    """

    channel_type: Literal["http"] = Field(..., description="Channel type discriminator")

    @override
    def _alert(self, title: str, body: str) -> None:
        """Send an alert message via HTTP.

        Args:
            title: The alert title.
            body: The alert message to send.

        Raises:
            requests.RequestException: If the HTTP request fails after all retries.
        """
        payload = {"title": title, "message": body}
        self._make_http_request(payload)
