"""HTTP channel for sending alert messages via HTTP requests.

This module implements the HTTP alert channel that sends alerts
through HTTP endpoints like webhooks. It supports custom headers, different
HTTP methods, and configurable timeouts and failure handling.

The HttpAlertChannel follows the Flint framework patterns for configuration-driven
initialization and implements the ChannelModel interface.
"""

import json
import logging
import time
from typing import Literal

import requests
from pydantic import Field, HttpUrl, PositiveInt
from typing_extensions import override

from flint import BaseModel
from flint.alert.channels import ChannelModel
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class Retry(BaseModel):
    """Configuration for handling channel failures and retries.

    This class defines how alert channels should behave when failures occur,
    including retry logic and error escalation settings.

    Attributes:
        raise_on_error: Whether to raise errors when alert sending fails
        max_attempts: Maximum number of retry attempts for failed alerts
        delay_in_seconds: Delay between retry attempts in seconds
    """

    raise_on_error: bool = Field(..., description="Whether to raise errors when alert sending fails")
    max_attempts: int = Field(..., description="Maximum number of retry attempts for failed alerts", ge=0, le=3)
    delay_in_seconds: int = Field(..., description="Delay between retry attempts in seconds", ge=0)


class HttpChannel(ChannelModel):
    """HTTP alert channel for webhook-based alerts.

    This class implements HTTP alerting functionality for sending alerts
    to webhooks or HTTP endpoints. It supports custom headers, different HTTP
    methods, and configurable timeouts.

    Attributes:
        channel_id: Always "http" for HTTP channels
        url: HTTP endpoint URL for sending alerts
        method: HTTP method to use (GET, POST, PUT, etc.)
        headers: Dictionary of HTTP headers to include in requests
        timeout: Request timeout in seconds
        retry: Configuration for handling channel failures and retries
    """

    channel_id: Literal["http"] = Field("http", description="Type identifier for the HTTP channel")
    url: HttpUrl = Field(..., description="HTTP endpoint URL for sending alerts")
    method: Literal["GET", "POST"] = Field(..., description="HTTP method to use (GET, POST)")
    headers: dict[str, str] = Field(
        default_factory=dict, description="Dictionary of HTTP headers to include in requests"
    )
    timeout: PositiveInt = Field(..., description="Request timeout in seconds")
    retry: Retry = Field(..., description="Configuration for handling channel failures and retries")

    @override
    def _alert(self, title: str, body: str) -> None:
        """Send an alert message via HTTP.

        Args:
            title: The alert title.
            body: The alert message to send.

        Raises:
            requests.RequestException: If the HTTP request fails after all retries.
        """
        payload = json.dumps({"title": title, "message": body})

        for attempt in range(self.retry.max_attempts + 1):
            try:
                response = requests.request(
                    method=self.method,
                    url=self.url.encoded_string(),
                    headers=self.headers,
                    data=payload,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                logger.info("HTTP alert sent successfully to %s", self.url)
                return

            except requests.RequestException as e:
                if attempt < self.retry.max_attempts:
                    logger.warning(
                        "HTTP alert attempt %d failed: %s. Retrying in %d seconds...",
                        attempt + 1,
                        e,
                        self.retry.delay_in_seconds,
                    )
                    time.sleep(self.retry.delay_in_seconds)
                else:
                    logger.error("HTTP alert failed after %d attempts: %s", self.retry.max_attempts + 1, e)
                    if self.retry.raise_on_error:
                        raise e
                    return
