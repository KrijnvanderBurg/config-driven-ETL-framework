"""HTTP utility classes for the Flint ETL framework.

This module provides shared HTTP functionality that can be used by both
alert channels and actions. It includes retry logic, timeout handling,
and standardized HTTP request functionality.
"""

import json
import logging
import time
from typing import Any

import requests
from pydantic import BaseModel, Field, HttpUrl, PositiveInt

from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class Retry(BaseModel):
    """Configuration for handling HTTP failures and retries.

    This class defines how HTTP requests should behave when failures occur,
    including retry logic and error escalation settings.

    Attributes:
        raise_on_error: Whether to raise errors when HTTP request fails
        max_attempts: Maximum number of retry attempts for failed requests
        delay_in_seconds: Delay between retry attempts in seconds
    """

    raise_on_error: bool = Field(..., description="Whether to raise errors when HTTP request fails")
    max_attempts: int = Field(..., description="Maximum number of retry attempts for failed requests", ge=0, le=3)
    delay_in_seconds: PositiveInt = Field(..., description="Delay between retry attempts in seconds", ge=1, le=30)


class HttpBase(BaseModel):
    """Base class for HTTP functionality shared between alerts and actions.

    This class provides common HTTP configuration and request handling
    that can be inherited by both alert channels and actions.

    Attributes:
        url: HTTP endpoint URL for sending requests
        method: HTTP method to use (GET, POST, PUT, etc.)
        headers: Dictionary of HTTP headers to include in requests
        timeout: Request timeout in seconds
        retry: Configuration for handling failures and retries
    """

    url: HttpUrl = Field(..., description="HTTP endpoint URL for sending requests")
    method: str = Field(..., description="HTTP method to use (GET, POST, PUT, etc.)")
    headers: dict[str, str] = Field(
        default_factory=dict, description="Dictionary of HTTP headers to include in requests"
    )
    timeout: PositiveInt = Field(..., description="Request timeout in seconds", ge=1, le=30)
    retry: Retry = Field(..., description="Configuration for handling failures and retries")

    def _make_http_request(self, payload: dict[str, Any] | None = None) -> None:
        """Make an HTTP request with retry logic.

        Args:
            payload: Optional payload to send in the request body.

        Raises:
            requests.RequestException: If the HTTP request fails after all retries.
        """
        data = json.dumps(payload)

        for attempt in range(self.retry.max_attempts + 1):
            try:
                response = requests.request(
                    method=self.method,
                    url=str(self.url),
                    headers=self.headers,
                    data=data,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                logger.info("HTTP request sent successfully to %s", self.url)
                return

            except requests.RequestException as e:
                if attempt < self.retry.max_attempts:
                    logger.warning(
                        "HTTP request attempt %d failed: %s. Retrying in %d seconds...",
                        attempt + 1,
                        e,
                        self.retry.delay_in_seconds,
                    )
                    time.sleep(self.retry.delay_in_seconds)
                else:
                    logger.error("HTTP request failed after %d attempts: %s", self.retry.max_attempts + 1, e)
                    if self.retry.raise_on_error:
                        raise e
