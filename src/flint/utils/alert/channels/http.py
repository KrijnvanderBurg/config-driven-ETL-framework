"""HTTP channel for sending alert notifications via HTTP requests.

This module implements the HTTP alert channel that sends notifications
through HTTP endpoints like webhooks. It supports custom headers, different
HTTP methods, and configurable timeouts and failure handling.

The HttpChannel follows the Flint framework patterns for configuration-driven
initialization and implements the BaseChannel interface.
"""

import logging
from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import ConfigurationKeyError
from flint.utils.alert.channels.base import BaseChannel, FailureHandling
from flint.utils.logger import get_logger

URL: Final[str] = "url"
METHOD: Final[str] = "method"
HEADERS: Final[str] = "headers"
TIMEOUT: Final[str] = "timeout"
FAILURE_HANDLING: Final[str] = "failure_handling"

logger: logging.Logger = get_logger(__name__)


@dataclass
class HttpChannel(BaseChannel):
    """HTTP alert channel for webhook-based notifications.

    This class implements HTTP alerting functionality for sending notifications
    to webhooks or HTTP endpoints. It supports custom headers, different HTTP
    methods, and configurable timeouts.

    Attributes:
        url: HTTP endpoint URL for sending alerts
        method: HTTP method to use (GET, POST, PUT, etc.)
        headers: Dictionary of HTTP headers to include in requests
        timeout: Request timeout in seconds
        failure_handling: Configuration for failure handling and retries
    """

    url: str
    method: str
    headers: dict[str, str]
    timeout: int
    failure_handling: FailureHandling

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create an HttpChannel instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing HTTP channel configuration with keys:
                  - url: HTTP endpoint URL
                  - method: HTTP method (POST, GET, etc.)
                  - headers: Dictionary of HTTP headers
                  - timeout: Request timeout in seconds
                  - failure_handling: Failure handling configuration

        Returns:
            An HttpChannel instance configured from the dictionary

        Examples:
            >>> config = {
            ...     "url": "${SLACK_WEBHOOK_URL}",
            ...     "method": "POST",
            ...     "headers": {"Content-Type": "application/json"},
            ...     "timeout": 30,
            ...     "failure_handling": {...}
            ... }
            >>> http_channel = HttpChannel.from_dict(config)
        """
        logger.debug("Creating HttpChannel from configuration dictionary")
        try:
            url = dict_[URL]
            method = dict_[METHOD]
            headers = dict_[HEADERS]
            timeout = dict_[TIMEOUT]
            failure_handling = FailureHandling.from_dict(dict_[FAILURE_HANDLING])
        except KeyError as e:
            raise ConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            url=url,
            method=method,
            headers=headers,
            timeout=timeout,
            failure_handling=failure_handling,
        )
