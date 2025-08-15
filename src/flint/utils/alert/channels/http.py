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
from flint.models import Model
from flint.utils.alert.channels.base import BaseConfig
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

URL: Final[str] = "url"
METHOD: Final[str] = "method"
HEADERS: Final[str] = "headers"
TIMEOUT: Final[str] = "timeout"
RETRY: Final[str] = "retry"

ERROR_ON_ALERT_FAILURE: Final[str] = "error_on_alert_failure"
ATTEMPTS: Final[str] = "attempts"
DELAY_IN_SECONDS: Final[str] = "delay_in_seconds"


@dataclass
class Retry(Model):
    """Configuration for handling channel failures and retries.

    This class defines how alert channels should behave when failures occur,
    including retry logic and error escalation settings.

    Attributes:
        error_on_alert_failure: Whether to raise errors when alert sending fails
        retry_attempts: Number of retry attempts for failed alerts
        retry_delay_seconds: Delay between retry attempts in seconds
    """

    error_on_alert_failure: bool
    attempts: int
    delay_in_seconds: int

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a FailureHandling instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing failure handling configuration

        Returns:
            A FailureHandling instance configured from the dictionary

        Examples:
            >>> config = {
            ...     "error_on_alert_failure": True,
            ...     "attempts": 3,
            ...     "delay_in_seconds": 30
            ... }
            >>> retry = FailureHandling.from_dict(config)
        """
        logger.debug("Creating FailureHandling from configuration dictionary")
        try:
            error_on_alert_failure = dict_[ERROR_ON_ALERT_FAILURE]
            attempts = dict_[ATTEMPTS]
            delay_in_seconds = dict_[DELAY_IN_SECONDS]
        except KeyError as e:
            raise ConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            error_on_alert_failure=error_on_alert_failure,
            attempts=attempts,
            delay_in_seconds=delay_in_seconds,
        )


@dataclass
class HttpConfig(BaseConfig):
    """HTTP config for webhook-based notifications.

    This class implements HTTP alerting functionality for sending notifications
    to webhooks or HTTP endpoints. It supports custom headers, different HTTP
    methods, and configurable timeouts.

    Attributes:
        url: HTTP endpoint URL for sending alerts
        method: HTTP method to use (GET, POST, PUT, etc.)
        headers: Dictionary of HTTP headers to include in requests
        timeout: Request timeout in seconds
        failure_handling: Configuration for handling channel failures and retries
    """

    url: str
    method: str
    headers: dict[str, str]
    timeout: int
    retry: Retry

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
            retry_dict = dict_[RETRY]
        except KeyError as e:
            raise ConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        retry = Retry.from_dict(retry_dict)

        return cls(
            url=url,
            method=method,
            headers=headers,
            timeout=timeout,
            retry=retry,
        )

    def _alert(self, title: str, body: str) -> None:
        """Send an alert message via HTTP.

        Args:
            body: The alert message to send.
            title: The alert title.

        Raises:
            NotImplementedError: HTTP sending is not yet implemented.
        """
        raise NotImplementedError("HTTP sending not yet implemented")
