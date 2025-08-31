"""HTTP POST action configuration for sensor management.

This module provides dataclasses for HTTP POST action configurations including
retry policies and request settings.
It follows the from_dict pattern consistent with other components in the Flint framework.
"""

import logging
from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import FlintConfigurationKeyError
from flint.job.models import Model
from flint.sensor.actions.base import BaseAction
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

# Retry constants
ERROR_ON_ALERT_FAILURE: Final[str] = "error_on_alert_failure"
ATTEMPTS: Final[str] = "attempts"
DELAY_IN_SECONDS: Final[str] = "delay_in_seconds"


@dataclass
class ActionRetry(Model):
    """Configuration for action retry policies.

    This class manages retry configuration for actions that may fail,
    providing resilience and fault tolerance.

    Attributes:
        error_on_alert_failure: Whether to raise an error on alert failure
        attempts: Number of retry attempts
        delay_in_seconds: Delay between retry attempts in seconds
    """

    error_on_alert_failure: bool
    attempts: int
    delay_in_seconds: int

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create an ActionRetry instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing retry configuration with keys:
                  - error_on_alert_failure: Whether to error on failure
                  - attempts: Number of retry attempts
                  - delay_in_seconds: Delay between retries

        Returns:
            An ActionRetry instance configured from the dictionary

        Raises:
            FlintConfigurationKeyError: If a required key is missing.

        Examples:
            >>> config = {
            ...     "error_on_alert_failure": True,
            ...     "attempts": 3,
            ...     "delay_in_seconds": 30
            ... }
            >>> retry = ActionRetry.from_dict(config)
        """
        logger.debug("Creating ActionRetry from configuration dictionary")

        try:
            error_on_alert_failure = dict_[ERROR_ON_ALERT_FAILURE]
            attempts = dict_[ATTEMPTS]
            delay_in_seconds = dict_[DELAY_IN_SECONDS]
        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            error_on_alert_failure=error_on_alert_failure,
            attempts=attempts,
            delay_in_seconds=delay_in_seconds,
        )


# HTTP POST action constants
URL: Final[str] = "url"
METHOD: Final[str] = "method"
HEADERS: Final[str] = "headers"
TIMEOUT: Final[str] = "timeout"
RETRY: Final[str] = "retry"


@dataclass
class HttpPostAction(BaseAction):
    """Configuration for HTTP POST actions.

    This class manages HTTP POST action configuration including
    URL, method, headers, timeout, and retry policies.

    Attributes:
        url: Request URL
        method: HTTP method
        headers: Request headers
        timeout: Request timeout in seconds
        retry: Retry configuration
    """

    url: str
    method: str
    headers: dict[str, str]
    timeout: int
    retry: ActionRetry

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create an HttpPostAction instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing HTTP POST action configuration with keys:
                  - url: Request URL
                  - method: HTTP method
                  - headers: Request headers
                  - timeout: Request timeout
                  - retry: Retry configuration

        Returns:
            An HttpPostAction instance configured from the dictionary

        Raises:
            FlintConfigurationKeyError: If a required key is missing.

        Examples:
            >>> config = {
            ...     "url": "https://hooks.slack.com",
            ...     "method": "POST",
            ...     "headers": {"Content-Type": "application/json"},
            ...     "timeout": 30,
            ...     "retry": {...}
            ... }
            >>> action = HttpPostAction.from_dict(config)
        """
        logger.debug("Creating HttpPostAction from configuration dictionary")

        try:
            url = dict_[URL]
            method = dict_[METHOD]
            headers = dict_[HEADERS]
            timeout = dict_[TIMEOUT]
            retry = ActionRetry.from_dict(dict_[RETRY])
        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            url=url,
            method=method,
            headers=headers,
            timeout=timeout,
            retry=retry,
        )

    def _execute(self) -> None:
        """Execute the HTTP POST action.

        This method would implement the actual HTTP POST request logic.
        """
        # Implementation would make the actual HTTP POST request
        # This is just a placeholder for the actual implementation
        logger.debug("Executing HTTP POST action to %s", self.url)
