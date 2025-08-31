"""HTTP polling watcher configuration for sensor management.

This module provides dataclasses for HTTP polling watcher configurations including
polling settings, request configurations, and HTTP-specific trigger conditions.
It follows the from_dict pattern consistent with other components in the Flint framework.
"""

import logging
from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import FlintConfigurationKeyError
from flint.job.models import Model
from flint.sensor.watchers.base import BaseWatcher
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

# Polling constants
INTERVAL_SECONDS: Final[str] = "interval_seconds"
FAILURE_THRESHOLD: Final[str] = "failure_threshold"


@dataclass
class PollingConfig(Model):
    """Configuration for HTTP polling settings.

    This class manages polling configuration for HTTP-based watchers,
    defining how frequently to check and failure thresholds.

    Attributes:
        interval_seconds: Polling interval in seconds
        failure_threshold: Number of failures before triggering alerts
    """

    interval_seconds: int
    failure_threshold: int

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a PollingConfig instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing polling configuration with keys:
                  - interval_seconds: Polling interval
                  - failure_threshold: Failure threshold

        Returns:
            A PollingConfig instance configured from the dictionary

        Raises:
            FlintConfigurationKeyError: If a required key is missing.

        Examples:
            >>> config = {
            ...     "interval_seconds": 60,
            ...     "failure_threshold": 3
            ... }
            >>> polling = PollingConfig.from_dict(config)
        """
        logger.debug("Creating PollingConfig from configuration dictionary")

        try:
            interval_seconds = dict_[INTERVAL_SECONDS]
            failure_threshold = dict_[FAILURE_THRESHOLD]
        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            interval_seconds=interval_seconds,
            failure_threshold=failure_threshold,
        )


# Request retry constants
ERROR_ON_ALERT_FAILURE: Final[str] = "error_on_alert_failure"
ATTEMPTS: Final[str] = "attempts"
DELAY_IN_SECONDS: Final[str] = "delay_in_seconds"


@dataclass
class RequestRetry(Model):
    """Configuration for HTTP request retry policies.

    This class manages retry configuration for HTTP requests that may fail,
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
        """Create a RequestRetry instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing retry configuration with keys:
                  - error_on_alert_failure: Whether to error on failure
                  - attempts: Number of retry attempts
                  - delay_in_seconds: Delay between retries

        Returns:
            A RequestRetry instance configured from the dictionary

        Raises:
            FlintConfigurationKeyError: If a required key is missing.

        Examples:
            >>> config = {
            ...     "error_on_alert_failure": True,
            ...     "attempts": 3,
            ...     "delay_in_seconds": 30
            ... }
            >>> retry = RequestRetry.from_dict(config)
        """
        logger.debug("Creating RequestRetry from configuration dictionary")

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


# Request constants
URL: Final[str] = "url"
METHOD: Final[str] = "method"
HEADERS: Final[str] = "headers"
TIMEOUT: Final[str] = "timeout"
RETRY: Final[str] = "retry"


@dataclass
class RequestConfig(Model):
    """Configuration for HTTP request settings.

    This class manages HTTP request configuration including
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
    retry: RequestRetry

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a RequestConfig instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing request configuration with keys:
                  - url: Request URL
                  - method: HTTP method
                  - headers: Request headers
                  - timeout: Request timeout
                  - retry: Retry configuration

        Returns:
            A RequestConfig instance configured from the dictionary

        Raises:
            FlintConfigurationKeyError: If a required key is missing.

        Examples:
            >>> config = {
            ...     "url": "https://api.example.com",
            ...     "method": "POST",
            ...     "headers": {"Content-Type": "application/json"},
            ...     "timeout": 30,
            ...     "retry": {...}
            ... }
            >>> request = RequestConfig.from_dict(config)
        """
        logger.debug("Creating RequestConfig from configuration dictionary")

        try:
            url = dict_[URL]
            method = dict_[METHOD]
            headers = dict_[HEADERS]
            timeout = dict_[TIMEOUT]
            retry = RequestRetry.from_dict(dict_[RETRY])
        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            url=url,
            method=method,
            headers=headers,
            timeout=timeout,
            retry=retry,
        )


# HTTP trigger conditions constants
STATUS_CODES: Final[str] = "status_codes"
RESPONSE_REGEX: Final[str] = "response_regex"


@dataclass
class HttpTriggerConditions(Model):
    """Configuration for HTTP trigger conditions.

    This class manages trigger conditions for HTTP watchers,
    defining when actions should be triggered based on HTTP responses.

    Attributes:
        status_codes: List of acceptable HTTP status codes
        response_regex: List of regex patterns to match in response body
    """

    status_codes: list[int]
    response_regex: list[str]

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create an HttpTriggerConditions instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing HTTP trigger conditions with keys:
                  - status_codes: List of acceptable status codes
                  - response_regex: List of response regex patterns

        Returns:
            An HttpTriggerConditions instance configured from the dictionary

        Raises:
            FlintConfigurationKeyError: If a required key is missing.

        Examples:
            >>> config = {
            ...     "status_codes": [200, 201],
            ...     "response_regex": ["healthy", "ok"]
            ... }
            >>> conditions = HttpTriggerConditions.from_dict(config)
        """
        logger.debug("Creating HttpTriggerConditions from configuration dictionary")

        try:
            status_codes = dict_[STATUS_CODES]
            response_regex = dict_[RESPONSE_REGEX]
        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            status_codes=status_codes,
            response_regex=response_regex,
        )


# Config sub-constants
POLLING: Final[str] = "polling"
REQUEST: Final[str] = "request"
TRIGGER_CONDITIONS: Final[str] = "trigger_conditions"


@dataclass
class HttpPollingWatcher(BaseWatcher):
    """Configuration for HTTP polling watchers.

    This class manages complete HTTP polling watcher configuration including
    polling settings, request configuration, and trigger conditions.

    Attributes:
        polling: Polling configuration for the watcher
        request: HTTP request configuration
        trigger_conditions: Conditions for triggering actions
    """

    polling: PollingConfig
    request: RequestConfig
    trigger_conditions: HttpTriggerConditions

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create an HttpPollingWatcher instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing HTTP polling watcher configuration with keys:
                  - polling: Polling configuration
                  - request: Request configuration
                  - trigger_conditions: Trigger condition configuration

        Returns:
            An HttpPollingWatcher instance configured from the dictionary

        Raises:
            FlintConfigurationKeyError: If a required key is missing.

        Examples:
            >>> config = {
            ...     "polling": {...},
            ...     "request": {...},
            ...     "trigger_conditions": {...}
            ... }
            >>> watcher = HttpPollingWatcher.from_dict(config)
        """
        logger.debug("Creating HttpPollingWatcher from configuration dictionary")

        try:
            polling = PollingConfig.from_dict(dict_[POLLING])
            request = RequestConfig.from_dict(dict_[REQUEST])
            trigger_conditions = HttpTriggerConditions.from_dict(dict_[TRIGGER_CONDITIONS])
        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            polling=polling,
            request=request,
            trigger_conditions=trigger_conditions,
        )
