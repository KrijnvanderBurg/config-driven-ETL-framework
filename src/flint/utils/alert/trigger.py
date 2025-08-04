"""Trigger rules for alert channel selection and filtering.

This module defines the trigger rules system that determines which channels
should receive specific alerts based on conditions like log level, message
content, and environment variables.

The trigger system supports flexible condition matching with various criteria
to enable sophisticated alert trigger logic.
"""

import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import ConfigurationKeyError
from flint.models import Model
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

# RoutingRule constants
NAME: Final[str] = "name"
ENABLED: Final[str] = "enabled"
CHANNEL_NAMES: Final[str] = "channel_names"
TEMPLATE: Final[str] = "Template"
CONDITIONS: Final[str] = "conditions"

# Template constants
PREPEND_TITLE: Final[str] = "prepend_title"
APPEND_TITLE: Final[str] = "append_title"
PREPEND_MESSAGE: Final[str] = "prepend_message"
APPEND_MESSAGE: Final[str] = "append_message"

# Conditions constants
EXCEPTION_CONTAINS: Final[str] = "exception_contains"
EXCEPTION_REGEX: Final[str] = "exception_regex"
ENV_VARS_MATCHES: Final[str] = "env_vars_matches"


@dataclass
class Template(Model):
    """Configuration for alert message templates and formatting.

    This class manages the template configuration for formatting alert messages,
    including prefixes and suffixes for both titles and message content.

    Attributes:
        prepend_title: Text to prepend to alert titles
        append_title: Text to append to alert titles
        prepend_message: Text to prepend to alert messages
        append_message: Text to append to alert messages
    """

    prepend_title: str
    append_title: str
    prepend_message: str
    append_message: str

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a Templates instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing template configuration with keys:
                  - prepend_title: Text to prepend to alert titles
                  - append_title: Text to append to alert titles
                  - prepend_message: Text to prepend to alert messages
                  - append_message: Text to append to alert messages

        Returns:
            A Templates instance configured from the dictionary

        Raises:
            ConfigurationKeyError: If a required key is missing.

        Examples:
            >>> config = {
            ...     "prepend_title": "ETL Pipeline Alert",
            ...     "append_title": "Alert Notification",
            ...     "prepend_message": "Attention: ETL Pipeline Alert",
            ...     "append_message": "Please take necessary actions."
            ... }
            >>> templates = Templates.from_dict(config)
        """
        logger.debug("Creating Templates from configuration dictionary")

        try:
            prepend_title = dict_[PREPEND_TITLE]
            append_title = dict_[APPEND_TITLE]
            prepend_message = dict_[PREPEND_MESSAGE]
            append_message = dict_[APPEND_MESSAGE]
        except KeyError as e:
            raise ConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            prepend_title=prepend_title,
            append_title=append_title,
            prepend_message=prepend_message,
            append_message=append_message,
        )

    def format_message(self, message: str) -> str:
        """Format a message with prepend and append templates.

        Args:
            message: The raw message to format

        Returns:
            The formatted message with templates applied
        """
        return f"{self.prepend_message}{message}{self.append_message}"

    def format_title(self, title: str) -> str:
        """Format a title with prepend and append templates.

        Args:
            title: The raw title to format

        Returns:
            The formatted title with templates applied
        """
        return f"{self.prepend_title}{title}{self.append_title}"


@dataclass
class Conditions(Model):
    """Conditions for determining when a trigger rule should apply.

    This class defines the various conditions that can be used to filter
    and route alerts to appropriate channels based on alert characteristics.

    Attributes:
        exception_contains: List of strings that must be present in the exception message
        exception_regex: Regular expression pattern for exception message matching
        env_vars_matches: Dictionary of environment variables and their expected values
    """

    exception_contains: list[str]
    exception_regex: str
    env_vars_matches: dict[str, list[str]]

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a Conditions instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing condition configuration with keys:
                  - exception_contains: List of required exception message strings
                  - exception_regex: Regular expression for exception message matching
                  - env_vars_matches: Environment variable matching rules

        Returns:
            A Conditions instance configured from the dictionary

        Examples:
            >>> config = {
            ...     "exception_contains": ["database", "connection"],
            ...     "exception_regex": ".*timeout.*",
            ...     "env_vars_matches": {"ENVIRONMENT": ["production"]}
            ... }
            >>> conditions = Conditions.from_dict(config)
        """
        logger.debug("Creating Conditions from configuration dictionary")

        try:
            exception_contains = dict_[EXCEPTION_CONTAINS]
            exception_regex = dict_[EXCEPTION_REGEX]
            env_vars_matches = dict_[ENV_VARS_MATCHES]
        except KeyError as e:
            raise ConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            exception_contains=exception_contains,
            exception_regex=exception_regex,
            env_vars_matches=env_vars_matches,
        )

    def _is_exception_contains(self, exception: Exception) -> bool:
        """Check if the exception message contains any of the required strings.

        Args:
            exception: The exception to check

        Returns:
            True if any of the required strings are found in the message, False otherwise
        """
        if not self.exception_contains:
            logger.debug("No exception_contains configured; skipping contains check.")
            return True

        message = str(exception)

        for required in self.exception_contains:
            if required in message:
                logger.debug("Exception message contains required string: '%s'", required)
                return True
        logger.debug("Exception message does not contain any required strings.")
        return False

    def _is_exception_regex(self, exception: Exception) -> bool:
        """Check if the exception message matches the configured regex.

        Args:
            exception: The exception to check

        Returns:
            True if the message matches the regex, False otherwise
        """
        if not self.exception_regex:
            logger.debug("No exception_regex configured; skipping regex check.")
            return True

        message = str(exception)

        if not self.exception_regex:
            logger.debug("No exception_regex configured; skipping regex check.")
            return True

        if re.search(self.exception_regex, message):
            logger.debug("Exception message matches regex: '%s'", self.exception_regex)
            return True

        logger.debug("Exception message does not match regex: '%s'", self.exception_regex)
        return False

    def _is_env_vars_matches(self) -> bool:
        """Check if any environment variable matches the configured rules.

        Returns:
            True if any specified environment variable matches one of its expected values, False otherwise
        """
        if not self.env_vars_matches:
            logger.debug("No env_vars_matches configured; skipping environment variable check.")
            return True

        for var, expected_values in self.env_vars_matches.items():
            actual_value = os.environ.get(var)

            if actual_value is None:
                continue

            if actual_value in expected_values:
                logger.debug(
                    "Environment variable '%s' value '%s' matches expected values: %s",
                    var,
                    actual_value,
                    expected_values,
                )
                return True

        logger.debug("No environment variable conditions are satisfied.")
        return False

    def is_all_conditions_met(self, exception: Exception) -> bool:
        """Check if all conditions are met for the given exception and environment variables.

        Args:
            exception: The exception or message to evaluate

        Returns:
            True if all conditions are satisfied, False otherwise
        """
        return (
            self._is_exception_contains(exception)
            and self._is_exception_regex(exception)
            and self._is_env_vars_matches()
        )


@dataclass
class Trigger(Model):
    """Individual trigger rule for alert channel selection.

    This class represents a single trigger rule that defines conditions
    for when alerts should be sent to specific channel_names.

    Attributes:
        name: Unique name for the trigger rule
        enabled: Whether this rule is currently active
        channel_names: List of channel names that should receive alerts matching this rule
        conditions: Conditions that must be met for this rule to apply
    """

    name: str
    enabled: bool
    channel_names: list[str]
    template: Template
    conditions: Conditions

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a RoutingRule instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing trigger rule configuration with keys:
                  - name: Unique name for the rule
                  - enabled: Whether the rule is active
                  - channel_names: List of channel names
                  - conditions: Condition configuration

        Returns:
            A RoutingRule instance configured from the dictionary

        Examples:
            >>> config = {
            ...     "name": "production_critical_errors",
            ...     "enabled": True,
            ...     "channel_names": ["email", "http"],
            ...     "conditions": {...}
            ... }
            >>> rule = RoutingRule.from_dict(config)
        """
        logger.debug("Creating RoutingRule from configuration dictionary")

        try:
            name = dict_[NAME]
            enabled = dict_[ENABLED]
            channel_names = dict_[CHANNEL_NAMES]
            conditions = Conditions.from_dict(dict_[CONDITIONS])
            template = Template.from_dict(dict_[TEMPLATE])
        except KeyError as e:
            raise ConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            name=name,
            enabled=enabled,
            channel_names=channel_names,
            template=template,
            conditions=conditions,
        )

    def is_fire(self, exception: Exception) -> bool:
        """Check if the conditions are met for triggering an alert.

        This method evaluates the conditions against the current alert context
        to determine if the trigger should be activated.
        """

        if not self.enabled:
            logger.debug("Trigger '%s' is disabled; skipping fire check.", self.name)
            return False

        if not self.conditions.is_all_conditions_met(exception):
            logger.debug("Conditions for trigger '%s' are not met; skipping fire check.", self.name)
            return False

        logger.debug("Trigger '%s' conditions met", self.name)
        return True
