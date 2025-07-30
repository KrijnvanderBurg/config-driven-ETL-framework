"""Trigger rules for alert channel selection and filtering.

This module defines the trigger rules system that determines which channels
should receive specific alerts based on conditions like log level, message
content, and environment variables.

The trigger system supports flexible condition matching with various criteria
to enable sophisticated alert trigger logic.
"""

import logging
from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import ConfigurationKeyError
from flint.models import Model
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

# Conditions constants
EXCEPTION_CONTAINS: Final[str] = "exception_contains"
EXCEPTION_REGEX: Final[str] = "exception_regex"
ENV_VARS_MATCHES: Final[str] = "env_vars_matches"

# RoutingRule constants
NAME: Final[str] = "name"
ENABLED: Final[str] = "enabled"
CHANNEL_NAMES: Final[str] = "channel_names"
CONDITIONS: Final[str] = "conditions"

# Triggers constants
RULES: Final[str] = "rules"


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


@dataclass
class RoutingRule(Model):
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
        except KeyError as e:
            raise ConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            name=name,
            enabled=enabled,
            channel_names=channel_names,
            conditions=conditions,
        )


@dataclass
class Triggers(Model):
    """Collection of trigger rules for alert processing.

    This class manages a collection of trigger rules that determine
    which channels should receive specific alerts based on their conditions.

    Attributes:
        rules: List of RoutingRule instances
    """

    rules: list[RoutingRule]

    @classmethod
    def from_dict(cls, dict_: Any) -> Self:
        """Create a Triggers instance from a dictionary or list configuration.

        Args:
            dict_: List of dictionaries containing trigger rule configurations,
                  or a dictionary with a 'rules' key containing the list

        Returns:
            A Triggers instance configured from the dictionary

        Examples:
            >>> config = [
            ...     {"name": "rule1", "enabled": True, ...},
            ...     {"name": "rule2", "enabled": False, ...}
            ... ]
            >>> triggers = Triggers.from_dict(config)
        """
        logger.debug("Creating Triggers from configuration")

        try:
            rules_config = dict_
            rules = [RoutingRule.from_dict(rule_dict) for rule_dict in rules_config]
        except KeyError as e:
            raise ConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(rules=rules)
