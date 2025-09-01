"""Base watcher configuration for sensor management.

This module provides the base classes for sensor watcher configurations
supporting different watcher types like file system and HTTP polling watchers.
It follows the from_dict pattern consistent with other components in the Flint framework.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import FlintConfigurationKeyError
from flint.job.models import Model
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


@dataclass
class BaseWatcher(Model, ABC):
    """Base configuration for sensor watchers.

    This class serves as a base for all watcher configurations, providing
    common interface for watcher implementations.
    """

    def check_conditions(self) -> bool:
        """Check if the watcher conditions are met.

        Returns:
            True if conditions are met and actions should be triggered, False otherwise.

        Raises:
            NotImplementedError: Must be implemented by subclasses.
        """
        logger.debug("Checking conditions for watcher: %s", self.__class__.__name__)
        result = self._check_conditions()
        logger.debug("Conditions check result for %s: %s", self.__class__.__name__, result)
        return result

    @abstractmethod
    def _check_conditions(self) -> bool:
        """Internal method to handle condition checking logic.

        This method should implement the actual logic for checking
        if the watcher conditions are met. It is expected to be implemented by subclasses.

        Returns:
            True if conditions are met, False otherwise.
        """
        raise NotImplementedError("_check_conditions must be implemented by subclasses.")


# Watcher constants
NAME: Final[str] = "name"
TYPE: Final[str] = "type"
ENABLED: Final[str] = "enabled"
CONFIG: Final[str] = "config"
TRIGGER_ACTIONS: Final[str] = "trigger_actions"


@dataclass
class Watcher(Model):
    """Wrapper configuration for sensor watchers.

    This class manages watcher configuration for sensors, supporting
    different watcher types like file system and HTTP polling watchers.

    Attributes:
        name: Unique name for the watcher
        type: Type of watcher (file_system, http_polling, etc.)
        enabled: Whether this watcher is currently active
        config: The actual watcher implementation instance
        trigger_actions: List of action names to trigger when conditions are met
    """

    name: str
    type: str
    enabled: bool
    config: BaseWatcher
    trigger_actions: list[str]

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a Watcher instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing watcher configuration with keys:
                  - name: Unique name for the watcher
                  - type: Type of watcher
                  - enabled: Whether the watcher is active
                  - config: Watcher-specific configuration
                  - trigger_actions: List of action names to trigger

        Returns:
            A Watcher instance configured from the dictionary

        Raises:
            FlintConfigurationKeyError: If a required key is missing.
            ValueError: If an unknown watcher type is specified.

        Examples:
            >>> config = {
            ...     "name": "file-watcher",
            ...     "type": "file_system",
            ...     "enabled": True,
            ...     "config": {...},
            ...     "trigger_actions": ["notify-files-ready"]
            ... }
            >>> watcher = Watcher.from_dict(config)
        """
        logger.debug("Creating Watcher from configuration dictionary")

        try:
            name = dict_[NAME]
            type_ = dict_[TYPE]
            enabled = dict_[ENABLED]
            config_dict = dict_[CONFIG]
            trigger_actions = dict_[TRIGGER_ACTIONS]
        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        # Create the appropriate watcher instance
        config: BaseWatcher
        if type_ == "file_system":
            from flint.sensor.watchers.file_system import FileSystemWatcher

            config = FileSystemWatcher.from_dict(config_dict)
        elif type_ == "http_polling":
            from flint.sensor.watchers.http_polling import HttpPollingWatcher

            config = HttpPollingWatcher.from_dict(config_dict)
        else:
            raise ValueError(f"Unknown watcher type: {type_}")

        return cls(
            name=name,
            type=type_,
            enabled=enabled,
            config=config,
            trigger_actions=trigger_actions,
        )
