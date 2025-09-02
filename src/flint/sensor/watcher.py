"""Base watcher configuration for sensor management.

This module provides the base Watcher dataclass for sensor watcher configurations
supporting different watcher types like file system and HTTP polling watchers.
It follows the from_dict pattern consistent with other components in the Flint framework.
"""

import logging
from dataclasses import dataclass
from typing import Any, Final, Self

from flint.etl.models import Model
from flint.exceptions import FlintConfigurationKeyError
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

# Watcher constants
NAME: Final[str] = "name"
TYPE: Final[str] = "type"
ENABLED: Final[str] = "enabled"
CONFIG: Final[str] = "config"
TRIGGER_ACTIONS: Final[str] = "trigger_actions"


@dataclass
class SensorWatcher(Model):
    """Base configuration for sensor watchers.

    This class manages watcher configuration for sensors, supporting
    different watcher types like file system and HTTP polling watchers.

    Attributes:
        name: Unique name for the watcher
        type: Type of watcher (file_system, http_polling, etc.)
        enabled: Whether this watcher is currently active
        config: Configuration dictionary specific to the watcher type
        trigger_actions: List of action names to trigger when conditions are met
    """

    name: str
    type: str
    enabled: bool
    config: dict[str, Any]
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
            config = dict_[CONFIG]
            trigger_actions = dict_[TRIGGER_ACTIONS]
        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            name=name,
            type=type_,
            enabled=enabled,
            config=config,
            trigger_actions=trigger_actions,
        )
