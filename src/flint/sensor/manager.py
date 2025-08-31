"""Sensor manager for handling sensor configurations and scheduling.

This module provides the main SensorManager class that orchestrates sensor
processing including scheduling, watchers, and actions. It serves as the root object
for the sensor system, managing schedules, watchers, and action configurations.

The SensorManager uses the from_dict classmethod pattern consistent with other
components in the Flint framework to create instances from configuration data.
"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Self

from flint.exceptions import FlintConfigurationKeyError
from flint.job.models import Model
from flint.sensor.actions.base import SensorAction
from flint.sensor.schedule import Schedule
from flint.sensor.watchers.base import Watcher
from flint.utils.file import FileHandlerContext
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

SENSOR: Final[str] = "sensor"
SCHEDULE: Final[str] = "schedule"
WATCHERS: Final[str] = "watchers"
ACTIONS: Final[str] = "actions"


@dataclass
class SensorManager(Model):
    """Main sensor manager that coordinates sensor processing and scheduling.

    This class serves as the root object for the sensor system, managing the
    configuration and coordination of schedules, watchers, and actions.
    It implements the Model interface to support configuration-driven initialization.

    Attributes:
        schedule: Cron-like scheduling configuration for the sensor
        watchers: List of watchers that monitor different conditions
        actions: List of actions that can be triggered by watchers
    """

    schedule: Schedule
    watchers: list[Watcher]
    actions: list[SensorAction]

    @classmethod
    def from_file(cls, filepath: Path) -> Self:
        """Create a SensorManager instance from a configuration file.

        Loads and parses a configuration file to create a SensorManager instance.

        Args:
            filepath: Path to the configuration file.

        Returns:
            A fully configured SensorManager instance.
        """
        logger.info("Creating SensorManager from file: %s", filepath)

        handler = FileHandlerContext.from_filepath(filepath=filepath)
        dict_: dict[str, Any] = handler.read()

        sensor = cls.from_dict(dict_=dict_)
        logger.info("Successfully created SensorManager from JSON file: %s", filepath)
        return sensor

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a SensorManager instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing sensor configuration with keys:
                  - schedule: Schedule configuration for the sensor
                  - watchers: Watcher configurations for monitoring
                  - actions: Action configurations for triggering

        Returns:
            A SensorManager instance configured from the dictionary

        Raises:
            FlintConfigurationKeyError: If required configuration keys are missing

        Examples:
            >>> config = {
            ...     "schedule": {...},
            ...     "watchers": [...],
            ...     "actions": [...]
            ... }
            >>> manager = SensorManager.from_dict(config)
        """
        logger.debug("Creating SensorManager from configuration dictionary")

        try:
            sensor_dict = dict_[SENSOR]
            schedule = Schedule.from_dict(sensor_dict[SCHEDULE])

            watchers: list[Watcher] = []
            for watcher_dict in sensor_dict[WATCHERS]:
                watcher = Watcher.from_dict(watcher_dict)
                watchers.append(watcher)

            actions: list[SensorAction] = []
            for action_dict in sensor_dict[ACTIONS]:
                action = SensorAction.from_dict(action_dict)
                actions.append(action)
        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            schedule=schedule,
            watchers=watchers,
            actions=actions,
        )
