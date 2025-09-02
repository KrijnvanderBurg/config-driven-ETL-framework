"""Base action configuration for sensor management.

This module provides the base classes for sensor action configurations
supporting different action types like HTTP POST and job triggering.
It follows the from_dict pattern consistent with other components in the Flint framework.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Final, Self

from flint.etl.models import Model
from flint.exceptions import FlintConfigurationKeyError
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


@dataclass
class BaseAction(Model, ABC):
    """Base configuration for sensor actions.

    This class serves as a base for all action configurations, providing
    common interface for action implementations.
    """

    def execute(self) -> None:
        """Execute the action.

        Raises:
            NotImplementedError: Must be implemented by subclasses.
        """
        logger.debug("Executing action: %s", self.__class__.__name__)
        self._execute()
        logger.info("Action executed: %s", self.__class__.__name__)

    @abstractmethod
    def _execute(self) -> None:
        """Internal method to handle action execution logic.

        This method should implement the actual logic for executing
        the action. It is expected to be implemented by subclasses.
        """
        raise NotImplementedError("_execute must be implemented by subclasses.")


# Action constants
NAME: Final[str] = "name"
TYPE: Final[str] = "type"
CONFIG: Final[str] = "config"


@dataclass
class SensorAction(Model):
    """Wrapper configuration for sensor actions.

    This class manages action configuration for sensors, supporting
    different action types like HTTP POST and job triggering.

    Attributes:
        name: Unique name for the action
        type: Type of action (http_post, trigger_job, etc.)
        config: The actual action implementation instance
    """

    name: str
    type: str
    config: BaseAction

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a SensorAction instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing action configuration with keys:
                  - name: Unique name for the action
                  - type: Type of action
                  - config: Action-specific configuration

        Returns:
            A SensorAction instance configured from the dictionary

        Raises:
            FlintConfigurationKeyError: If a required key is missing.
            ValueError: If an unknown action type is specified.

        Examples:
            >>> config = {
            ...     "name": "notify-files-ready",
            ...     "type": "http_post",
            ...     "config": {...}
            ... }
            >>> action = SensorAction.from_dict(config)
        """
        logger.debug("Creating SensorAction from configuration dictionary")

        try:
            name = dict_[NAME]
            type_ = dict_[TYPE]
            config_dict = dict_[CONFIG]
        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        # Create the appropriate action instance
        config: BaseAction
        if type_ == "http_post":
            from flint.sensor.actions.http_post import HttpPostAction

            config = HttpPostAction.from_dict(config_dict)
        elif type_ == "trigger_job":
            from flint.sensor.actions.etl_in_sensor import EtlInSensor

            config = EtlInSensor.from_dict(config_dict)
        else:
            raise ValueError(f"Unknown action type: {type_}")

        return cls(
            name=name,
            type=type_,
            config=config,
        )
