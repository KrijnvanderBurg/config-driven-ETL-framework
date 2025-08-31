"""Base action configuration for sensor management.

This module provides the base SensorAction dataclass for sensor action configurations
supporting different action types like HTTP POST and job triggering.
It follows the from_dict pattern consistent with other components in the Flint framework.
"""

import logging
from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import FlintConfigurationKeyError
from flint.job.models import Model
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

# Action constants
NAME: Final[str] = "name"
TYPE: Final[str] = "type"
CONFIG: Final[str] = "config"


@dataclass
class SensorAction(Model):
    """Base configuration for sensor actions.

    This class manages action configuration for sensors, supporting
    different action types like HTTP POST and job triggering.

    Attributes:
        name: Unique name for the action
        type: Type of action (http_post, trigger_job, etc.)
        config: Configuration dictionary specific to the action type
    """

    name: str
    type: str
    config: dict[str, Any]

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
            config = dict_[CONFIG]
        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            name=name,
            type=type_,
            config=config,
        )
