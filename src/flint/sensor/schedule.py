"""Schedule configuration for sensor management.

This module provides the Schedule dataclass for managing cron-like scheduling
configuration for sensors. It follows the from_dict pattern consistent with
other components in the Flint framework.
"""

import logging
from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import FlintConfigurationKeyError
from flint.job.models import Model
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

# Schedule constants
EXPRESSION: Final[str] = "expression"
TIMEZONE: Final[str] = "timezone"


@dataclass
class SensorSchedule(Model):
    """Configuration for sensor scheduling.

    This class manages the schedule configuration for sensors using cron-like
    expressions with timezone support.

    Attributes:
        expression: Cron expression defining when the sensor should run
        timezone: Timezone for the cron expression evaluation
    """

    expression: str
    timezone: str

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a Schedule instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing schedule configuration with keys:
                  - expression: Cron expression for scheduling
                  - timezone: Timezone for schedule evaluation

        Returns:
            A Schedule instance configured from the dictionary

        Raises:
            FlintConfigurationKeyError: If a required key is missing.

        Examples:
            >>> config = {
            ...     "expression": "*/5 * * * *",
            ...     "timezone": "UTC"
            ... }
            >>> schedule = Schedule.from_dict(config)
        """
        logger.debug("Creating Schedule from configuration dictionary")

        try:
            expression = dict_[EXPRESSION]
            timezone = dict_[TIMEZONE]
        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            expression=expression,
            timezone=timezone,
        )
