"""ETL job triggering action configuration for sensor management.

This module provides dataclasses for ETL job triggering action configurations.
It follows the from_dict pattern consistent with other components in the Flint framework.
"""

import logging
from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import FlintConfigurationKeyError
from flint.sensor.actions.base import BaseAction
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

# ETL job trigger action constants
JOB_NAMES: Final[str] = "job_names"


@dataclass
class EtlInSensor(BaseAction):
    """Configuration for ETL job triggering actions.

    This class manages ETL job triggering action configuration,
    specifying which jobs should be triggered in order.

    Attributes:
        job_names: Ordered list of job names to trigger
    """

    job_names: list[str]

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create an EtlInSensor instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing ETL job trigger configuration with keys:
                  - job_names: Ordered list of job names to trigger

        Returns:
            An EtlInSensor instance configured from the dictionary

        Raises:
            FlintConfigurationKeyError: If a required key is missing.

        Examples:
            >>> config = {
            ...     "job_names": ["extract-data", "transform-data", "load-data"]
            ... }
            >>> action = EtlInSensor.from_dict(config)
        """
        logger.debug("Creating EtlInSensor from configuration dictionary")

        try:
            job_names = dict_[JOB_NAMES]
        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            job_names=job_names,
        )

    def _execute(self) -> None:
        """Execute the ETL job triggering action.

        This method would implement the actual ETL job triggering logic.
        """
        # Implementation would trigger the specified jobs in order
        # This is just a placeholder for the actual implementation
        logger.debug("Triggering ETL jobs in order: %s", self.job_names)
