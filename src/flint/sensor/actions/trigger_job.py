"""Trigger job action configuration for sensor management.

This module provides dataclasses for job triggering action configurations.
It follows the from_dict pattern consistent with other components in the Flint framework.
"""

import logging
from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import FlintConfigurationKeyError
from flint.sensor.actions.base import BaseAction
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

# Trigger job action constants
JOB_NAME: Final[str] = "job_name"


@dataclass
class TriggerJobAction(BaseAction):
    """Configuration for job triggering actions.

    This class manages job triggering action configuration,
    specifying which job should be triggered.

    Attributes:
        job_name: Name of the job to trigger
    """

    job_name: str

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a TriggerJobAction instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing job trigger configuration with keys:
                  - job_name: Name of the job to trigger

        Returns:
            A TriggerJobAction instance configured from the dictionary

        Raises:
            FlintConfigurationKeyError: If a required key is missing.

        Examples:
            >>> config = {
            ...     "job_name": "process-data-files"
            ... }
            >>> action = TriggerJobAction.from_dict(config)
        """
        logger.debug("Creating TriggerJobAction from configuration dictionary")

        try:
            job_name = dict_[JOB_NAME]
        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            job_name=job_name,
        )

    def _execute(self) -> None:
        """Execute the job triggering action.

        This method would implement the actual job triggering logic.
        """
        # Implementation would trigger the specified job
        # This is just a placeholder for the actual implementation
        logger.debug("Triggering job: %s", self.job_name)
