"""ETL implementation for managing ETL processes.

This module provides the ETL class, which is the central component of the ingestion framework.
It manages the Extract, Transform, and Load phases of the ETL pipeline, orchestrating
the data flow from source to destination.

Jobs can be created from configuration files or dictionaries, with automatic instantiation
of the appropriate Extract, Transform, and Load components based on the configuration.
"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Self

from flint.etl.core.job import Job
from flint.exceptions import FlintConfigurationKeyError
from flint.utils.file import FileHandlerContext
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

ETL: Final[str] = "etl"


@dataclass
class Etl:
    """ """

    jobs: list[Job]

    @classmethod
    def from_file(cls, filepath: Path) -> Self:
        """Create a ETL instance from a configuration file.

        Loads and parses a configuration file to create a ETL instance.
        Currently supports JSON configuration files.

        Args:
            filepath: Path to

        Returns:
            A fully configured ETL instance.
        """
        logger.info("Creating ETL from file: %s", filepath)

        handler = FileHandlerContext.from_filepath(filepath=filepath)
        file: dict[str, Any] = handler.read()

        etl = cls.from_dict(dict_=file)
        logger.info("Successfully created ETL from JSON file: %s", filepath)
        return etl

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a ETL instance from a dictionary configuration.

        Parses a dictionary containing job configuration to create the extracts,
        transforms, and loads components and assemble them into a ETL instance.

        Args:
            dict_: Dictionary containing job configuration data.

        Returns:
            A fully configured ETL instance.

        Raises:
            FlintConfigurationKeyError: If a required key is missing from the dictionary.
        """
        logger.debug("Creating ETL from dictionary with keys: %s", list(dict_.keys()))

        try:
            jobs: list[Job] = []

            for _, job_dict in enumerate(dict_[ETL]):
                job: Job = Job.from_dict(dict_=job_dict)
                jobs.append(job)

        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        etl = cls(jobs=jobs)
        logger.info("Successfully created ETL with %d jobs", len(etl.jobs))
        return etl

    def validate_all(self) -> None:
        """Validate all jobs in the ETL pipeline.

        Validates each job in the ETL instance by calling their validate method.
        Raises an exception if any job fails validation.
        """
        logger.info("Validating all %d jobs in ETL pipeline", len(self.jobs))

        for i, job in enumerate(self.jobs):
            logger.info("Validating job %d/%d: %s", i + 1, len(self.jobs), job.name)
            job.validate()

        logger.info("All jobs in ETL pipeline validated successfully")

    def execute_all(self) -> None:
        """Execute all jobs in the ETL pipeline.

        Executes each job in the ETL instance by calling their execute method.
        Raises an exception if any job fails during execution.
        """
        logger.info("Executing all %d jobs in ETL pipeline", len(self.jobs))

        for i, job in enumerate(self.jobs):
            logger.info("Executing job %d/%d: %s", i + 1, len(self.jobs), job.name)
            job.execute()

        logger.info("All jobs in ETL pipeline executed successfully")
