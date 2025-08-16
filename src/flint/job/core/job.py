"""Job implementation for managing ETL processes.

This module provides the Job class, which is the central component of the ingestion framework.
It manages the Extract, Transform, and Load phases of the ETL pipeline, orchestrating
the data flow from source to destination.

Jobs can be created from configuration files or dictionaries, with automatic instantiation
of the appropriate Extract, Transform, and Load components based on the configuration.
"""

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Self

from flint.core.extract import Extract
from flint.core.load import Load
from flint.core.transform import Transform
from flint.core.validation import ValidateModelNamesAreUnique, ValidateUpstreamNamesExist
from flint.exceptions import FlintConfigurationKeyError
from flint.utils.file import FileHandlerContext
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

JOB: Final[str] = "job"
EXTRACTS: Final[str] = "extracts"
TRANSFORMS: Final[str] = "transforms"
LOADS: Final[str] = "loads"


@dataclass
class Job:
    """A complete ETL job that orchestrates extract, transform, and load operations.

    The Job class is the main entry point for the ingestion framework. It coordinates
    the execution of extraction, transformation, and loading operations in sequence.
    Jobs can be constructed from configuration files or dictionaries, making it easy
    to define pipelines without code changes.

    Attributes:
        extracts (list[Extract]): Collection of Extract components to obtain data from sources.
        transforms (list[Transform]): Collection of Transform components to process the data.
        loads (list[Load]): Collection of Load components to write data to destinations.

    Example:
        ```python
        from pathlib import Path
        from flint.core.job import Job

        # Create from a configuration file
        job = Job.from_file(Path("config.json"))

        # Execute the ETL pipeline
        job.execute()
        ```
    """

    extracts: list[Extract]
    transforms: list[Transform]
    loads: list[Load]

    @classmethod
    def from_file(cls, filepath: Path) -> Self:
        """Create a Job instance from a configuration file.

        Loads and parses a configuration file to create a Job instance.
        Currently supports JSON configuration files.

        Args:
            filepath: Path to the configuration file.

        Returns:
            A fully configured Job instance.
        """
        logger.info("Creating Job from file: %s", filepath)

        handler = FileHandlerContext.from_filepath(filepath=filepath)
        file: dict[str, Any] = handler.read()

        job = cls.from_dict(dict_=file)
        logger.info("Successfully created Job from JSON file: %s", filepath)
        return job

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a Job instance from a dictionary configuration.

        Parses a dictionary containing job configuration to create the extracts,
        transforms, and loads components and assemble them into a Job instance.

        Args:
            dict_: Dictionary containing job configuration data.

        Returns:
            A fully configured Job instance.

        Raises:
            ConfigurationKeyError: If a required key is missing from the dictionary.
        """
        logger.debug("Creating Job from dictionary with keys: %s", list(dict_.keys()))

        try:
            job_config = dict_[JOB]
            logger.debug("Processing job configuration with keys: %s", list(job_config.keys()))

            extracts: list[Extract] = []
            extract_configs: list[dict] = job_config[EXTRACTS]
            logger.debug("Processing %d extract configurations", len(extract_configs))

            for i, extract_dict in enumerate(extract_configs):
                logger.debug(
                    "Creating extract %d/%d: %s", i + 1, len(extract_configs), extract_dict.get("name", "unnamed")
                )
                extract: Extract = Extract.from_dict(dict_=extract_dict)
                extracts.append(extract)

            transforms: list[Transform] = []
            transform_configs: list[dict] = job_config[TRANSFORMS]
            logger.debug("Processing %d transform configurations", len(transform_configs))

            for i, transform_dict in enumerate(transform_configs):
                logger.debug(
                    "Creating transform %d/%d: %s", i + 1, len(transform_configs), transform_dict.get("name", "unnamed")
                )
                transform: Transform = Transform.from_dict(dict_=transform_dict)
                transforms.append(transform)

            loads: list[Load] = []
            load_configs: list[dict] = job_config[LOADS]
            logger.debug("Processing %d load configurations", len(load_configs))

            for i, load_dict in enumerate(load_configs):
                logger.debug("Creating load %d/%d: %s", i + 1, len(load_configs), load_dict.get("name", "unnamed"))
                load: Load = Load.from_dict(dict_=load_dict)
                loads.append(load)

        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        job = cls(extracts=extracts, transforms=transforms, loads=loads)
        logger.info(
            "Successfully created Job with %d extracts, %d transforms, %d loads",
            len(extracts),
            len(transforms),
            len(loads),
        )
        return job

    def validate(self) -> None:
        """Validate the job configuration."""
        logger.info("Starting job configuration validation")

        ValidateModelNamesAreUnique(data=self)
        ValidateUpstreamNamesExist(data=self)

        logger.info("Job configuration validated successfully")

    def execute(self) -> None:
        """Execute the complete ETL pipeline.

        Runs the extract, transform, and load phases in sequence.
        This is the main entry point for running a configured job.
        """
        start_time = time.time()
        logger.info(
            "Starting job execution with %d extracts, %d transforms, %d loads",
            len(self.extracts),
            len(self.transforms),
            len(self.loads),
        )

        self._extract()
        self._transform()
        self._load()

        execution_time = time.time() - start_time
        logger.info("Job completed successfully in %.2f seconds", execution_time)

    def _extract(self) -> None:
        """Execute the extraction phase of the ETL pipeline.

        Calls the extract method on each configured extract component,
        retrieving data from the specified sources.
        """
        logger.info("Starting extract phase with %d extractors", len(self.extracts))
        start_time = time.time()

        for i, extract in enumerate(self.extracts):
            extract_start_time = time.time()
            logger.debug("Running extractor %d/%d: %s", i, len(self.extracts), extract.model.name)
            extract.extract()
            extract_time = time.time() - extract_start_time
            logger.debug("Extractor %s completed in %.2f seconds", extract.model.name, extract_time)

        phase_time = time.time() - start_time
        logger.info("Extract phase completed successfully in %.2f seconds", phase_time)

    def _transform(self) -> None:
        """Execute the transformation phase of the ETL pipeline.

        Copies data from upstream components to the current transform component
        and applies the transformation operations to modify the data.
        """
        logger.info("Starting transform phase with %d transformers", len(self.transforms))
        start_time = time.time()

        for i, transform in enumerate(self.transforms):
            transform_start_time = time.time()
            logger.debug("Running transformer %d/%d: %s", i, len(self.transforms), transform.model.name)
            transform.transform()
            transform_time = time.time() - transform_start_time
            logger.debug("Transformer %s completed in %.2f seconds", transform.model.name, transform_time)

        phase_time = time.time() - start_time
        logger.info("Transform phase completed successfully in %.2f seconds", phase_time)

    def _load(self) -> None:
        """Execute the loading phase of the ETL pipeline.

        Copies data from upstream components to the current load component
        and writes the transformed data to the target destinations.
        """
        logger.info("Starting load phase with %d loaders", len(self.loads))
        start_time = time.time()

        for i, load in enumerate(self.loads):
            load_start_time = time.time()
            logger.debug("Running loader %d/%d: %s", i, len(self.loads), load.model.name)
            load.load()
            load_time = time.time() - load_start_time
            logger.debug("Loader %s completed in %.2f seconds", load.model.name, load_time)

        phase_time = time.time() - start_time
        logger.info("Load phase completed successfully in %.2f seconds", phase_time)
