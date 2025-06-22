"""Job implementation for managing ETL processes.

This module provides the Job class, which is the central component of the ingestion framework.
It manages the Extract, Transform, and Load phases of the ETL pipeline, orchestrating
the data flow from source to destination.

Jobs can be created from configuration files or dictionaries, with automatic instantiation
of the appropriate Extract, Transform, and Load components based on the configuration.
"""

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Self

from flint.core.extract import Extract
from flint.core.load import Load
from flint.core.transform import Transform
from flint.core.validation import ValidateModelNamesAreUnique
from flint.exceptions import DictKeyError
from flint.utils.file import FileHandlerContext
from flint.utils.logger import set_logger

logger = set_logger(__name__)

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

        Raises:
            NotImplementedError: If the file format is not supported.
        """
        handler = FileHandlerContext.from_filepath(filepath=filepath)
        file: dict[str, Any] = handler.read()

        if Path(filepath).suffix == ".json":
            return cls.from_dict(dict_=file)

        raise NotImplementedError("No handling options found.")

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
            DictKeyError: If a required key is missing from the dictionary.
        """
        try:
            extracts: list[Extract] = []
            for extract_dict in dict_[EXTRACTS]:
                extract: Extract = Extract.from_dict(dict_=extract_dict)
                extracts.append(extract)

            transforms: list[Transform] = []
            for transform_dict in dict_[TRANSFORMS]:
                transform: Transform = Transform.from_dict(dict_=transform_dict)
                transforms.append(transform)

            loads: list[Load] = []
            for load_dict in dict_[LOADS]:
                load: Load = Load.from_dict(dict_=load_dict)
                loads.append(load)
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=dict_) from e

        return cls(extracts=extracts, transforms=transforms, loads=loads)

    def validate(self) -> None:
        """Validate the job configuration."""
        ValidateModelNamesAreUnique(data=self)
        logger.info("Job configuration validated successfully")

    def execute(self) -> None:
        """Execute the complete ETL pipeline.

        Runs the extract, transform, and load phases in sequence.
        This is the main entry point for running a configured job.
        """
        start_time = time.time()
        logger.info("Starting job execution")

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

        for i, extract in enumerate(self.extracts, 1):
            logger.debug("Running extractor %d/%d: %s", i, len(self.extracts), extract.model.name)
            extract.extract()

    def _transform(self) -> None:
        """Execute the transformation phase of the ETL pipeline.

        Copies data from upstream components to the current transform component
        and applies the transformation operations to modify the data.
        """
        logger.info("Starting transform phase with %d transformers", len(self.transforms))

        for i, transform in enumerate(self.transforms, 1):
            logger.debug("Running transformer %d/%d: %s", i, len(self.transforms), transform.model.name)
            transform.transform()

    def _load(self) -> None:
        """Execute the loading phase of the ETL pipeline.

        Copies data from upstream components to the current load component
        and writes the transformed data to the target destinations.
        """
        logger.info("Starting load phase with %d loaders", len(self.loads))

        for i, load in enumerate(self.loads, 1):
            logger.debug("Running loader %d/%d: %s", i, len(self.loads), load.model.name)
            load.load()
