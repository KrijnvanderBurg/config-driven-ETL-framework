"""Job implementation for managing ETL processes.

This module provides the Job class, which is the central component of the ingestion framework.
It manages the Extract, Transform, and Load phases of the ETL pipeline, orchestrating
the data flow from source to destination.

Jobs can be created from configuration files or dictionaries, with automatic instantiation
of the appropriate Extract, Transform, and Load components based on the configuration.
"""

import logging
import time

from pydantic import Field
from typing_extensions import override

from flint.runtime.jobs.models.model_job import JobBase, JobEngine
from flint.runtime.jobs.spark.extract import ExtractSparkUnion
from flint.runtime.jobs.spark.load import LoadSparkUnion
from flint.runtime.jobs.spark.transform import TransformSparkUnion
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class JobSpark(JobBase):
    """A complete ETL job that orchestrates extract, transform, and load operations.

    The Job class is the main entry point for the ingestion framework. It coordinates
    the execution of extraction, transformation, and loading operations in sequence.
    Jobs can be constructed from configuration files or dictionaries, making it easy
    to define pipelines without code changes.

    Attributes:
        id (str): unique identifier of the etl job
        extracts (list[Extract]): Collection of Extract components to obtain data from sources.
        transforms (list[Transform]): Collection of Transform components to process the data.
        loads (list[Load]): Collection of Load components to write data to destinations.

    Example:
        ```python
        from pathlib import Path
        from flint.runtime.etl.spark.job import Job

        # Create from a configuration file
        job = Job.from_file(Path("config.json"))

        # Execute the ETL pipeline
        job.execute()
        ```
    """

    engine_type: JobEngine = JobEngine.SPARK
    extracts: list[ExtractSparkUnion] = Field(..., description="Collection of Extract components")
    transforms: list[TransformSparkUnion] = Field(
        ..., description="Collection of Transform components to process the data"
    )
    loads: list[LoadSparkUnion] = Field(..., description="Collection of Load components")

    @override
    def _execute(self) -> None:
        """Execute the engine-specific ETL pipeline logic.

        Executes the three phases of the ETL pipeline in sequence:
        1. Extract: Retrieve data from sources
        2. Transform: Apply transformations to the data
        3. Load: Write transformed data to destinations
        """
        start_time = time.time()
        logger.info(
            "Starting Spark job execution with %d extracts, %d transforms, %d loads",
            len(self.extracts),
            len(self.transforms),
            len(self.loads),
        )

        self._extract()
        self._transform()
        self._load()

        execution_time = time.time() - start_time
        logger.info("Spark job completed in %.2f seconds", execution_time)

    def _extract(self) -> None:
        """Execute the extraction phase of the ETL pipeline.

        Calls the extract method on each configured extract component,
        retrieving data from the specified sources.
        """
        logger.info("Starting extract phase with %d extractors", len(self.extracts))
        start_time = time.time()

        for i, extract in enumerate(self.extracts):
            extract_start_time = time.time()
            logger.debug("Running extractor %d/%d: %s", i, len(self.extracts), extract.id_)
            extract.extract()
            extract_time = time.time() - extract_start_time
            logger.debug("Extractor %s completed in %.2f seconds", extract.id_, extract_time)

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
            logger.debug("Running transformer %d/%d: %s", i, len(self.transforms), transform.id_)
            transform.transform()
            transform_time = time.time() - transform_start_time
            logger.debug("Transformer %s completed in %.2f seconds", transform.id_, transform_time)

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
            logger.debug("Running loader %d/%d: %s", i, len(self.loads), load.id_)
            load.load()
            load_time = time.time() - load_start_time
            logger.debug("Loader %s completed in %.2f seconds", load.id_, load_time)

        phase_time = time.time() - start_time
        logger.info("Load phase completed successfully in %.2f seconds", phase_time)
