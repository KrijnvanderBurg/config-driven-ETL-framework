"""Job implementation for managing ETL processes.

This module provides the Job class, which is the central component of the ingestion framework.
It manages the Extract, Transform, and Load phases of the ETL pipeline, orchestrating
the data flow from source to destination.

Jobs can be created from configuration files or dictionaries, with automatic instantiation
of the appropriate Extract, Transform, and Load components based on the configuration.
"""

import logging
import time
from typing import Self

from pydantic import model_validator
from typing_extensions import override

from samara.runtime.jobs.models.model_job import JobBase, JobEngine
from samara.runtime.jobs.spark.extract import ExtractSparkUnion
from samara.runtime.jobs.spark.load import LoadSparkUnion
from samara.runtime.jobs.spark.transform import TransformSparkUnion
from samara.types import DataFrameRegistry, StreamingQueryRegistry
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class JobSpark(JobBase[ExtractSparkUnion, TransformSparkUnion, LoadSparkUnion]):
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
        from samara.runtime.etl.spark.job import Job

        # Create from a configuration file
        job = Job.from_file(Path("config.json"))

        # Execute the ETL pipeline
        job.execute()
        ```
    """

    engine_type: JobEngine = JobEngine.SPARK

    @model_validator(mode="after")
    def validate_join_upstream_references(self) -> Self:
        """Validate join transform other_upstream_id references exist and are in correct order.

        Ensures that:
        - Join transform other_upstream_ids reference existing extract or previously defined transform IDs
        - Join transforms cannot reference themselves
        - Join transforms can only reference transforms that appear before them in the list

        Returns:
            Self: The validated instance.

        Raises:
            ValueError: If invalid other_upstream_id references are found in join functions.
        """
        extract_ids = {extract.id_ for extract in self.extracts}
        valid_upstream_ids_for_transforms = extract_ids.copy()

        for transform in self.transforms:
            # Validate join function's other_upstream_id if present
            for function in transform.functions:
                if function.function_type == "join":
                    other_upstream_id = function.arguments.other_upstream_id
                    # Check if join references the transform itself
                    if other_upstream_id == transform.id_:
                        raise ValueError(
                            f"Transform '{transform.id_}' has join function with other_upstream_id '{other_upstream_id}' "
                            f"in job '{self.id_}' which references its own id. "
                            f"A join cannot reference the same transform."
                        )
                    # Check if other_upstream_id exists in extracts or previously defined transforms
                    if other_upstream_id not in valid_upstream_ids_for_transforms:
                        raise ValueError(
                            f"Transform '{transform.id_}' has join function with other_upstream_id '{other_upstream_id}' "
                            f"in job '{self.id_}' which either does not exist or is defined later in the transforms list. "
                            f"other_upstream_id must reference an existing extract or a transform that appears before this one."
                        )

            # Add current transform ID to valid upstream IDs for subsequent transforms
            valid_upstream_ids_for_transforms.add(transform.id_)

        return self

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

    @override
    def _clear(self) -> None:
        """Clear Spark-specific registries to free memory.

        Clears all DataFrames and streaming queries from the registries after
        job execution. This ensures memory is freed and prevents data leakage
        between jobs.
        """
        logger.debug("Clearing DataFrameRegistry after job: %s", self.id_)
        DataFrameRegistry().clear()

        logger.debug("Clearing StreamingQueryRegistry after job: %s", self.id_)
        StreamingQueryRegistry().clear()
