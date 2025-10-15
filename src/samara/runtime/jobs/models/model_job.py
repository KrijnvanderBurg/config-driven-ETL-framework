"""Job models for the Samara ETL framework.

This module provides the base job models and discriminated union for different
engine types. It includes:
    - Base job model with common attributes
    - Engine-specific job implementations
    - Discriminated union using Pydantic's discriminator feature
"""

import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import Generic, Self, TypeVar

from pydantic import Field, model_validator

from samara import BaseModel
from samara.exceptions import FlintJobError
from samara.runtime.jobs.hooks import Hooks
from samara.runtime.jobs.models.model_extract import ExtractModel
from samara.runtime.jobs.models.model_load import LoadModel
from samara.runtime.jobs.models.model_transform import TransformModel
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

ExtractT = TypeVar("ExtractT", bound=ExtractModel)
TransformT = TypeVar("TransformT", bound=TransformModel)
LoadT = TypeVar("LoadT", bound=LoadModel)


class JobEngine(Enum):
    """Enumeration of supported job engines.

    Defines the different execution engines that can be used to run ETL jobs.
    This enum is used as the discriminator for job type selection.
    """

    SPARK = "spark"
    # Future engines can be added here:
    # POLARS = "polars"


class JobBase(BaseModel, ABC, Generic[ExtractT, TransformT, LoadT]):
    """Abstract base class for all job types.

    Defines the common interface and attributes that all job implementations
    must provide, regardless of the underlying execution engine.

    This class handles:
    - Job enabled/disabled state checking
    - Hook execution at appropriate lifecycle points (onStart, onError, onSuccess, onFinally)
    - Exception handling and wrapping in FlintJobError

    Subclasses only need to implement the _execute() method with engine-specific logic.

    Attributes:
        id: Unique identifier for the job
        description: Human-readable description of the job's purpose
        enabled: Whether this job should be executed
        engine_type: The execution engine to use for this job
        hooks: Hooks to execute at various stages of the job lifecycle
        extracts: Collection of Extract components to obtain data from sources
        transforms: Collection of Transform components to process the data
        loads: Collection of Load components to write data to destinations
    """

    id_: str = Field(..., alias="id", description="Unique identifier for the job", min_length=1)
    description: str = Field(..., description="Human-readable description of the job's purpose")
    enabled: bool = Field(..., description="Whether this job should be executed")
    engine_type: JobEngine = Field(..., description="The execution engine to use for this job")
    extracts: list[ExtractT] = Field(..., description="Collection of Extract components")
    transforms: list[TransformT] = Field(..., description="Collection of Transform components")
    loads: list[LoadT] = Field(..., description="Collection of Load components")
    hooks: Hooks = Field(..., description="Hooks to execute at various stages of the job lifecycle")

    @model_validator(mode="after")
    def validate_unique_ids(self) -> Self:
        """Validate all IDs are unique within the job.

        Ensures that all extract, transform, and load IDs are unique within this job.

        Returns:
            Self: The validated instance.

        Raises:
            ValueError: If duplicate IDs are found.
        """
        # Collect all IDs as lists (not sets) to detect duplicates
        extract_ids_list = [extract.id_ for extract in self.extracts]
        transform_ids_list = [transform.id_ for transform in self.transforms]
        load_ids_list = [load.id_ for load in self.loads]

        # Validate unique IDs within the job
        all_ids = extract_ids_list + transform_ids_list + load_ids_list
        duplicates = {id_ for id_ in all_ids if all_ids.count(id_) > 1}
        if duplicates:
            raise ValueError(f"Duplicate IDs found in job '{self.id_}': {', '.join(sorted(duplicates))}")

        return self

    @model_validator(mode="after")
    def validate_upstream_references(self) -> Self:
        """Validate all upstream_id references exist and are in the correct order.

        Ensures that:
        - All transform upstream_ids reference existing extract or previously defined transform IDs
        - Transforms cannot reference themselves
        - Transforms can only reference transforms that appear before them in the list
        - All load upstream_ids reference existing extract or transform IDs
        - Join transform other_upstream_ids reference existing extract or previously defined transform IDs

        Returns:
            Self: The validated instance.

        Raises:
            ValueError: If invalid upstream_id references are found.
        """
        # Convert to sets for upstream reference validation
        extract_ids = {extract.id_ for extract in self.extracts}

        # Validate transform upstream_ids reference existing extracts or previously defined transforms
        # Build valid upstream IDs progressively as we process transforms in order
        valid_upstream_ids_for_transforms = extract_ids.copy()
        for transform in self.transforms:
            # Check if transform references itself
            if transform.upstream_id == transform.id_:
                raise ValueError(
                    f"Transform '{transform.id_}' references itself as upstream_id "
                    f"in job '{self.id_}'. A transform cannot reference its own id."
                )

            # Check if upstream_id exists in extracts or previously defined transforms
            if transform.upstream_id not in valid_upstream_ids_for_transforms:
                raise ValueError(
                    f"Transform '{transform.id_}' references upstream_id '{transform.upstream_id}' "
                    f"in job '{self.id_}' which either does not exist or is defined later in the transforms list. "
                    f"upstream_id must reference an existing extract or a transform that appears before this one."
                )

            # Validate join function other_upstream_id references if transform has functions
            # Check if the transform model has a functions field using Pydantic's model_fields
            # This is the proper way to introspect Pydantic model fields without using hasattr/getattr
            if "functions" in transform.model_fields:
                functions = transform.functions
                for function in functions:
                    # Check if this is a join function
                    # function_type is a Literal field, so string comparison is type-safe
                    if function.function_type == "join":
                        other_upstream_id = function.arguments.other_upstream_id
                        
                        # Check if other_upstream_id references the transform itself
                        if other_upstream_id == transform.id_:
                            raise ValueError(
                                f"Join function in transform '{transform.id_}' references itself as other_upstream_id "
                                f"in job '{self.id_}'. A join cannot reference its own transform id."
                            )
                        
                        # Check if other_upstream_id exists in extracts or previously defined transforms
                        if other_upstream_id not in valid_upstream_ids_for_transforms:
                            raise ValueError(
                                f"Join function in transform '{transform.id_}' references other_upstream_id "
                                f"'{other_upstream_id}' in job '{self.id_}' which either does not exist or is defined "
                                f"later in the transforms list. other_upstream_id must reference an existing extract or "
                                f"a transform that appears before this one."
                            )

            # Add current transform ID to valid upstream IDs for subsequent transforms
            valid_upstream_ids_for_transforms.add(transform.id_)

        # Validate load upstream_ids reference existing extracts or transforms
        transform_ids = {transform.id_ for transform in self.transforms}
        valid_upstream_ids_for_loads = extract_ids | transform_ids
        for load in self.loads:
            if load.upstream_id not in valid_upstream_ids_for_loads:
                raise ValueError(
                    f"Load '{load.id_}' references non-existent upstream_id '{load.upstream_id}' "
                    f"in job '{self.id_}'. upstream_id must reference an existing extract or transform."
                )

        return self

    def execute(self) -> None:
        """Execute the complete ETL pipeline with comprehensive exception handling.

        Checks if the job is enabled before execution. If disabled, returns immediately.

        Triggers hooks at appropriate lifecycle points:
        - onStart: When execution begins
        - onError: When any exception occurs
        - onSuccess: When execution completes successfully
        - onFinally: Always executed at the end

        After execution completes (success or failure), clears all DataFrames and
        streaming queries from registries to free memory and prevent data leakage
        between jobs.

        Raises:
            FlintJobError: Wraps configuration and I/O exceptions with context,
                preserving the original exception as the cause.
        """
        if not self.enabled:
            logger.info("Job '%s' is disabled. Skipping execution.", self.id_)
            return

        self.hooks.on_start()

        try:
            logger.info("Starting job execution: %s", self.id_)
            self._execute()
            logger.info("Job completed successfully: %s", self.id_)
            self.hooks.on_success()
        except (ValueError, KeyError, OSError) as e:
            logger.error("Job '%s' failed: %s", self.id_, e)
            self.hooks.on_error()
            raise FlintJobError(f"Error occurred during job '{self.id_}' execution") from e
        finally:
            self.hooks.on_finally()
            self._clear()

    @abstractmethod
    def _execute(self) -> None:
        """Execute the engine-specific ETL pipeline logic.

        This method must be implemented by each engine-specific job class
        to handle the execution of the ETL pipeline using the appropriate
        execution engine.
        """

    @abstractmethod
    def _clear(self) -> None:
        """Clear engine-specific registries to free memory.

        This method must be implemented by each engine-specific job class
        to clear all data registries (DataFrames, streaming queries, etc.)
        after job execution completes. This ensures memory is freed and
        prevents data leakage between jobs.
        """
