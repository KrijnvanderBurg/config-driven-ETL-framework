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

from pydantic import Field

from samara import BaseModel
from samara.exceptions import FlintJobError
from samara.runtime.jobs.hooks import Hooks
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class JobEngine(Enum):
    """Enumeration of supported job engines.

    Defines the different execution engines that can be used to run ETL jobs.
    This enum is used as the discriminator for job type selection.
    """

    SPARK = "spark"
    # Future engines can be added here:
    # POLARS = "polars"


class JobBase(BaseModel, ABC):
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
    """

    id_: str = Field(..., alias="id", description="Unique identifier for the job", min_length=1)
    description: str = Field(..., description="Human-readable description of the job's purpose")
    enabled: bool = Field(..., description="Whether this job should be executed")
    engine_type: JobEngine = Field(..., description="The execution engine to use for this job")
    hooks: Hooks = Field(..., description="Hooks to execute at various stages of the job lifecycle")

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
