"""Job models for the Flint ETL framework.

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

from flint import BaseModel
from flint.utils.logger import get_logger

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

    Attributes:
        name: Unique identifier for the job
        description: Human-readable description of the job's purpose
        enabled: Whether this job should be executed
        engine: The execution engine to use for this job
    """

    name: str = Field(..., description="Unique identifier for the job", min_length=1)
    description: str = Field(..., description="Human-readable description of the job's purpose")
    enabled: bool = Field(..., description="Whether this job should be executed")
    engine: JobEngine = Field(..., description="The execution engine to use for this job")

    @abstractmethod
    def execute(self) -> None:
        """Execute the complete ETL pipeline.

        This method must be implemented by each engine-specific job class
        to handle the execution of the ETL pipeline using the appropriate
        execution engine.
        """
