"""Data models for loading operations in the ingestion framework.

This module defines the data models and configuration structures used for
representing data loading operations. It includes:

- Enums for representing loading methods, modes, and formats
- Data classes for structuring loading configuration
- Utility methods for parsing and validating loading parameters
- Constants for standard configuration keys

These models serve as the configuration schema for the Load components
and provide a type-safe interface between configuration and implementation.
"""

import logging
from abc import ABC
from enum import Enum
from typing import Literal

from pydantic import Field

from flint import BaseModel
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class LoadMethod(Enum):
    """Enumeration of supported data loading methods.

    Defines the different methods that can be used to write data to destinations,
    such as batch processing or streaming.

    These values are used in configuration files to specify how data should
    be loaded to the destination.
    """

    BATCH = "batch"
    STREAMING = "streaming"


class LoadModel(BaseModel, ABC):
    """Abstract base class for load operation models.

    This class defines the configuration model for data loading operations,
    specifying the identifier, upstream source, method, and destination for the load.

    It serves as the foundation for more specific load model types based on
    the destination type (file, database, etc.).

    Attributes:
        id: Unique identifier for this load operation
        upstream_id: Identifier of the upstream component providing data
        method: Loading method (batch or streaming)
        location (str): URI that identifies where to load data in the modelified format.
        schema_location (str): URI that identifies where to load schema.
        options (dict[str, Any]): Options for the sink input.
    """

    id_: str = Field(..., alias="id", description="Identifier for this load operation", min_length=1)
    upstream_id: str = Field(..., description="Identifier of the upstream component providing data", min_length=1)
    method: LoadMethod = Field(..., description="Loading method (batch or streaming)")
    location: str = Field(
        ..., description="URI that identifies where to load data in the modelified format.", min_length=1
    )
    schema_location: str = Field(..., description="URI that identifies where to load schema.")


class LoadModelFile(LoadModel):
    """Abstract base class for file-based load models.

    Args:
        load_type: Type discriminator for file-based loading
        mode: Write mode for the load operation
        data_format: Format of the output files
    """

    load_type: Literal["file"] = Field(..., description="Load type discriminator")
    mode: str = Field(..., description="Write mode for the load operation")
    data_format: str = Field(..., description="Format of the output files")
