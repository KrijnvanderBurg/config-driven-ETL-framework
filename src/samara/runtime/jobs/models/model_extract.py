"""Data models for extraction operations in the ingestion framework.

This module defines the data models and configuration structures used for
representing extraction operations. It includes:

- Enums for representing extraction methods and formats
- Data classes for structuring extraction configuration
- Utility methods for parsing and validating extraction parameters
- Constants for standard configuration keys

These models serve as the configuration schema for the Extract components
and provide a type-safe interface between configuration and implementation.
"""

import logging
from enum import Enum
from typing import Literal

from pydantic import Field, FilePath
from samara import BaseModel
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class ExtractMethod(Enum):
    """Enumeration of supported data extraction methods.

    Defines the different methods that can be used to read data from sources,
    such as batch processing or streaming.

    These values are used in configuration files to specify how data should
    be extracted from the source.
    """

    BATCH = "batch"
    STREAMING = "streaming"


class ExtractModel(BaseModel):
    """
    Base model for data extraction operations.

    This model serves as a base class for defining extraction configurations,
    including the method of extraction and the format of the data.

    Args:
        id: Identifier for this extraction operation
        method: Method of extraction (batch or streaming)
        data_format: Format of the data to extract (parquet, json, csv)
        options: PySpark reader options as key-value pairs
    """

    id_: str = Field(..., alias="id", description="Identifier for this extraction operation", min_length=1)
    method: ExtractMethod = Field(..., description="Method of extraction (batch or streaming)")
    data_format: str = Field(..., description="Format of the data to extract (parquet, json, csv, etc.)")
    schema_: str | FilePath = Field(..., alias="schema", description="Schema definition - can be a file path or string")


class ExtractFileModel(ExtractModel):
    """
    Model for file extraction using PySpark.

    This model configures extraction operations for reading files with PySpark,
    including format, location, and schema information.

    Args:
        extract_type: Type discriminator for file-based extraction
        id: Identifier for this extraction operation
        method: Method of extraction (batch or streaming)
        data_format: Format of the files to extract (parquet, json, csv)
        location: URI where the files are located
        schema_: Schema definition - can be a file path or JSON string (defaults to empty string)
    """

    extract_type: Literal["file"] = Field(..., description="Extract type discriminator")
    location: str = Field(..., description="URI where the files are located")
