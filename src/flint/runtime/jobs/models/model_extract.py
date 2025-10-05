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
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import Field, FilePath, model_validator
from pyspark.sql.types import StructType

from flint import BaseModel
from flint.runtime.jobs.spark.schema import SchemaFilepathHandler, SchemaStringHandler
from flint.utils.logger import get_logger

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


class ExtractFormat(Enum):
    """Types of input and structures for extract."""

    PARQUET = "parquet"
    JSON = "json"
    CSV = "csv"


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

    id: str = Field(..., description="Identifier for this extraction operation", min_length=1)
    method: ExtractMethod = Field(..., description="Method of extraction (batch or streaming)")
    data_format: ExtractFormat = Field(..., description="Format of the data to extract (parquet, json, csv, etc.)")
    options: dict[str, Any] = Field(..., description="PySpark reader options as key-value pairs")


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

    extract_type: Literal["file"]
    location: str
    schema_: str | FilePath = ""
    _schema_parsed: StructType | None = None

    @model_validator(mode="after")
    def parse_schema(self) -> Self:
        """Parse schema_ field into _schema_parsed after model creation.

        This validator automatically converts the schema_ field value into a
        PySpark StructType based on the input type:
        - File path ending in .json: Uses SchemaFilepathHandler
        - JSON string: Uses SchemaStringHandler

        Returns:
            Self: The model instance with _schema_parsed populated
        """
        if not self.schema_:
            return self

        # Convert to string for processing
        schema_str = str(self.schema_).strip()

        # Detect if it's a file path or JSON string
        if schema_str.endswith(".json"):
            # File path - use FilepathHandler
            self._schema_parsed = SchemaFilepathHandler.parse(schema=Path(schema_str))
        else:
            # JSON string - use StringHandler
            self._schema_parsed = SchemaStringHandler.parse(schema=schema_str)

        return self
