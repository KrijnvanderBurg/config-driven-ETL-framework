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

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Final, Self

from pyspark.sql.types import StructType

from ingestion_framework.exceptions import DictKeyError
from ingestion_framework.utils.schema import SchemaFilepathHandler

from . import Model

NAME: Final[str] = "name"
METHOD: Final[str] = "method"
DATA_FORMAT: Final[str] = "data_format"
LOCATION: Final[str] = "location"
SCHEMA: Final[str] = "schema"
OPTIONS: Final[str] = "options"


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


@dataclass
class ExtractModel(Model):
    """
    Base model for data extraction operations.

    This model serves as a base class for defining extraction configurations,
    including the method of extraction and the format of the data.

    Args:
        name: Identifier for this extraction operation
        method: Method of extraction (batch or streaming)
        data_format: Format of the data to extract (parquet, json, csv)
    """

    name: str
    method: ExtractMethod
    data_format: ExtractFormat


@dataclass
class ExtractFileModel(ExtractModel):
    """
    Model for file extraction using PySpark.

    This model configures extraction operations for reading files with PySpark,
    including format, location, and schema information.

    Args:
        name: Identifier for this extraction operation
        method: Method of extraction (batch or streaming)
        data_format: Format of the files to extract (parquet, json, csv)
        location: URI where the files are located
        options: PySpark reader options as key-value pairs
        schema: Optional schema definition for the data structure
    """

    location: str
    options: dict[str, str]
    schema: StructType | None = None

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """
        Create an ExtractModelFilePyspark object from a configuration dictionary.

        Args:
            dict_: The configuration dictionary containing extraction parameters

        Returns:
            An initialized extraction model for file-based sources

        Raises:
            DictKeyError: If required keys are missing from the configuration
        """
        try:
            name = dict_[NAME]
            method = ExtractMethod(dict_[METHOD])
            data_format = ExtractFormat(dict_[DATA_FORMAT])
            location = dict_[LOCATION]
            options = dict_[OPTIONS]
            schema = SchemaFilepathHandler.parse(schema=Path(dict_[SCHEMA]))
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            name=name,
            method=method,
            data_format=data_format,
            location=location,
            options=options,
            schema=schema,
        )
