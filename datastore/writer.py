"""
Data Writer Module.

This module defines an abstract class `Writer` along with supporting enumerations for configuring data write operations.
It provides data writing, allowing customization through implementing `Writer` abstract methods.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from abc import ABC, abstractmethod
from enum import Enum

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery


class WriterFormat(Enum):
    """Enumeration for types of input and structures for the writer."""

    PARQUET = "parquet"
    JSON = "json"
    CSV = "csv"


# Formats of writer that are considered files.
WRITER_FORMAT_FILES = [
    WriterFormat.PARQUET,
    WriterFormat.JSON,
    WriterFormat.CSV,
]


class WriterType(Enum):
    """Enumeration for types of write modes."""

    BATCH = "batch"
    STREAMING = "streaming"


class WriterOperation(Enum):
    """Enumeration for types of write operations."""

    COMPELTE = "complete"
    APPEND = "append"
    UPDATE = "update"


class WriterSpec:
    """
    Specification of the sink input.

    Args:
        spec_id (str): ID of the sink specification.
        writer_type (str): Type of sink write mode.
        writer_operation (str): Type of sink operation.
        writer_format (str): Format of the sink input.
        location (str): URI that identifies where to write data in the specified format.
        options (dict): Execution options.
    """

    def __init__(
        self,
        spec_id: str,
        writer_type: str,
        writer_operation: str,
        writer_format: str,
        location: str,
        options: dict | None = None,
    ):
        self.spec_id = spec_id
        self.writer_type = WriterType(writer_type)
        self.writer_operation = WriterOperation(writer_operation)
        self.writer_format = WriterFormat(writer_format)
        self.location = location
        self.options: dict = options or {}


class Writer(ABC):
    """Abstract Writer class."""

    def __init__(self, spec: WriterSpec, dataframe: DataFrame):
        """
        Construct Writer instance.

        Args:
            spec (WriterSpec): Writer specification for writing data.
            df (DataFrame): DataFrame to write.
        """
        self.spec = spec
        self.dataframe = dataframe

    @abstractmethod
    def write(self) -> StreamingQuery | None:
        """
        Abstract write method.

        Raises:
            NotImplementedError: This method must be implemented by the subclass.
        """
        raise NotImplementedError
