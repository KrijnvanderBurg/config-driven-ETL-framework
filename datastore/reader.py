"""
IO reader interface and factory, reader implementations are in module datastore.writers.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from abc import ABC, abstractmethod
from enum import Enum

from datastore.schema import Schema
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


class ReaderFormat(Enum):
    """Types of input and structures for reader."""

    PARQUET = "parquet"
    JSON = "json"
    CSV = "csv"


# Formats of reader that are considered files.
READER_FORMAT_FILES = [
    ReaderFormat.PARQUET,
    ReaderFormat.JSON,
    ReaderFormat.CSV,
]


class ReaderType(Enum):
    """
    Types of read operations.
    """

    BATCH = "batch"
    STREAMING = "streaming"


class ReaderSpec:
    """
    Specification of source input.

    spec_id (str): ID of the source specification.
    reader_type (ReaderType): ReadType type of source operation.
    reader_format (ReaderFormat): format of the source input.
    location (str): uri that identifies from where to read data in the specified format.
    options (dict): Execution options.
    schema (str): schema to be parsed to StructType.
    schema_filepath (str): filepath to schema file.
    """

    def __init__(
        self,
        spec_id: str,
        reader_type: str,
        reader_format: str,
        location: str,
        options: dict | None = None,
        schema: str | None = None,
        schema_filepath: str | None = None,
    ):
        self.spec_id = spec_id
        self.reader_type = ReaderType(reader_type)
        self.reader_format = ReaderFormat(reader_format)
        self.location = location
        self.options: dict = options or {}
        self.schema: StructType | None = Schema.from_spec(schema=schema, schema_filepath=schema_filepath)


class Reader(ABC):
    """Reader abstract class."""

    def __init__(self, spec: ReaderSpec):
        """
        Construct reader instance.

        Args:
            spec (ReaderSpec): reader specification for reading data.
        """
        self.spec: ReaderSpec = spec

    @abstractmethod
    def read(self) -> DataFrame:
        """
        Abstract read method.

        Returns:
            A dataframe read according to the input specification.
        """
        raise NotImplementedError
