"""
IO reader interface and factory, reader implementations are in module store.io.reader.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from abc import ABC, abstractmethod
from collections.abc import Generator
from enum import Enum

from pyspark.sql import DataFrame


class ReaderFormat(Enum):
    """Types of input and structures for reader."""

    PARQUET = "parquet"
    JSON = "json"
    CSV = "csv"

    @classmethod
    def values(cls) -> Generator:
        """
        Generate a list of all enum values.

        Return:
            Generator with all enum values.
        """
        return (c.value for c in cls)

    @classmethod
    def exists(cls, reader_format: str) -> bool:
        """
        Checks if the input format exists in the enum values.

        Args:
            reader_format (ReaderFormat): format to check if exists.

        Return:
            If the input format exists in enum.
        """
        return reader_format in cls.values()


class ReaderType(Enum):
    """
    Types of read operations.
    """

    BATCH = "batch"
    STREAMING = "streaming"

    @classmethod
    def values(cls) -> Generator:
        """
        Generate a list of all enum values.

        Return:
            Generator with all enum values.
        """
        return (c.value for c in cls)

    @classmethod
    def exists(cls, reader_format: str) -> bool:
        """
        Checks if the input format exists in the enum values.

        Args:
            reader_format (ReaderFormat): format to check if exists.

        Return:
            If the input format exists in enum.
        """
        return reader_format in cls.values()


class ReaderSpec:
    """
    Specification of source input.

    spec_id (str): ID of the source specification.
    reader_type (ReaderType): ReadType type of source operation.
    reader_format (ReaderFormat): format of the source input.
    location (str): uri that identifies from where to read data in the specified format.
    """

    def __init__(self, spec_id: str, reader_type: str, reader_format: str, location: str):
        self.spec_id = spec_id

        if ReaderType.exists(reader_type):
            self.reader_type = reader_type
        else:
            raise ValueError(f"Invalid reader_type value {reader_type}.")
        self.reader_format = reader_format
        self.location = location


class Reader(ABC):
    """Reader abstract class."""

    def __init__(self, spec: ReaderSpec):
        """
        Construct reader instance.

        Args:
            spec (ReaderSpec): reader specification for reading data.
        """
        self._spec: ReaderSpec = spec

    @abstractmethod
    def read(self) -> DataFrame:
        """
        Abstract read method.

        Returns:
            A dataframe read according to the input specification.
        """
        raise NotImplementedError
