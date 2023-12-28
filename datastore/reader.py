"""
IO reader interface and factory, reader implementations are in module store.io.reader.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED 
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

# standard library imports
from abc import ABC, abstractmethod
from enum import Enum
from typing import Generator, Optional
from dataclasses import dataclass

from datastore.reader.parquet_reader import ParquetReader

from pyspark.sql import DataFrame


class ReaderFormat(Enum):
    """Types of input and structures for reader.
    
    Allowed values:
    - PARQUET: Parquet format
    - JSON: JSON format
    - CSV: CSV format

    Note: If any new formats are added then update the tests accordingly.
    """

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
    def exists(cls, reader_format : str) -> bool:
        """
        Checks if the input format exists in the enum values.

        Args:
            reader_format: format to check if exists.

        Return:
            If the input format exists in enum.
        """
        return reader_format in cls.values()


class ReaderType(Enum):
    """
    Types of read operations.

    BATCH: read data as batch.
    STREAMING: read data as stream.
    """

    BATCH = "batch"
    STREAMING = "streaming"


@dataclass
class ReaderSpec(object):
    """
    Specification of source input.

    id: ID of the source specification.
    reader_type: ReadType type of source operation.
    reader_format: format of the source input.
    df_name: dataframe name.
    location: uri that identifies from where to read data in the specified format.
    """

    id : str
    reader_type : str
    reader_format : Optional[str] = None
    df_name : Optional[DataFrame] = None
    location : Optional[str] = None


class Reader(ABC):
    """Reader abstract class."""

    def __init__(self, spec : ReaderSpec):
        """
        Construct reader instance.

        Args:
            input_spec: input specification for reading data.
        """
        self._spec : ReaderSpec = spec

    @abstractmethod
    def read(self) -> DataFrame:
        """
        Abstract read method.

        Returns:
            A dataframe read according to the input specification.
        """
        raise NotImplementedError
    

class ReaderFactory(ABC):
    """Class for reader factory."""

    @classmethod
    def __new__(cls, spec : ReaderSpec) -> DataFrame:
        """
        Get data by reader specification using factory pattern.

        Args:
            spec: reader specification to read data.

        Returns:
            A dataframe containing the data.
        """
        if spec.reader_format == ReaderFormat.PARQUET.value:
            return ParquetReader(spec = spec).read()
        else:
            raise NotImplementedError(
                f"The requested input spec format {spec.reader_format} is not supported."
            )
