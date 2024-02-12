"""
IO extract interface and factory, extract implementations are in module datastore.loads.


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


class ExtractFormat(Enum):
    """Types of input and structures for extract."""

    PARQUET = "parquet"
    JSON = "json"
    CSV = "csv"


class ExtractMethod(Enum):
    """
    Types of extract operations.
    """

    BATCH = "batch"
    STREAMING = "streaming"


class ExtractSpec:
    """
    Specification of extract.

    Args:
        spec_id (str): ID of the extract specification.
        method (ExtractMethod): ReadType method of extract operation.
        data_format (ExtractFormat): format of the extract.
        location (str): uri that identifies from where to extract data in the specified format.
        options (dict): Execution options.
        schema (str): schema to be parsed to StructType.
        schema_filepath (str): filepath to schema file.
    """

    def __init__(
        self,
        spec_id: str,
        method: str,
        data_format: str,
        location: str,
        options: dict | None = None,
        schema: str | None = None,
        schema_filepath: str | None = None,
    ):
        self.spec_id = spec_id
        self.method = ExtractMethod(method)
        self.data_format = ExtractFormat(data_format)
        self.location = location
        self.options: dict = options or {}
        self.schema: StructType | None = Schema.from_spec(schema=schema, filepath=schema_filepath)

    @classmethod
    def from_confeti(cls, confeti: dict):
        """
        Get the extract specifications from confeti.

        Returns:
            List of extract specifications.
        """
        return cls(**confeti)


class ExtractStrategy(ABC):
    """Extract abstract class."""

    def __init__(self, spec: ExtractSpec):
        """
        Construct extract instance.

        Args:
            spec (ExtractSpec): extract specification for extracting data.
        """
        self.spec: ExtractSpec = spec

    @abstractmethod
    def extract(self) -> DataFrame:
        """
        Abstract extract method.

        Returns:
            A dataframe extract according to the extract specification.
        """
        raise NotImplementedError
