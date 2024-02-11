"""
Data Load Module.

This module defines an abstract class `Load` along with supporting enumerations for configuring data load operations.
It provides data writing, allowing customization through implementing `Load` abstract methods.

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


class LoadFormat(Enum):
    """Enumeration for methods of input and structures for the load."""

    PARQUET = "parquet"
    JSON = "json"
    CSV = "csv"


# Formats of load that are considered files.
LOAD_FILES_FORMAT = [
    LoadFormat.PARQUET,
    LoadFormat.JSON,
    LoadFormat.CSV,
]


class LoadMethod(Enum):
    """Enumeration for methods of load modes."""

    BATCH = "batch"
    STREAMING = "streaming"


class LoadOperation(Enum):
    """Enumeration for methods of load operations."""

    COMPLETE = "complete"
    APPEND = "append"
    UPDATE = "update"


class LoadSpec:
    """
    Specification of the sink input.

    Args:
        spec_id (str): ID of the sink specification.
        method (str): Type of sink load mode.
        operation (str): Type of sink operation.
        data_format (str): Format of the sink input.
        location (str): URI that identifies where to load data in the specified format.
        options (dict): Execution options.
    """

    def __init__(
        self,
        spec_id: str,
        method: str,
        operation: str,
        data_format: str,
        location: str,
        options: dict | None = None,
    ):
        self.spec_id = spec_id
        self.method = LoadMethod(method)
        self.operation = LoadOperation(operation)
        self.data_format = LoadFormat(data_format)
        self.location = location
        self.options: dict = options or {}

    @classmethod
    def from_confeti(cls, confeti: dict):
        """Get the load specifications from confeti.

        Returns:
            List of load specifications.
        """
        return cls(**confeti)


class Load(ABC):
    """Abstract Load class."""

    def __init__(self, spec: LoadSpec, dataframe: DataFrame):
        """
        Construct Load instance.

        Args:
            spec (LoadSpec): Load specification for writing data.
            dataframe (DataFrame): DataFrame to load.
        """
        self.spec = spec
        self.dataframe = dataframe

    @abstractmethod
    def load(self) -> StreamingQuery | None:
        """
        Abstract load method.

        Raises:
            NotImplementedError: This method must be implemented by the subclass.
        """
        raise NotImplementedError
