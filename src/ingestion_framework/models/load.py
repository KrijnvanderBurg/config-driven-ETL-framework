"""
Load interface and implementations for various data formats.

This module provides abstract classes and implementations for data loading
to various destinations and formats.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Final, Self

from ingestion_framework.exceptions import DictKeyError

from . import Model

NAME: Final[str] = "name"
UPSTREAM_NAME: Final[str] = "upstream_name"
METHOD: Final[str] = "method"
MODE: Final[str] = "mode"
DATA_FORMAT: Final[str] = "data_format"
LOCATION: Final[str] = "location"
SCHEMA_LOCATION: Final[str] = "schema_location"
OPTIONS: Final[str] = "options"


class LoadMethod(Enum):
    """Enumeration for methods of load modes."""

    BATCH = "batch"
    STREAMING = "streaming"


class LoadMode(Enum):
    """Enumeration for methods of load modes."""

    # pyspark batch
    APPEND = "append"
    OVERWRITE = "overwrite"
    ERROR = "error"
    ERROR_IF_EXISTS = "errorifexists"
    IGNORE = "ignore"

    # pyspark streaming
    # APPEND = "append"  # already added above for pyspark batch
    COMPLETE = "complete"
    UPDATE = "update"


class LoadFormat(Enum):
    """Enumeration for methods of input and structures for the load."""

    PARQUET = "parquet"
    JSON = "json"
    CSV = "csv"


@dataclass
class LoadModel(Model, ABC):
    """
    Abstract base class for load operation models.

    This class defines the configuration model for data loading operations,
    specifying the name, upstream source, method, and destination for the load.

    Args:
        name (str): ID of the sink modelification.
        upstream_name (list[str]): ID of the sink modelification.
        method (LoadMethod): Type of sink load mode.
        location (str): URI that identifies where to load data in the modelified format.
        schema_location (str): URI that identifies where to load schema.
        options (dict[str, Any]): Options for the sink input.
    """

    name: str
    upstream_name: str
    method: LoadMethod
    location: str
    schema_location: str | None
    options: dict[str, str]

    @classmethod
    @abstractmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """
        Create a loading model from a configuration dictionary.

        Args:
            dict_: Configuration dictionary containing loading parameters

        Returns:
            An initialized loading model

        Raises:
            DictKeyError: If required keys are missing from the configuration
        """


@dataclass
class LoadModelFile(LoadModel):
    """Abstract base class for file-based load models."""

    mode: LoadMode
    data_format: LoadFormat

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """
        Create a LoadModelFilePyspark object from a dict_ dictionary.

        Args:
            dict_ (dict[str, Any]): The dict_ dictionary.

        Returns:
            LoadModelFilePyspark: LoadModelFilePyspark object.
        """
        try:
            name = dict_[NAME]
            upstream_name = dict_[UPSTREAM_NAME]
            method = LoadMethod(dict_[METHOD])
            mode = LoadMode(dict_[MODE])
            data_format = LoadFormat(dict_[DATA_FORMAT])
            location = dict_[LOCATION]
            schema_location = dict_.get(SCHEMA_LOCATION, None)
            options = dict_.get(OPTIONS, {})
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            name=name,
            upstream_name=upstream_name,
            method=method,
            mode=mode,
            data_format=data_format,
            location=location,
            schema_location=schema_location,
            options=options,
        )
