"""
PySpark implementation for data extraction operations.

This module provides concrete implementations for extracting data using PySpark.
It includes:
    - Abstract base classes for extraction
    - Concrete file-based extractors
    - A registry and context for selecting extraction strategies
    - Support for both batch and streaming extraction
"""

from abc import ABC, abstractmethod
from typing import Any, Final, Generic, Self, TypeVar

from pyspark.sql import DataFrame

from ingestion_framework.models.model_extract import ExtractFileModel, ExtractFormat, ExtractMethod
from ingestion_framework.types import DataFrameRegistry, RegistryDecorator, Singleton
from ingestion_framework.utils.spark import SparkHandler

NAME: Final[str] = "name"
METHOD: Final[str] = "method"
DATA_FORMAT: Final[str] = "data_format"
LOCATION: Final[str] = "location"
SCHEMA: Final[str] = "schema"
OPTIONS: Final[str] = "options"

ExtractModelT = TypeVar("ExtractModelT", bound=ExtractFileModel)


class ExtractRegistry(RegistryDecorator, metaclass=Singleton):
    """Registry for Extract implementations.

    Maps ExtractFormat enum values to concrete Extract implementations.
    Enables dynamic selection of the appropriate Extract class based on the
    data format specified in configuration.
    """


class Extract(Generic[ExtractModelT], ABC):
    """Abstract base class for data extraction operations.

    Defines the interface for all extraction implementations, supporting both
    batch and streaming extractions. Manages a data registry for extracted DataFrames.

    Attributes:
        _model: The model class used for configuration
        model: The configuration model for this extraction
        data_registry: Registry for storing extracted DataFrames
    """

    _model: type[ExtractModelT]

    def __init__(self, model: ExtractModelT) -> None:
        """Initialize the extraction operation.

        Args:
            model: Configuration model for the extraction
        """
        self.model = model
        self.data_registry = DataFrameRegistry()

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create an extraction instance from a configuration dictionary.

        Args:
            dict_: Configuration dictionary containing extraction specifications

        Returns:
            An initialized extraction instance

        Raises:
            DictKeyError: If required keys are missing from the configuration
        """
        model = cls._model.from_dict(dict_=dict_)
        return cls(model=model)

    def extract(self) -> None:
        """Main extraction method.

        Selects batch or streaming extraction based on the model configuration
        and stores the result in the data registry.
        """
        spark_handler: SparkHandler = SparkHandler()
        spark_handler.add_configs(options=self.model.options)

        if self.model.method == ExtractMethod.BATCH:
            self.data_registry[self.model.name] = self._extract_batch()
        elif self.model.method == ExtractMethod.STREAMING:
            self.data_registry[self.model.name] = self._extract_streaming()
        else:
            raise ValueError(f"Extraction method {self.model.method} is not supported for Pyspark.")

    @abstractmethod
    def _extract_batch(self) -> DataFrame:
        """Extract data in batch mode.

        Returns:
            DataFrame: The extracted data as a DataFrame.
        """

    @abstractmethod
    def _extract_streaming(self) -> DataFrame:
        """Extract data in streaming mode.

        Returns:
            DataFrame: The extracted data as a streaming DataFrame.
        """


@ExtractRegistry.register(ExtractFormat.PARQUET)
@ExtractRegistry.register(ExtractFormat.JSON)
@ExtractRegistry.register(ExtractFormat.CSV)
class ExtractFile(Extract[ExtractFileModel]):
    """Concrete extractor for file-based sources (CSV, JSON, Parquet).

    Supports both batch and streaming extraction using PySpark's DataFrame API.
    """

    _model = ExtractFileModel
    _spark: SparkHandler = SparkHandler()

    def _extract_batch(self) -> DataFrame:
        """Read from file in batch mode using PySpark.

        Returns:
            DataFrame: The extracted data as a DataFrame.
        """
        return self._spark.session.read.load(
            path=self.model.location,
            format=self.model.data_format.value,
            schema=self.model.schema,
            **self.model.options,
        )

    def _extract_streaming(self) -> DataFrame:
        """Read from file in streaming mode using PySpark.

        Returns:
            DataFrame: The extracted data as a streaming DataFrame.
        """
        return self._spark.session.readStream.load(
            path=self.model.location,
            format=self.model.data_format.value,
            schema=self.model.schema,
            **self.model.options,
        )


class ExtractContext:
    """Context for creating and managing extraction strategies.

    Implements the Strategy pattern for data extraction, allowing different
    extraction implementations to be selected based on the data format.
    """

    @classmethod
    def factory(cls, dict_: dict[str, type[Extract]]) -> type[Extract]:
        """Create an appropriate extract class based on the format specified in the configuration.

        Uses the ExtractRegistry to look up the appropriate implementation class
        based on the data format specified in the configuration.

        Args:
            dict_: Configuration dictionary that must include a 'data_format' key
                compatible with the ExtractFormat enum

        Returns:
            type[Extract]: The concrete extraction class for the specified format.

        Raises:
            NotImplementedError: If the specified extract format is not supported.
            KeyError: If the 'data_format' key is missing from the configuration.
        """
        try:
            extract_format = ExtractFormat(dict_[DATA_FORMAT])
            return ExtractRegistry.get(extract_format)
        except KeyError as e:
            format_name = dict_.get(DATA_FORMAT, "<missing>")
            raise NotImplementedError(f"Extract format {format_name} is not supported.") from e
