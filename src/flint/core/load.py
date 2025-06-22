"""Load interface and implementations for various data formats.

This module provides abstract classes and implementations for loading data to
various destinations and formats using Apache PySpark. It includes:

- Abstract base classes defining the loading interface
- Concrete implementations for different output formats (CSV, JSON, etc.)
- Support for both batch and streaming writes
- Registry mechanism for dynamically selecting appropriate loaders
- Configuration-driven loading functionality

The Load components represent the final phase in the ETL pipeline, responsible
for writing processed data to target destinations.
"""

import json
from abc import ABC, abstractmethod
from typing import Any, Final, Generic, Self, TypeVar

from pyspark.sql.streaming.query import StreamingQuery

from flint.models.model_load import LoadFormat, LoadMethod, LoadModel, LoadModelFile
from flint.types import DataFrameRegistry, RegistryDecorator, Singleton, StreamingQueryRegistry
from flint.utils.spark import SparkHandler

NAME: Final[str] = "name"
UPSTREAM_NAME: Final[str] = "upstream_name"
METHOD: Final[str] = "method"
MODE: Final[str] = "mode"
DATA_FORMAT: Final[str] = "data_format"
LOCATION: Final[str] = "location"
SCHEMA_LOCATION: Final[str] = "schema_location"
OPTIONS: Final[str] = "options"

LoadModelT = TypeVar("LoadModelT", bound=LoadModel)


class LoadRegistry(RegistryDecorator, metaclass=Singleton):
    """
    Registry for Extract implementations.

    Maps ExtractFormat enum values to concrete ExtractAbstract implementations.
    """


class Load(Generic[LoadModelT], ABC):
    """
    Abstract base class for data loading operations.

    This class defines the interface for all loading implementations,
    supporting both batch and streaming loads to various destinations.
    """

    model_cls: type[LoadModelT]

    def __init__(self, model: LoadModelT) -> None:
        """
        Initialize the loading operation.

        Args:
            model: Configuration model for the loading operation
        """
        self.model = model
        self.data_registry = DataFrameRegistry()
        self.streaming_query_registry = StreamingQueryRegistry()

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """
        Create a loading instance from a configuration dictionary.

        Args:
            dict_: Configuration dictionary containing loading specifications

        Returns:
            An initialized loading instance of the appropriate type based on data_format

        Raises:
            DictKeyError: If required keys are missing from the configuration
            NotImplementedError: If the specified load format is not supported
        """
        # If called on a concrete class, use that class directly
        if cls is not Load:
            model = cls.model_cls.from_dict(dict_=dict_)
            return cls(model=model)

        # If called on the base class, determine the concrete class using the registry
        try:
            data_format = dict_[DATA_FORMAT]
            load_format = LoadFormat(data_format)
            load_class = LoadRegistry.get(load_format)
            model = load_class.model_cls.from_dict(dict_=dict_)
            return load_class(model=model)
        except KeyError as e:
            raise NotImplementedError(f"Load format {dict_.get(DATA_FORMAT, '<missing>')} is not supported.") from e

    @abstractmethod
    def _load_batch(self) -> None:
        """
        Perform batch loading of data to the destination.
        """

    @abstractmethod
    def _load_streaming(self) -> StreamingQuery:
        """
        Perform streaming loading of data to the destination.

        Returns:
            A streaming query object that can be used to monitor the stream
        """

    def _load_schema(self) -> None:
        """
        load schema from DataFrame.
        """
        if self.model.schema_location is None:
            return

        schema = json.dumps(self.data_registry[self.model.name].schema.jsonValue())

        with open(self.model.schema_location, mode="w", encoding="utf-8") as f:
            f.write(schema)

    def load(self) -> None:
        """
        Load data with PySpark.
        """
        spark_handler: SparkHandler = SparkHandler()
        spark_handler.add_configs(options=self.model.options)

        self.data_registry[self.model.name] = self.data_registry[self.model.upstream_name]

        if self.model.method == LoadMethod.BATCH:
            self._load_batch()
        elif self.model.method == LoadMethod.STREAMING:
            self.streaming_query_registry[self.model.name] = self._load_streaming()
        else:
            raise ValueError("Loading method %s is not supported for PySpark" % self.model.method)

        self._load_schema()


@LoadRegistry.register(LoadFormat.PARQUET)
@LoadRegistry.register(LoadFormat.JSON)
@LoadRegistry.register(LoadFormat.CSV)
class LoadFile(Load[LoadModelFile]):
    """
    Concrete class for file loading using PySpark DataFrame.
    """

    model_cls = LoadModelFile

    def _load_batch(self) -> None:
        """
        Write to file in batch mode.
        """
        self.data_registry[self.model.name].write.save(
            path=self.model.location,
            format=self.model.data_format.value,
            mode=self.model.mode.value,
            **self.model.options,
        )

    def _load_streaming(self) -> StreamingQuery:
        """
        Write to file in streaming mode.

        Returns:
            StreamingQuery: Represents the ongoing streaming query.
        """
        return self.data_registry[self.model.name].writeStream.start(
            path=self.model.location,
            format=self.model.data_format.value,
            outputMode=self.model.mode.value,
            **self.model.options,
        )
