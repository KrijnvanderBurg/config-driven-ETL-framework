"""
Load interface and implementations for various data formats.

This module provides abstract classes and implementations for data loading
to various destinations and formats.
"""

import json
from abc import ABC, abstractmethod
from typing import Any, Final, Generic, Self, TypeVar

from pyspark.sql.streaming.query import StreamingQuery

from ingestion_framework.models.load import LoadFormat, LoadMethod, LoadModel, LoadModelFile
from ingestion_framework.types import DataFrameRegistry, RegistryDecorator, Singleton, StreamingQueryRegistry
from ingestion_framework.utils.spark import SparkHandler

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

    load_model_concrete: type[LoadModelT]

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
            An initialized loading instance

        Raises:
            DictKeyError: If required keys are missing from the configuration
        """
        model = cls.load_model_concrete.from_dict(dict_=dict_)
        return cls(model=model)

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
        load data with PySpark.
        """
        spark_handler: SparkHandler = SparkHandler()
        spark_handler.add_configs(options=self.model.options)

        # Copy the dataframe from upstream to current name
        self.data_registry[self.model.name] = self.data_registry[self.model.upstream_name]

        if self.model.method == LoadMethod.BATCH:
            self._load_batch()
        elif self.model.method == LoadMethod.STREAMING:
            self.streaming_query_registry[self.model.name] = self._load_streaming()
        else:
            raise ValueError(f"Loading method {self.model.method} is not supported for Pyspark.")

        self._load_schema()


@LoadRegistry.register(LoadFormat.PARQUET)
@LoadRegistry.register(LoadFormat.JSON)
@LoadRegistry.register(LoadFormat.CSV)
class LoadFile(Load[LoadModelFile]):
    """
    Concrete class for file loading using PySpark DataFrame.
    """

    load_model_concrete = LoadModelFile

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


class LoadContext:
    """
    Abstract context class for creating and managing loading strategies.

    This class implements the Strategy pattern for data loading, allowing
    different loading implementations to be selected based on the data format.
    """

    @classmethod
    def factory(cls, dict_: dict[str, type[Load]]) -> type[Load[LoadModel]]:
        """
        Create an appropriate load class based on the format specified in the configuration.

        This factory method uses the LoadRegistry to look up the appropriate
        implementation class based on the data format.

        Args:
            dict_: Configuration dictionary that must include a 'data_format' key
                compatible with the LoadFormat enum

        Returns:
            The concrete loading class for the specified format

        Raises:
            NotImplementedError: If the specified load format is not supported
            KeyError: If the 'data_format' key is missing from the configuration
        """
        data_format = dict_[DATA_FORMAT]

        try:
            load_format = LoadFormat(data_format)
            return LoadRegistry.get(load_format)
        except KeyError as e:
            raise NotImplementedError(f"Load format {data_format} is not supported.") from e
