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
import logging
from abc import ABC, abstractmethod
from typing import Any, Final, Generic, Self, TypeVar

from pyspark.sql.streaming.query import StreamingQuery

from flint.etl.models.model_load import LoadFormat, LoadMethod, LoadModel, LoadModelFile
from flint.types import DataFrameRegistry, RegistryDecorator, Singleton, StreamingQueryRegistry
from flint.utils.logger import get_logger
from flint.utils.spark import SparkHandler

logger: logging.Logger = get_logger(__name__)

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
        logger.debug(
            "Initializing Load with model: %s, destination: %s", model.name, getattr(model, "location", "unknown")
        )
        self.model = model
        self.data_registry = DataFrameRegistry()
        self.streaming_query_registry = StreamingQueryRegistry()
        logger.info("Load initialized successfully for: %s", model.name)

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """
        Create a loading instance from a configuration dictionary.

        Args:
            dict_: Configuration dictionary containing loading specifications

        Returns:
            An initialized loading instance of the appropriate type based on data_format

        Raises:
            FlintConfigurationKeyError: If required keys are missing from the configuration
            NotImplementedError: If the specified load format is not supported
        """
        logger.debug("Creating Load from dictionary: %s", dict_)

        # If called on a concrete class, use that class directly
        if cls is not Load:
            logger.debug("Using concrete class: %s", cls.__name__)
            model = cls.model_cls.from_dict(dict_=dict_)
            instance = cls(model=model)
            logger.info("Successfully created %s instance for: %s", cls.__name__, model.name)
            return instance

        # If called on the base class, determine the concrete class using the registry
        try:
            data_format = dict_[DATA_FORMAT]
            logger.debug("Determining load class for format: %s", data_format)
            load_format = LoadFormat(data_format)
            load_class = LoadRegistry.get(load_format)
            logger.debug("Selected load class: %s", load_class.__name__)
            model = load_class.model_cls.from_dict(dict_=dict_)
            instance = load_class(model=model)
            logger.info(
                "Successfully created %s instance for format %s, target: %s",
                load_class.__name__,
                data_format,
                model.name,
            )
            return instance
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
            logger.debug("No schema location specified for %s, skipping schema export", self.model.name)
            return

        logger.debug("Exporting schema for %s to: %s", self.model.name, self.model.schema_location)

        schema = json.dumps(self.data_registry[self.model.name].schema.jsonValue())

        with open(self.model.schema_location, mode="w", encoding="utf-8") as f:
            f.write(schema)

        logger.info("Schema exported successfully for %s to: %s", self.model.name, self.model.schema_location)

    def load(self) -> None:
        """
        Load data with PySpark.
        """
        logger.info(
            "Starting load operation for: %s from upstream: %s using method: %s",
            self.model.name,
            self.model.upstream_name,
            self.model.method.value,
        )

        spark_handler: SparkHandler = SparkHandler()
        logger.debug("Adding Spark configurations: %s", self.model.options)
        spark_handler.add_configs(options=self.model.options)

        logger.debug("Copying dataframe from %s to %s", self.model.upstream_name, self.model.name)
        self.data_registry[self.model.name] = self.data_registry[self.model.upstream_name]

        if self.model.method == LoadMethod.BATCH:
            logger.debug("Performing batch load for: %s", self.model.name)
            self._load_batch()
            logger.info("Batch load completed successfully for: %s", self.model.name)
        elif self.model.method == LoadMethod.STREAMING:
            logger.debug("Performing streaming load for: %s", self.model.name)
            self.streaming_query_registry[self.model.name] = self._load_streaming()
            logger.info("Streaming load started successfully for: %s", self.model.name)
        else:
            raise ValueError(f"Loading method {self.model.method} is not supported for PySpark")

        self._load_schema()
        logger.info("Load operation completed successfully for: %s", self.model.name)


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
        logger.debug(
            "Writing file in batch mode - path: %s, format: %s, mode: %s",
            self.model.location,
            self.model.data_format.value,
            self.model.mode.value,
        )

        row_count = self.data_registry[self.model.name].count()
        logger.debug("Writing %d rows to %s", row_count, self.model.location)

        self.data_registry[self.model.name].write.save(
            path=self.model.location,
            format=self.model.data_format.value,
            mode=self.model.mode.value,
            **self.model.options,
        )

        logger.info("Batch write successful - wrote %d rows to %s", row_count, self.model.location)

    def _load_streaming(self) -> StreamingQuery:
        """
        Write to file in streaming mode.

        Returns:
            StreamingQuery: Represents the ongoing streaming query.
        """
        logger.debug(
            "Writing file in streaming mode - path: %s, format: %s, mode: %s",
            self.model.location,
            self.model.data_format.value,
            self.model.mode.value,
        )

        streaming_query = self.data_registry[self.model.name].writeStream.start(
            path=self.model.location,
            format=self.model.data_format.value,
            outputMode=self.model.mode.value,
            **self.model.options,
        )

        logger.info(
            "Streaming write started successfully for %s, query ID: %s", self.model.location, streaming_query.id
        )
        return streaming_query
