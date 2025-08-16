"""
PySpark implementation for data extraction operations.

This module provides concrete implementations for extracting data using PySpark.
It includes:
    - Abstract base classes for extraction
    - Concrete file-based extractors
    - A registry for selecting extraction strategies
    - Support for both batch and streaming extraction
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Final, Generic, Self, TypeVar

from pyspark.sql import DataFrame

from flint.models.model_extract import ExtractFileModel, ExtractFormat, ExtractMethod
from flint.types import DataFrameRegistry, RegistryDecorator, Singleton
from flint.utils.logger import get_logger
from flint.utils.spark import SparkHandler

logger: logging.Logger = get_logger(__name__)

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
        model_cls: The model class used for configuration
        model: The configuration model for this extraction
        data_registry: Registry for storing extracted DataFrames
    """

    model_cls: type[ExtractModelT]

    def __init__(self, model: ExtractModelT) -> None:
        """Initialize the extraction operation.

        Args:
            model: Configuration model for the extraction
        """
        logger.debug("Initializing Extract with model: %s", model.name)
        self.model = model
        self.data_registry = DataFrameRegistry()
        logger.info("Extract initialized successfully for source: %s", model.name)

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create an extraction instance from a configuration dictionary.

        Args:
            dict_: Configuration dictionary containing extraction specifications

        Returns:
            An initialized extraction instance of the appropriate type based on data_format

        Raises:
            ConfigurationKeyError: If required keys are missing from the configuration
            NotImplementedError: If the specified extract format is not supported.
        """
        logger.debug("Creating Extract from dictionary: %s", dict_)

        # If called on a concrete class, use that class directly
        if cls is not Extract:
            logger.debug("Using concrete class: %s", cls.__name__)
            model = cls.model_cls.from_dict(dict_=dict_)
            instance = cls(model=model)
            logger.info("Successfully created %s instance for: %s", cls.__name__, model.name)
            return instance

        try:
            data_format = dict_[DATA_FORMAT]
            logger.debug("Determining extract class for format: %s", data_format)
            extract_format = ExtractFormat(data_format)
            extract_class = ExtractRegistry.get(extract_format)
            logger.debug("Selected extract class: %s", extract_class.__name__)
            model = extract_class.model_cls.from_dict(dict_=dict_)
            instance = extract_class(model=model)
            logger.info(
                "Successfully created %s instance for format %s, source: %s",
                extract_class.__name__,
                data_format,
                model.name,
            )
            return instance
        except KeyError as e:
            raise NotImplementedError(f"Extract format {dict_.get(DATA_FORMAT, '<missing>')} is not supported.") from e

    def extract(self) -> None:
        """Main extraction method.

        Selects batch or streaming extraction based on the model configuration
        and stores the result in the data registry.
        """
        logger.info("Starting extraction for source: %s using method: %s", self.model.name, self.model.method.value)

        spark_handler: SparkHandler = SparkHandler()
        logger.debug("Adding Spark configurations: %s", self.model.options)
        spark_handler.add_configs(options=self.model.options)

        if self.model.method == ExtractMethod.BATCH:
            logger.debug("Performing batch extraction for: %s", self.model.name)
            self.data_registry[self.model.name] = self._extract_batch()
            logger.info("Batch extraction completed successfully for: %s", self.model.name)
        elif self.model.method == ExtractMethod.STREAMING:
            logger.debug("Performing streaming extraction for: %s", self.model.name)
            self.data_registry[self.model.name] = self._extract_streaming()
            logger.info("Streaming extraction completed successfully for: %s", self.model.name)
        else:
            raise ValueError(f"Extraction method {self.model.method} is not supported for PySpark")

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

    model_cls = ExtractFileModel
    _spark: SparkHandler = SparkHandler()

    def _extract_batch(self) -> DataFrame:
        """Read from file in batch mode using PySpark.

        Returns:
            DataFrame: The extracted data as a DataFrame.
        """
        logger.debug(
            "Reading files in batch mode - path: %s, format: %s", self.model.location, self.model.data_format.value
        )

        dataframe = self._spark.session.read.load(
            path=self.model.location,
            format=self.model.data_format.value,
            schema=self.model.schema,
            **self.model.options,
        )
        row_count = dataframe.count()
        logger.info("Batch extraction successful - loaded %d rows from %s", row_count, self.model.location)
        return dataframe

    def _extract_streaming(self) -> DataFrame:
        """Read from file in streaming mode using PySpark.

        Returns:
            DataFrame: The extracted data as a streaming DataFrame.
        """
        logger.debug(
            "Reading files in streaming mode - path: %s, format: %s", self.model.location, self.model.data_format.value
        )

        dataframe = self._spark.session.readStream.load(
            path=self.model.location,
            format=self.model.data_format.value,
            schema=self.model.schema,
            **self.model.options,
        )
        logger.info("Streaming extraction successful for %s", self.model.location)
        return dataframe
