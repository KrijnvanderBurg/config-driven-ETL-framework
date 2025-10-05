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
from typing import ClassVar

from pyspark.sql import DataFrame

from flint.runtime.jobs.models.model_extract import ExtractFileModel, ExtractMethod, ExtractModel
from flint.runtime.jobs.spark.session import SparkHandler
from flint.types import DataFrameRegistry
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class ExtractSpark(ExtractModel, ABC):
    """Abstract base class for data extraction operations.

    Defines the interface for all extraction implementations, supporting both
    batch and streaming extractions. Manages a data registry for extracted DataFrames.

    Attributes:
        model_cls: The model class used for configuration
        model: The configuration model for this extraction
        data_registry: Registry for storing extracted DataFrames
    """

    spark: ClassVar[SparkHandler] = SparkHandler()
    data_registry: ClassVar[DataFrameRegistry] = DataFrameRegistry()

    def extract(self) -> None:
        """Main extraction method.

        Selects batch or streaming extraction based on the model configuration
        and stores the result in the data registry.
        """
        logger.info("Starting extraction for source: %s using method: %s", self.id, self.method.value)

        logger.debug("Adding Spark configurations: %s", self.options)
        self.spark.add_configs(options=self.options)

        if self.method == ExtractMethod.BATCH:
            logger.debug("Performing batch extraction for: %s", self.id)
            self.data_registry[self.id] = self._extract_batch()
            logger.info("Batch extraction completed successfully for: %s", self.id)
        elif self.method == ExtractMethod.STREAMING:
            logger.debug("Performing streaming extraction for: %s", self.id)
            self.data_registry[self.id] = self._extract_streaming()
            logger.info("Streaming extraction completed successfully for: %s", self.id)
        else:
            raise ValueError(f"Extraction method {self.method} is not supported for PySpark")

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


class ExtractFileSpark(ExtractSpark, ExtractFileModel):
    """Concrete extractor for file-based sources (CSV, JSON, Parquet).

    Supports both batch and streaming extraction using PySpark's DataFrame API.
    """

    def _extract_batch(self) -> DataFrame:
        """Read from file in batch mode using PySpark.

        Returns:
            DataFrame: The extracted data as a DataFrame.
        """
        logger.debug("Reading files in batch mode - path: %s, format: %s", self.location, self.data_format.value)

        dataframe = self.spark.session.read.load(
            path=self.location,
            format=self.data_format.value,
            schema=self._schema_parsed,
            **self.options,
        )
        row_count = dataframe.count()
        logger.info("Batch extraction successful - loaded %d rows from %s", row_count, self.location)
        return dataframe

    def _extract_streaming(self) -> DataFrame:
        """Read from file in streaming mode using PySpark.

        Returns:
            DataFrame: The extracted data as a streaming DataFrame.
        """
        logger.debug("Reading files in streaming mode - path: %s, format: %s", self.location, self.data_format.value)

        dataframe = self.spark.session.readStream.load(
            path=self.location,
            format=self.data_format.value,
            schema=self._schema_parsed,
            **self.options,
        )
        logger.info("Streaming extraction successful for %s", self.location)
        return dataframe


# ExtractSparkUnion = Annotated[
#     Union[ExtractFileSpark],
#     Discriminator("type"),
# ]

ExtractSparkUnion = ExtractFileSpark
