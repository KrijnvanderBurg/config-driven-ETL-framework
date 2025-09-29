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
from typing import ClassVar

from pyspark.sql.streaming.query import StreamingQuery

from flint.runtime.jobs.models.model_load import LoadMethod, LoadModel, LoadModelFile
from flint.runtime.jobs.spark.session import SparkHandler
from flint.types import DataFrameRegistry, StreamingQueryRegistry
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class LoadSpark(LoadModel, ABC):
    """
    Abstract base class for data loading operations.

    This class defines the interface for all loading implementations,
    supporting both batch and streaming loads to various destinations.
    """

    spark: ClassVar[SparkHandler] = SparkHandler()
    data_registry: ClassVar[DataFrameRegistry] = DataFrameRegistry()
    streaming_query_registry: ClassVar[StreamingQueryRegistry] = StreamingQueryRegistry()

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

    def _export_schema(self, schema_json: str, schema_path: str) -> None:
        """
        Export DataFrame schema to a JSON file.

        Args:
            schema_json: JSON representation of the DataFrame schema
            schema_path: File path where schema should be written
        """
        logger.debug("Exporting schema for %s to: %s", self.name, schema_path)

        with open(schema_path, mode="w", encoding="utf-8") as f:
            f.write(schema_json)

        logger.info("Schema exported successfully for %s to: %s", self.name, schema_path)

    def load(self) -> None:
        """
        Load data with PySpark.
        """
        logger.info(
            "Starting load operation for: %s from upstream: %s using method: %s",
            self.name,
            self.upstream_name,
            self.method.value,
        )

        logger.debug("Adding Spark configurations: %s", self.options)
        self.spark.add_configs(options=self.options)

        logger.debug("Copying dataframe from %s to %s", self.upstream_name, self.name)
        self.data_registry[self.name] = self.data_registry[self.upstream_name]

        if self.method == LoadMethod.BATCH:
            logger.debug("Performing batch load for: %s", self.name)
            self._load_batch()
            logger.info("Batch load completed successfully for: %s", self.name)
        elif self.method == LoadMethod.STREAMING:
            logger.debug("Performing streaming load for: %s", self.name)
            self.streaming_query_registry[self.name] = self._load_streaming()
            logger.info("Streaming load started successfully for: %s", self.name)
        else:
            raise ValueError(f"Loading method {self.method} is not supported for PySpark")

        # Export schema if location is specified
        if self.schema_location is not None:
            schema_json = json.dumps(self.data_registry[self.name].schema.jsonValue())
            self._export_schema(schema_json, self.schema_location)

        logger.info("Load operation completed successfully for: %s", self.name)


class LoadFileSpark(LoadSpark, LoadModelFile):
    """
    Concrete class for file loading using PySpark DataFrame.
    """

    def _load_batch(self) -> None:
        """
        Write to file in batch mode.
        """
        logger.debug(
            "Writing file in batch mode - path: %s, format: %s, mode: %s",
            self.location,
            self.data_format.value,
            self.mode.value,
        )

        row_count = self.data_registry[self.name].count()
        logger.debug("Writing %d rows to %s", row_count, self.location)

        self.data_registry[self.name].write.save(
            path=self.location,
            format=self.data_format.value,
            mode=self.mode.value,
            **self.options,
        )

        logger.info("Batch write successful - wrote %d rows to %s", row_count, self.location)

    def _load_streaming(self) -> StreamingQuery:
        """
        Write to file in streaming mode.

        Returns:
            StreamingQuery: Represents the ongoing streaming query.
        """
        logger.debug(
            "Writing file in streaming mode - path: %s, format: %s, mode: %s",
            self.location,
            self.data_format.value,
            self.mode.value,
        )

        streaming_query = self.data_registry[self.name].writeStream.start(
            path=self.location,
            format=self.data_format.value,
            outputMode=self.mode.value,
            **self.options,
        )

        logger.info("Streaming write started successfully for %s, query ID: %s", self.location, streaming_query.id)
        return streaming_query


# For now, just use LoadFileSpark directly since it's the only load type
# When more load types are added, this will become a discriminated union:
# LoadSparkUnion = Annotated[Union[LoadFileSpark, LoadDatabaseSpark], Discriminator("type")]
LoadSparkUnion = LoadFileSpark
