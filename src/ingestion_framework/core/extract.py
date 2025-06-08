"""
PySpark implementation for data extraction operations.

This module provides concrete implementations for extracting data using PySpark.
"""

from abc import abstractmethod
from typing import Any, Final, Generic, Self, TypeVar

from pyspark.sql import DataFrame

from ingestion_framework.models.extract import ExtractFileModel, ExtractFormat, ExtractMethod
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
    """
    Registry for Extract implementations.

    Maps ExtractFormat enum values to concrete ExtractAbstract implementations.
    """


class Extract(Generic[ExtractModelT]):
    """
    Abstract base class for data extraction operations.

    This class defines the interface for all extraction implementations,
    supporting both batch and streaming extractions.
    """

    extract_model_concrete: type[ExtractModelT]

    def __init__(self, model: ExtractModelT) -> None:
        """
        Initialize the extraction operation.

        Args:
            model: Configuration model for the extraction
        """
        self.model = model
        self.data_registry = DataFrameRegistry()

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """
        Create an extraction instance from a configuration dictionary.

        Args:
            dict_: Configuration dictionary containing extraction specifications

        Returns:
            An initialized extraction instance

        Raises:
            DictKeyError: If required keys are missing from the configuration
        """
        model = cls.extract_model_concrete.from_dict(dict_=dict_)
        return cls(model=model)

    @abstractmethod
    def _extract_batch(self) -> DataFrame:
        """
        Extract data in batch mode.

        Returns:
            The extracted data as a DataFrame
        """

    @abstractmethod
    def _extract_streaming(self) -> DataFrame:
        """
        Extract data in streaming mode.

        Returns:
            The extracted data as a streaming DataFrame
        """

    def extract(self) -> None:
        """
        Main extraction method.
        """
        spark_handler: SparkHandler = SparkHandler()
        spark_handler.add_configs(options=self.model.options)

        if self.model.method == ExtractMethod.BATCH:
            self.data_registry[self.model.name] = self._extract_batch()
        elif self.model.method == ExtractMethod.STREAMING:
            self.data_registry[self.model.name] = self._extract_streaming()
        else:
            raise ValueError(f"Extraction method {self.model.method} is not supported for Pyspark.")


@ExtractRegistry.register(ExtractFormat.PARQUET)
@ExtractRegistry.register(ExtractFormat.JSON)
@ExtractRegistry.register(ExtractFormat.CSV)
class ExtractFile(Extract[ExtractFileModel]):
    """
    Abstract class for file extraction.
    """

    extract_model_concrete = ExtractFileModel
    _spark_handler: SparkHandler = SparkHandler()

    def _extract_batch(self) -> DataFrame:
        """
        Read from file in batch mode using PySpark.
        """

        return self._spark_handler.session.read.load(
            path=self.model.location,
            format=self.model.data_format.value,
            schema=self.model.schema,
            **self.model.options,
        )

    def _extract_streaming(self) -> DataFrame:
        """
        Read from file in streaming mode using PySpark.
        """
        return self._spark_handler.session.readStream.load(
            path=self.model.location,
            format=self.model.data_format.value,
            schema=self.model.schema,
            **self.model.options,
        )


class ExtractContext:
    """
    Abstract context class for creating and managing extraction strategies.

    This class implements the Strategy pattern for data extraction, allowing
    different extraction implementations to be selected based on the data format.
    """

    @classmethod
    def factory(cls, dict_: dict[str, type[Extract]]) -> type[Extract]:
        """
        Create an appropriate extract class based on the format specified in the configuration.

        This factory method uses the ExtractRegistry to look up the appropriate
        implementation class based on the data format.

        Args:
            dict_: Configuration dictionary that must include a 'data_format' key
                compatible with the ExtractFormat enum

        Returns:
            The concrete extraction class for the specified format

        Raises:
            NotImplementedError: If the specified extract format is not supported
            KeyError: If the 'data_format' key is missing from the configuration
        """
        try:
            extract_format = ExtractFormat(dict_[DATA_FORMAT])
            return ExtractRegistry.get(extract_format)
        except KeyError as e:
            format_name = dict_.get(DATA_FORMAT, "<missing>")
            raise NotImplementedError(f"Extract format {format_name} is not supported.") from e
