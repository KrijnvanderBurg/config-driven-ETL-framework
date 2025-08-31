"""Data models for transformation operations in the ingestion framework.

This module defines the data models and configuration structures used for
representing transformation operations. It includes:

- Base models for transformation function arguments
- Data classes for structuring transformation configuration
- Utility methods for parsing and validating transformation parameters
- Constants for standard configuration keys

These models serve as the configuration schema for the Transform components
and provide a type-safe interface between configuration and implementation.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Final, Generic, Self, TypeVar

from flint.exceptions import FlintConfigurationKeyError
from flint.job.models import Model
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

FUNCTIONS: Final[str] = "functions"
FUNCTION: Final[str] = "function"
ARGUMENTS: Final[str] = "arguments"

NAME: Final[str] = "name"
UPSTREAM_NAME: Final[str] = "upstream_name"
OPTIONS: Final[str] = "options"


@dataclass
class ArgsModel(Model, ABC):
    """Abstract base class for transformation function arguments.

    Serves as the foundation for all argument containers used by
    transformation functions in the framework. Each concrete subclass
    should implement type-specific argument handling for different
    transformation operations.

    All transformation argument models should inherit from this class
    to ensure a consistent interface throughout the framework.
    """


ArgsT = TypeVar("ArgsT", bound=ArgsModel)


@dataclass
class FunctionModel(Model, Generic[ArgsT], ABC):
    """
    Model specification for transformation functions.

    This class represents the configuration for a transformation function,
    including its name and arguments.

    Args:
        function: Name of the transformation function to execute
        arguments: Arguments model specific to the transformation function
    """

    function: str
    arguments: ArgsT

    @classmethod
    @abstractmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """
        Create a transformation function model from a configuration dictionary.

        Args:
            dict_: The configuration dictionary containing:
                - 'function': The name of the function to execute
                - 'arguments': The arguments specification for the function

        Raises:
            FlintConfigurationKeyError: If required keys are missing from the configuration.
            NotImplementedError: If the subclass doesn't override this method.
        """


FunctionModelT = TypeVar("FunctionModelT", bound=FunctionModel)


@dataclass
class TransformModel(Model):
    """
    Modelification for  data transformation.

    Args:
        name: Identifier for this transformation operation
        upstream_name: Identifier(s) of the upstream component(s) providing data
        options: PySpark transformation options as key-value pairs

    Examples:
        >>> df = spark.createDataFrame(data=[("Alice", 27), ("Bob", 32),], schema=["name", "age"])
        >>> dict = {"function": "cast", "arguments": {"columns": {"age": "StringType",}}}
        >>> transform = TransformFunction.from_dict(dict=dict[str, Any])
        >>> df = df.transform(func=transform).printSchema()
        root
        |-- name: string (nullable = true)
        |-- age: string (nullable = true)
    """

    name: str
    upstream_name: str
    options: dict[str, str]

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """
        Create a TransformModelAbstract object from a dict_ dictionary.

        Args:
            dict_ (dict[str, Any]): The dict_ dictionary.

        Returns:
            TransformModelAbstract: The TransformModelAbstract object created from the dict_ dictionary.

        Example:
            >>> "transforms": [
            >>>     {
            >>>         "name": "bronze-test-transform-dev",
            >>>         "upstream_name": ["bronze-test-extract-dev"],
            >>>         "functions": [
            >>>             {"function": "cast", "arguments": {"columns": {"age": "LongType"}}},
            >>>             // etc.
            >>>         ],
            >>>     }
            >>> ],
        """
        logger.debug("Creating TransformModel from dictionary: %s", dict_)

        try:
            name = dict_[NAME]
            upstream_name = dict_[UPSTREAM_NAME]
            options = dict_[OPTIONS]
            logger.debug("Parsed transform model - name: %s, upstream: %s", name, upstream_name)
        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        model = cls(name=name, upstream_name=upstream_name, options=options)
        logger.info("Successfully created TransformModel: %s", name)
        return model
