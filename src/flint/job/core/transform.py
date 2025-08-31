"""PySpark implementation for data transformation operations.

This module provides concrete implementations for transforming data using Apache PySpark.
It includes:

- Abstract base classes defining the transformation interface
- Function-based transformation support with configurable arguments
- Registry mechanisms for dynamically selecting transformation functions
- Configuration-driven transformation functionality

The Transform components represent the middle phase in the ETL pipeline, responsible
for manipulating data between extraction and loading.
"""

import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, Final, Generic, Self, TypeVar

from flint.job.models.model_transform import FunctionModel, TransformModel
from flint.types import DataFrameRegistry, RegistryDecorator, Singleton
from flint.utils.logger import get_logger
from flint.utils.spark import SparkHandler

logger: logging.Logger = get_logger(__name__)

FUNCTIONS: Final[str] = "functions"
FUNCTION: Final[str] = "function"
ARGUMENTS: Final[str] = "arguments"

NAME: Final[str] = "name"
UPSTREAM_NAME: Final[str] = "upstream_name"


FunctionModelT_co = TypeVar("FunctionModelT_co", bound=FunctionModel, covariant=True)


class TransformFunctionRegistry(RegistryDecorator, metaclass=Singleton):
    """Registry for transformation function implementations.

    A singleton registry that maps function names to their corresponding
    concrete Function implementations.

    This registry enables dynamic selection of the appropriate transformation
    function based on the function name specified in the configuration.

    Example:
        ```python
        # Register a new transformation function
        @TransformFunctionRegistry.register("filter_data")
        class FilterFunction(Function):
            # Implementation

        # Get the registered implementation for a function
        function_class = TransformFunctionRegistry.get("filter_data")
        ```
    """


class Function(Generic[FunctionModelT_co], ABC):
    """
     base class for transformation functions.

    This class defines the interface for all transformation functions in the system.
    Each function has a model that defines its behavior and parameters.
    """

    # Model class property with covariant type that allows subclasses to specify
    # more derived types than the base FunctionModel
    model_cls: type[FunctionModelT_co]

    def __init__(self, model: FunctionModelT_co) -> None:
        """
        Initialize a function transformation object.

        Args:
            model: The model object containing the function configuration.
        """
        logger.debug("Initializing Function with model: %s", model.function)
        self.model = model
        self.callable_ = self.transform()
        self.data_registry = DataFrameRegistry()
        logger.info("Function initialized successfully: %s", model.function)

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """
        Create a function instance from a configuration dictionary.

        Args:
            dict_: The configuration dictionary containing function specifications.

        Returns:
            A new function instance configured according to the provided parameters.

        Raises:
            FlintConfigurationKeyError: If required keys are missing from the configuration.
        """
        logger.debug("Creating Function from dictionary: %s", dict_)
        model = cls.model_cls.from_dict(dict_=dict_)
        instance = cls(model=model)
        logger.info("Successfully created Function instance: %s", model.function)
        return instance

    @abstractmethod
    def transform(self) -> Callable[..., Any]:
        """
        Create a callable transformation function based on the model.

        This method should implement the logic to create a function that
        can be called to transform data according to the model configuration.

        Returns:
            A callable function that applies the transformation to data.
        """


class Transform:
    """
    Concrete implementation for DataFrame transformation.

    This class provides functionality for transforming data.
    """

    def __init__(self, model: TransformModel, functions: list[Function[Any]]) -> None:
        logger.debug(
            "Initializing Transform - name: %s, upstream: %s, functions: %d",
            model.name,
            model.upstream_name,
            len(functions),
        )
        self.model = model
        self.functions = functions
        self.data_registry = DataFrameRegistry()
        logger.info("Transform initialized successfully: %s with %d functions", model.name, len(functions))

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """
        Create an instance of TransformAbstract from configuration.

        Args:
            dict_: Configuration dictionary containing transformation specifications.
                Must contain 'name' and 'upstream_name' keys, and optionally a 'functions' list.

        Returns:
            A new instance of the transformation class.

        Raises:
            FlintConfigurationKeyError: If required keys are missing from the configuration.
            NotImplementedError: If a specified function is not supported.
            Exception: If there's an unexpected error during Transform creation.
        """
        logger.debug("Creating Transform from dictionary: %s", dict_)

        model = TransformModel.from_dict(dict_=dict_)
        functions: list = []

        function_list = dict_.get(FUNCTIONS, [])
        logger.debug("Processing %d transformation functions", len(function_list))

        for functiondict_ in function_list:
            function_name: str = functiondict_[FUNCTION]
            logger.debug("Creating function instance: %s", function_name)

            try:
                function_concrete = TransformFunctionRegistry.get(function_name)
                function_instance = function_concrete.from_dict(dict_=functiondict_)
                functions.append(function_instance)
                logger.debug("Successfully created function: %s", function_name)
            except KeyError as e:
                raise NotImplementedError(f"{FUNCTION} {function_name} is not supported.") from e

        instance = cls(model=model, functions=functions)
        logger.info("Successfully created Transform: %s with %d functions", model.name, len(functions))
        return instance

    def transform(self) -> None:
        """
        Apply all transformation functions to the data source.

        This method performs the following steps:
        1. Copies the dataframe from the upstream source to current transform's name
        2. Sequentially applies each transformation function to the dataframe
        3. Each function updates the registry with its results

        Note:
            Functions are applied in the order they were defined in the configuration.
        """
        logger.info("Starting transformation for: %s from upstream: %s", self.model.name, self.model.upstream_name)

        spark_handler: SparkHandler = SparkHandler()
        logger.debug("Adding Spark configurations: %s", self.model.options)
        spark_handler.add_configs(options=self.model.options)

        # Copy the dataframe from upstream to current name
        logger.debug("Copying dataframe from %s to %s", self.model.upstream_name, self.model.name)
        self.data_registry[self.model.name] = self.data_registry[self.model.upstream_name]

        # Apply transformations
        logger.debug("Applying %d transformation functions", len(self.functions))
        for i, function in enumerate(self.functions):
            logger.debug("Applying function %d/%d: %s", i, len(self.functions), function.model.function)

            original_count = self.data_registry[self.model.name].count()
            self.data_registry[self.model.name] = function.callable_(df=self.data_registry[self.model.name])
            new_count = self.data_registry[self.model.name].count()

            logger.info(
                "Function %s applied - rows changed from %d to %d", function.model.function, original_count, new_count
            )

        logger.info("Transformation completed successfully for: %s", self.model.name)
