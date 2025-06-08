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

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, Final, Generic, Self, TypeVar

from ingestion_framework.models.model_transform import FunctionModel, TransformModel
from ingestion_framework.types import DataFrameRegistry, RegistryDecorator, Singleton

FUNCTIONS: Final[str] = "functions"
FUNCTION: Final[str] = "function"
ARGUMENTS: Final[str] = "arguments"

NAME: Final[str] = "name"
UPSTREAM_NAME: Final[str] = "upstream_name"


FunctionModelT = TypeVar("FunctionModelT", bound=FunctionModel)


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


class Function(Generic[FunctionModelT], ABC):
    """
     base class for transformation functions.

    This class defines the interface for all transformation functions in the system.
    Each function has a model that defines its behavior and parameters.
    """

    _model: type[FunctionModelT]

    def __init__(self, model: FunctionModelT) -> None:
        """
        Initialize a function transformation object.

        Args:
            model: The model object containing the function configuration.
        """
        self.model = model
        self.callable_ = self.transform()
        self.data_registry = DataFrameRegistry()

    @abstractmethod
    def transform(self) -> Callable[..., Any]:
        """
        Create a callable transformation function based on the model.

        This method should implement the logic to create a function that
        can be called to transform data according to the model configuration.

        Returns:
            A callable function that applies the transformation to data.
        """

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """
        Create a function instance from a configuration dictionary.

        Args:
            dict_: The configuration dictionary containing function specifications.

        Returns:
            A new function instance configured according to the provided parameters.

        Raises:
            DictKeyError: If required keys are missing from the configuration.
        """
        model = cls._model.from_dict(dict_=dict_)
        return cls(model=model)


FunctionT = TypeVar("FunctionT", bound=Function)


class TransformRegistry(RegistryDecorator, metaclass=Singleton):
    """
    Registry for Transform implementations.

    Maps transform enum values to concrete transform implementations.
    """


class Transform(Generic[FunctionT]):
    """
    Concrete implementation for DataFrame transformation.

    This class provides functionality for transforming data.
    """

    def __init__(self, model: TransformModel, functions: list[FunctionT]) -> None:
        self.model = model
        self.functions = functions
        self.data_registry = DataFrameRegistry()

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
            DictKeyError: If required keys are missing from the configuration.
            NotImplementedError: If a specified function is not supported.
        """
        model = TransformModel.from_dict(dict_=dict_)
        functions: list = []

        for functiondict_ in dict_.get(FUNCTIONS, []):
            function_name: str = functiondict_[FUNCTION]

            try:
                function_concrete = TransformFunctionRegistry.get(function_name)
                function_instance = function_concrete.from_dict(dict_=functiondict_)
                functions.append(function_instance)
            except KeyError as e:
                raise NotImplementedError(f"{FUNCTION} {function_name} is not supported.") from e

        return cls(model=model, functions=functions)

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
        # Copy the dataframe from upstream to current name
        self.data_registry[self.model.name] = self.data_registry[self.model.upstream_name]

        # Apply transformations
        for function in self.functions:
            self.data_registry[self.model.name] = function.callable_(df=self.data_registry[self.model.name])
