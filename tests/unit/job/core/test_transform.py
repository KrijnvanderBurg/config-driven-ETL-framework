"""
Unit tests for the transform module.
"""

from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrame

from flint.job.core.transform import Function, Transform, TransformFunctionRegistry
from flint.job.models.model_transform import ArgsModel, FunctionModel, TransformModel
from flint.types import DataFrameRegistry


class TestTransformFunctionRegistry:
    """
    Unit tests for the TransformFunctionRegistry class.
    """

    def test_registry_is_singleton(self) -> None:
        """Test that TransformFunctionRegistry is a singleton."""
        # Arrange & Act
        registry1 = TransformFunctionRegistry()
        registry2 = TransformFunctionRegistry()

        # Assert
        assert registry1 is registry2
        assert id(registry1) == id(registry2)

    def test_register_and_get_function(self) -> None:
        """Test registering and retrieving a function."""
        # Arrange
        registry = TransformFunctionRegistry()

        # Mock function class
        mock_function_class = MagicMock()

        # Act
        registry.register("test_function")(mock_function_class)
        retrieved_class = registry.get("test_function")

        # Assert
        assert retrieved_class == mock_function_class

    def test_get_nonexistent_function_raises_key_error(self) -> None:
        """Test that getting a non-existent function raises KeyError."""
        # Arrange
        registry = TransformFunctionRegistry()

        # Act & Assert
        with pytest.raises(KeyError):
            registry.get("nonexistent_function")


class MockArgsModel(ArgsModel):
    """Create a mock ArgsModel for testing."""

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> "MockArgsModel":
        """Mock from_dict implementation."""
        return cls()


class MockFunctionModel(FunctionModel[MockArgsModel]):
    """Mock function model for testing."""

    function: str
    arguments: MockArgsModel

    def __init__(self) -> None:
        """Initialize with default values for testing."""
        self.function = "test_function"
        self.arguments = MockArgsModel()

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> "MockFunctionModel":
        """Mock from_dict method."""
        return cls()


class MockFunction(Function[MockFunctionModel]):
    """Test implementation of Function abstract class."""

    model_cls = MockFunctionModel  # Define the class variable properly

    def transform(self) -> Callable[..., Any]:
        """Implementation of abstract method."""

        def transform_func(df: DataFrame) -> DataFrame:
            """Mock transform function."""
            # Mock transformation implementation
            return df

        return transform_func


class TestTransform:
    """Unit tests for the Transform class and its implementations."""

    def test_transform_initialization(self) -> None:
        """Test Transform initialization."""
        # Arrange
        model = TransformModel(name="test_transform", upstream_name="source", options={})
        function = MockFunction(model=MockFunctionModel())
        functions: list[Function[MockFunctionModel]] = [function]

        # Act
        transform = Transform(model=model, functions=functions)

        # Assert
        assert transform.model == model
        assert transform.functions == functions
        assert isinstance(transform.data_registry, DataFrameRegistry)

    def test_from_dict_with_no_functions(self) -> None:
        """Test creating a Transform from a dict with no functions."""
        # Arrange
        transform_dict = {"name": "test_transform", "upstream_name": "source", "functions": [], "options": {}}

        # Act
        transform = Transform.from_dict(transform_dict)

        # Assert
        assert transform.model.name == "test_transform"
        assert transform.model.upstream_name == "source"
        assert len(transform.functions) == 0

    @patch.object(TransformFunctionRegistry, "get")
    def test_from_dict_with_functions(self, mock_get: MagicMock) -> None:
        """Test creating a Transform from a dict with functions."""
        # Arrange
        mock_get.return_value = MockFunction

        transform_dict = {
            "name": "test_transform",
            "upstream_name": "source",
            "functions": [{"function": "test_function", "arguments": {"key": "value"}}],
            "options": {},
        }

        # Act
        transform = Transform.from_dict(transform_dict)

        # Assert
        assert transform.model.name == "test_transform"
        assert transform.model.upstream_name == "source"
        assert len(transform.functions) == 1
        mock_get.assert_called_once_with("test_function")

    @patch.object(TransformFunctionRegistry, "get")
    def test_from_dict_with_unsupported_function(self, mock_get: MagicMock) -> None:
        """Test that creating a Transform with an unsupported function raises NotImplementedError."""
        # Arrange
        mock_get.side_effect = KeyError("Unsupported function")

        transform_dict = {
            "name": "test_transform",
            "upstream_name": "source",
            "functions": [{"function": "unknown_function", "arguments": {"key": "value"}}],
            "options": {},
        }

        # Act & Assert
        with pytest.raises(NotImplementedError):
            Transform.from_dict(transform_dict)

    def test_transform_method(self) -> None:
        """Test the transform method."""
        # Arrange
        model = TransformModel(name="test_transform", upstream_name="source", options={})

        # Create mock function with callable
        function = MockFunction(model=MockFunctionModel())
        function.callable_ = MagicMock()

        # Create mock dataframes that have count() method
        mock_input_df = MagicMock()
        mock_input_df.count.return_value = 100
        mock_output_df = MagicMock()
        mock_output_df.count.return_value = 50

        function.callable_.return_value = mock_output_df

        transform = Transform(model=model, functions=[function])

        # Add test data to registry
        transform.data_registry["source"] = mock_input_df

        # Act
        transform.transform()

        # Assert
        assert transform.data_registry["test_transform"] == mock_output_df
        function.callable_.assert_called_once_with(df=mock_input_df)
