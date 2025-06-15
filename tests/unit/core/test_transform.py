"""
Unit tests for the transform module.
"""

from typing import Any, Callable
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrame

from flint.core.transform import Function, Transform, TransformFunctionRegistry
from flint.models.model_transform import TransformModel
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


class MockFunctionModel:
    """Mock function model for testing."""

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]):
        """Mock from_dict method."""
        return cls()


class TestFunction(Function[MockFunctionModel]):
    """Test implementation of Function abstract class."""

    model_cls = MockFunctionModel  # This was the issue - should be model_cls, not model_concrete

    def transform(self) -> Callable[..., Any]:
        """Implementation of abstract method."""

        def transform_func(df: DataFrame) -> None:
            """Mock transform function."""
            # Mock transformation implementation

        return transform_func


class TestTransform:
    """Unit tests for the Transform class and its implementations."""

    def test_transform_initialization(self) -> None:
        """Test Transform initialization."""
        # Arrange
        model = TransformModel(name="test_transform", upstream_name="source")
        functions = [TestFunction(model=MockFunctionModel())]

        # Act
        transform = Transform(model=model, functions=functions)

        # Assert
        assert transform.model == model
        assert transform.functions == functions
        assert isinstance(transform.data_registry, DataFrameRegistry)

    def test_from_dict_with_no_functions(self) -> None:
        """Test creating a Transform from a dict with no functions."""
        # Arrange
        transform_dict = {"name": "test_transform", "upstream_name": "source", "functions": []}

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
        mock_get.return_value = TestFunction

        transform_dict = {
            "name": "test_transform",
            "upstream_name": "source",
            "functions": [{"function": "test_function", "arguments": {"key": "value"}}],
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
        }

        # Act & Assert
        with pytest.raises(NotImplementedError):
            Transform.from_dict(transform_dict)

    def test_transform_method(self) -> None:
        """Test the transform method."""
        # Arrange
        model = TransformModel(name="test_transform", upstream_name="source")

        # Create mock function with callable
        function = TestFunction(model=MockFunctionModel())
        function.callable_ = MagicMock()
        function.callable_.return_value = "test_dataframe"  # Set return value for the mock

        transform = Transform(model=model, functions=[function])

        # Add test data to registry
        transform.data_registry["source"] = "test_dataframe"

        # Act
        transform.transform()

        # Assert
        assert transform.data_registry["test_transform"] == "test_dataframe"
        function.callable_.assert_called_once_with(df="test_dataframe")  # Fix parameter names
