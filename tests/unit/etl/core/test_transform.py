"""
Unit tests for the transform module.
"""

from typing import Any
from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame

from flint.etl.core.transform import (
    ARGUMENTS,
    FUNCTION,
    FUNCTIONS,
    NAME,
    UPSTREAM_NAME,
    Transform,
    TransformFunctionRegistry,
)
from flint.etl.models.model_transform import OPTIONS, TransformModel
from flint.types import DataFrameRegistry


@pytest.fixture
def transform_model() -> TransformModel:
    """Create a default TransformModel instance for testing."""
    return TransformModel(name="test_transform", upstream_name="source", options={"option1": "value1"})


@pytest.fixture
def transform_model_no_options() -> TransformModel:
    """Create a TransformModel instance with no options for testing."""
    return TransformModel(name="test_transform", upstream_name="source", options={})


@pytest.fixture
def dataframe_registry(transform_model: TransformModel) -> DataFrameRegistry:
    """Create a DataFrameRegistry with test dataframe for testing."""
    mock_df = MagicMock(spec=DataFrame)
    registry = DataFrameRegistry()
    registry[transform_model.upstream_name] = mock_df
    return registry


@pytest.fixture
def cast_transform_dict() -> dict[str, Any]:
    """Create transform dict for cast function testing."""
    return {
        NAME: "test_transform",
        UPSTREAM_NAME: "source",
        FUNCTIONS: [{FUNCTION: "cast", ARGUMENTS: {"columns": ["column1"], "to_type": "string"}}],
        OPTIONS: {},
    }


@pytest.fixture
def multiple_transform_dict() -> dict[str, Any]:
    """Create transform dict with multiple functions for testing."""
    return {
        NAME: "test_transform",
        UPSTREAM_NAME: "source",
        FUNCTIONS: [
            {FUNCTION: "cast", ARGUMENTS: {"columns": ["column1"], "to_type": "string"}},
            {FUNCTION: "filter", ARGUMENTS: {"condition": "column1 IS NOT NULL"}},
        ],
        OPTIONS: {},
    }


class TestTransform:
    """Unit tests for the Transform class."""

    def test_init_with_empty_functions(self, transform_model: TransformModel) -> None:
        """Test Transform initialization with empty functions list."""
        transform = Transform(model=transform_model, functions=[])
        assert transform.model == transform_model
        assert transform.functions == []

    def test_init_with_mock_functions(self, transform_model: TransformModel) -> None:
        """Test Transform initialization with mock functions."""
        mock_function = MagicMock()
        functions = [mock_function]

        transform = Transform(model=transform_model, functions=functions)
        assert transform.model == transform_model
        assert transform.functions == functions

    def test_from_dict_with_no_functions(self) -> None:
        """Test creating a Transform from a dict with no functions."""
        transform_dict = {NAME: "test_transform", UPSTREAM_NAME: "source", FUNCTIONS: [], OPTIONS: {}}

        transform = Transform.from_dict(transform_dict)

        assert transform.model.name == "test_transform"
        assert transform.model.upstream_name == "source"
        assert transform.model.options == {}
        assert len(transform.functions) == 0

    def test_transform_with_dataframe_registry(self, dataframe_registry: DataFrameRegistry) -> None:
        """Test calling the transform method with dataframe registry."""
        transform_model = TransformModel(name="test_transform", upstream_name="source", options={})

        transform = Transform(model=transform_model, functions=[])
        transform.data_registry = dataframe_registry

        # This should not raise an error
        transform.transform()

        # Verify the dataframe was copied to the new name
        assert "test_transform" in transform.data_registry._items


class TestTransformFunctionRegistry:
    """Unit tests for the TransformFunctionRegistry class."""

    def test_registry_is_singleton(self) -> None:
        """Test that TransformFunctionRegistry is a singleton."""
        registry1 = TransformFunctionRegistry()
        registry2 = TransformFunctionRegistry()

        assert registry1 is registry2
        assert id(registry1) == id(registry2)

    def test_register_and_get_function(self) -> None:
        """Test registering and retrieving a function."""
        registry = TransformFunctionRegistry()

        # Mock function class
        mock_function_class = MagicMock()

        # Register the function
        registry.register("test_function")(mock_function_class)
        retrieved_class = registry.get("test_function")

        assert retrieved_class == mock_function_class

    def test_get_nonexistent_function_raises_key_error(self) -> None:
        """Test that getting a non-existent function raises KeyError."""
        registry = TransformFunctionRegistry()

        with pytest.raises(KeyError):
            registry.get("nonexistent_function")


class TestDataFrameRegistry:
    """Unit tests for DataFrameRegistry functionality in transform context."""

    def test_dataframe_registry_store_and_retrieve(self) -> None:
        """Test storing and retrieving dataframes from registry."""
        registry = DataFrameRegistry()
        mock_df = MagicMock(spec=DataFrame)

        registry["test_df"] = mock_df
        retrieved_df = registry["test_df"]

        assert retrieved_df == mock_df

    def test_dataframe_registry_nonexistent_key_error(self) -> None:
        """Test that accessing non-existent dataframe raises KeyError with helpful message."""
        registry = DataFrameRegistry()

        with pytest.raises(KeyError) as exc_info:
            _ = registry["nonexistent"]

        assert "DataFrame 'nonexistent' not found" in str(exc_info.value)
        assert "Available DataFrames" in str(exc_info.value)

    def test_dataframe_registry_list_available_frames(self) -> None:
        """Test that error message includes available dataframes."""
        registry = DataFrameRegistry()
        mock_df1 = MagicMock(spec=DataFrame)
        mock_df2 = MagicMock(spec=DataFrame)

        registry["df1"] = mock_df1
        registry["df2"] = mock_df2

        with pytest.raises(KeyError) as exc_info:
            _ = registry["nonexistent"]

        error_message = str(exc_info.value)
        assert "df1" in error_message
        assert "df2" in error_message
