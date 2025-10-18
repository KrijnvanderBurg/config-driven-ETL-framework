"""Unit tests for FunctionSpark base class used in Spark transforms."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

import pytest

from samara.runtime.jobs.models.model_transform import ArgsModel
from samara.runtime.jobs.spark.transforms.base import FunctionSpark


class _DummyArgs(ArgsModel):
    """Minimal arguments model for testing DummyFunction."""

    test_arg: str | None = None


class DummyFunction(FunctionSpark):
    """Minimal concrete Function implementation for testing purposes.

    This dummy function is used to test the FunctionSpark base class
    without requiring actual Spark transformations.
    """

    function: Literal["dummy"]
    arguments: _DummyArgs

    def transform(self) -> Callable[..., Any | None]:
        """Return a callable that passes through the input unchanged.

        Returns:
            A callable function that returns its input as-is
        """

        def _f(df: Any = None) -> Any | None:
            return df

        return _f


class TestFunctionSparkCreation:
    """Tests for creating FunctionSpark instances."""

    @pytest.fixture
    def function_config(self) -> dict:
        """Provide configuration dictionary for creating a DummyFunction.

        Returns:
            Configuration dictionary with function type and arguments
        """
        return {"function": "dummy", "arguments": {"test_arg": "example"}}

    def test_function_creation__from_config__creates_valid_model(self, function_config: dict) -> None:
        """Test that FunctionSpark can be instantiated from configuration dictionary."""
        # Act
        function = DummyFunction(**function_config)

        # Assert
        assert function.function == "dummy"
        assert isinstance(function.arguments, _DummyArgs)
        assert function.arguments.test_arg == "example"

    def test_function_creation__with_none_arg__creates_valid_model(self) -> None:
        """Test that FunctionSpark can be created with None argument values."""
        # Arrange
        config = {"function": "dummy", "arguments": {"test_arg": None}}

        # Act
        function = DummyFunction(**config)

        # Assert
        assert function.function == "dummy"
        assert function.arguments.test_arg is None


class TestFunctionSparkTransform:
    """Tests for FunctionSpark transform method behavior."""

    @pytest.fixture
    def function_obj(self) -> DummyFunction:
        """Provide a DummyFunction instance for testing.

        Returns:
            Configured DummyFunction instance
        """
        return DummyFunction(function="dummy", arguments=_DummyArgs(test_arg="example"))

    def test_transform__returns_callable(self, function_obj: DummyFunction) -> None:
        """Test that transform method returns a callable function."""
        # Act
        transform_fn = function_obj.transform()

        # Assert
        assert isinstance(transform_fn, Callable)

    def test_transform__callable_returns_input_unchanged(self, function_obj: DummyFunction) -> None:
        """Test that the returned callable passes through input unchanged."""
        # Arrange
        transform_fn = function_obj.transform()
        sample_input = object()

        # Act
        result = transform_fn(sample_input)

        # Assert
        assert result is sample_input

    def test_transform__callable_handles_none_input(self, function_obj: DummyFunction) -> None:
        """Test that the returned callable correctly handles None input."""
        # Arrange
        transform_fn = function_obj.transform()

        # Act
        result = transform_fn(None)

        # Assert
        assert result is None
