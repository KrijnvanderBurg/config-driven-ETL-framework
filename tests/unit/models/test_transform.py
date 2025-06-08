"""Unit tests for the TransformModel and related classes."""

from abc import ABC
from dataclasses import dataclass
from typing import Any, Self

import pytest

from ingestion_framework.models.transform import ArgsModel, DictKeyError, FunctionModel, TransformModel


# Define some simple concrete implementations for testing abstract classes
@dataclass
class ArgsModelTest(ArgsModel):
    """Test implementation of ArgsModel for testing."""

    value: str

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        return cls(value=dict_["value"])


@dataclass
class FunctionModelTest(FunctionModel):
    """Test implementation of FunctionModel for testing."""

    function: str
    arguments: ArgsModelTest

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        return cls(function=dict_["function"], arguments=ArgsModelTest.from_dict(dict_["arguments"]))


class TestArgsModelClass:
    """Tests for ArgsModel abstract class."""

    @pytest.fixture
    def args_model_test(self) -> ArgsModelTest:
        """Return an initialized ArgsModelTest instance."""
        return ArgsModelTest(value="test_value")

    @pytest.fixture
    def valid_args_dict(self) -> dict[str, str]:
        """Return a valid dictionary for creating ArgsModelTest."""
        return {"value": "test_value"}

    def test_args_model_is_abstract(self) -> None:
        """Test that ArgsModel is an abstract class."""
        assert issubclass(ArgsModel, ABC)

    def test_test_args_model_concrete(self, args_model_test, valid_args_dict) -> None:
        """Test concrete implementation of ArgsModel."""
        assert args_model_test.value == "test_value"

        # Test from_dict
        args_from_dict = ArgsModelTest.from_dict(valid_args_dict)
        assert args_from_dict.value == "test_value"


class TestFunctionModelClass:
    """Tests for FunctionModel abstract class."""

    @pytest.fixture
    def valid_function_dict(self) -> dict[str, Any]:
        """Return a valid dictionary for creating a FunctionModel."""
        return {"function": "test_function", "arguments": {"value": "test_value"}}

    @pytest.fixture
    def args_model_test(self):
        """Return an initialized ArgsModelTest instance."""
        return ArgsModelTest(value="test_value")

    @pytest.fixture
    def function_model_test(self, args_model_test) -> FunctionModelTest:
        """Return an initialized FunctionModelTest instance."""
        return FunctionModelTest(function="test_function", arguments=args_model_test)

    def test_function_model_init(self, function_model_test, args_model_test) -> None:
        """Test initialization of a FunctionModel implementation."""
        assert function_model_test.function == "test_function"
        assert function_model_test.arguments == args_model_test
        assert function_model_test.arguments.value == "test_value"

    def test_function_model_from_dict(self, valid_function_dict) -> None:
        """Test from_dict method for a FunctionModel implementation."""
        func_model = FunctionModelTest.from_dict(valid_function_dict)

        assert func_model.function == "test_function"
        assert func_model.arguments.value == "test_value"

    def test_function_model_abstract_from_dict(self):
        """Test that FunctionModel's from_dict raises NotImplementedError when not overridden."""

        # Create a new class that does not override from_dict
        class BrokenFunctionModel(FunctionModel):
            pass

        with pytest.raises(NotImplementedError):
            BrokenFunctionModel.from_dict({})


class TestTransformModel:
    """Tests for TransformModel class."""

    @pytest.fixture
    def valid_transform_dict(self):
        """Return a valid dictionary for creating a TransformModel."""
        return {"name": "test_transform", "upstream_name": "test_extract"}

    @pytest.fixture
    def transform_model(self):
        """Return an initialized TransformModel instance."""
        return TransformModel(name="test_transform", upstream_name="test_extract")

    def test_transform_model_initialization(self, transform_model) -> None:
        """Test that TransformModel can be initialized with valid parameters."""
        assert transform_model.name == "test_transform"
        assert transform_model.upstream_name == "test_extract"

    def test_transform_model_from_dict_valid(self, valid_transform_dict) -> None:
        """Test from_dict method with valid dictionary."""
        # Execute
        model = TransformModel.from_dict(valid_transform_dict)

        # Assert
        assert model.name == "test_transform"
        assert model.upstream_name == "test_extract"

    def test_transform_model_from_dict_missing_key(self, valid_transform_dict) -> None:
        """Test from_dict method with missing key."""
        # Remove required 'name' key
        invalid_dict = valid_transform_dict.copy()
        del invalid_dict["name"]

        # Execute and Assert
        with pytest.raises(DictKeyError):
            TransformModel.from_dict(invalid_dict)
