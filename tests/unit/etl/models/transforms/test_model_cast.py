"""Unit tests for the CastFunctionModel class."""

from typing import Any

import pytest

from flint.etl.models.transforms.model_cast import CastFunctionModel
from flint.exceptions import FlintConfigurationKeyError


class TestCastFunctionModel:
    """Unit tests for the CastFunctionModel transformation model."""

    @pytest.fixture
    def test_columns(self) -> list[tuple[str, str]]:
        """Return test column data for fixtures."""
        return [
            ("age", "integer"),
            ("price", "decimal(10,2)"),
            ("is_active", "boolean"),
            ("date_column", "date"),
        ]

    @pytest.fixture
    def valid_cast_dict(self, test_columns) -> dict[str, Any]:
        """Return a valid dictionary for creating a CastFunctionModel."""
        return {
            "function": "cast",
            "arguments": {"columns": [{"column_name": name, "cast_type": type_} for name, type_ in test_columns]},
        }

    @pytest.fixture
    def cast_args(self, test_columns) -> CastFunctionModel.Args:
        """Return CastFunctionModel.Args instance."""
        return CastFunctionModel.Args(
            columns=[CastFunctionModel.Column(column_name=name, cast_type=type_) for name, type_ in test_columns]
        )

    @pytest.fixture
    def cast_model_cls(self, cast_args) -> CastFunctionModel:
        """Return initialized CastFunctionModel instance."""
        return CastFunctionModel(function="cast", arguments=cast_args)

    def test_cast_function_model_cls_args(self, cast_args, test_columns) -> None:
        """Test CastFunctionModel.Args initialization."""
        assert len(cast_args.columns) == len(test_columns)

        # Check all columns
        for i, (name, type_) in enumerate(test_columns):
            assert cast_args.columns[i].column_name == name
            assert cast_args.columns[i].cast_type == type_

    def test_cast_function_model_cls_initialization(
        self, cast_model_cls: CastFunctionModel, cast_args: CastFunctionModel.Args, test_columns
    ) -> None:
        """Test that CastFunctionModel can be initialized with valid parameters."""
        assert cast_model_cls.function == "cast"
        assert cast_model_cls.arguments is cast_args
        assert len(cast_model_cls.arguments.columns) == len(test_columns)

    def test_from_dict_valid(self, valid_cast_dict, test_columns) -> None:
        """Test from_dict method with valid dictionary."""
        # Act
        model = CastFunctionModel.from_dict(valid_cast_dict)

        # Assert
        assert model.function == "cast"
        assert len(model.arguments.columns) == len(test_columns)

        # Verify all columns by name and type
        for name, type_ in test_columns:
            col = next(col for col in model.arguments.columns if col.column_name == name)
            assert col.cast_type == type_

    def test_from_dict_missing_keys(self) -> None:
        """Test from_dict with missing keys raises ConfigurationKeyError."""
        # Test cases with different missing keys
        error_cases = [
            ({"arguments": {"columns": [{"column_name": "age", "cast_type": "integer"}]}}, "function"),
            ({"function": "cast"}, "arguments"),
            ({"function": "cast", "arguments": {}}, "columns"),
        ]

        # Act & Assert for each case
        for invalid_dict, _ in error_cases:
            with pytest.raises(FlintConfigurationKeyError):
                CastFunctionModel.from_dict(invalid_dict)
