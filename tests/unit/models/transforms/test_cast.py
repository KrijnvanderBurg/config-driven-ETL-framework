"""Unit tests for the CastFunctionModel class."""

from typing import Any

import pytest

from flint.models.transforms.model_cast import CastFunctionModel, DictKeyError


class TestCastFunctionModel:
    """Unit tests for the CastFunctionModel transformation model."""

    @pytest.fixture
    def valid_cast_dict(self) -> dict[str, Any]:
        """Return a valid dictionary for creating a CastFunctionModel."""
        return {
            "function": "cast",
            "arguments": {
                "columns": [
                    {"column_name": "age", "cast_type": "integer"},
                    {"column_name": "price", "cast_type": "decimal(10,2)"},
                    {"column_name": "is_active", "cast_type": "boolean"},
                    {"column_name": "date_column", "cast_type": "date"},
                ]
            },
        }

    @pytest.fixture
    def cast_args(self) -> CastFunctionModel.Args:
        """Return CastFunctionModel.Args instance."""
        return CastFunctionModel.Args(
            columns=[
                CastFunctionModel.Column(column_name="age", cast_type="integer"),
                CastFunctionModel.Column(column_name="price", cast_type="decimal(10,2)"),
                CastFunctionModel.Column(column_name="is_active", cast_type="boolean"),
                CastFunctionModel.Column(column_name="date_column", cast_type="date"),
            ]
        )

    @pytest.fixture
    def cast_model_cls(self, cast_args) -> CastFunctionModel:
        """Return initialized CastFunctionModel instance."""
        return CastFunctionModel(function="cast", arguments=cast_args)

    def test_cast_function_model_cls_args(self, cast_args) -> None:
        """Test CastFunctionModel.Args initialization."""
        assert len(cast_args.columns) == 4
        # Check first column
        assert cast_args.columns[0].column_name == "age"
        assert cast_args.columns[0].cast_type == "integer"
        # Check second column
        assert cast_args.columns[1].column_name == "price"
        assert cast_args.columns[1].cast_type == "decimal(10,2)"
        # Check third column
        assert cast_args.columns[2].column_name == "is_active"
        assert cast_args.columns[2].cast_type == "boolean"
        # Check fourth column
        assert cast_args.columns[3].column_name == "date_column"
        assert cast_args.columns[3].cast_type == "date"

    def test_cast_function_model_cls_initialization(self, cast_model_cls: CastFunctionModel, cast_args) -> None:
        """Test that CastFunctionModel can be initialized with valid parameters."""
        assert cast_model_cls.function == "cast"
        assert cast_model_cls.arguments is cast_args
        assert len(cast_model_cls.arguments.columns) == 4

    def test_from_dict_valid(self, valid_cast_dict) -> None:
        """Test from_dict method with valid dictionary."""
        # Act
        model = CastFunctionModel.from_dict(valid_cast_dict)

        # Assert
        assert model.function == "cast"
        assert len(model.arguments.columns) == 4

        # Find columns by name for comparison
        age_col = next(col for col in model.arguments.columns if col.column_name == "age")
        price_col = next(col for col in model.arguments.columns if col.column_name == "price")
        is_active_col = next(col for col in model.arguments.columns if col.column_name == "is_active")
        date_col = next(col for col in model.arguments.columns if col.column_name == "date_column")

        # Check the cast types
        assert age_col.cast_type == "integer"
        assert price_col.cast_type == "decimal(10,2)"
        assert is_active_col.cast_type == "boolean"
        assert date_col.cast_type == "date"

    def test_from_dict_missing_function_key(self) -> None:
        """Test from_dict with missing function key raises DictKeyError."""
        # Arrange
        invalid_dict = {"arguments": {"columns": [{"column_name": "age", "cast_type": "integer"}]}}

        # Act & Assert
        with pytest.raises(DictKeyError) as excinfo:
            CastFunctionModel.from_dict(invalid_dict)
        assert "function" in str(excinfo.value)

    def test_from_dict_missing_arguments_key(self) -> None:
        """Test from_dict with missing arguments key raises DictKeyError."""
        # Arrange
        invalid_dict = {"function": "cast"}

        # Act & Assert
        with pytest.raises(DictKeyError) as excinfo:
            CastFunctionModel.from_dict(invalid_dict)
        assert "arguments" in str(excinfo.value)

    def test_from_dict_missing_columns_key(self) -> None:
        """Test from_dict with missing columns key raises DictKeyError."""
        # Arrange
        invalid_dict = {"function": "cast", "arguments": {}}

        # Act & Assert
        with pytest.raises(DictKeyError) as excinfo:
            CastFunctionModel.from_dict(invalid_dict)
        assert "columns" in str(excinfo.value)
