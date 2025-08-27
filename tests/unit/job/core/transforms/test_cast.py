"""Unit tests for the CastFunction class."""

import pytest

from flint.job.core.transform import TransformFunctionRegistry
from flint.job.core.transforms.cast import CastFunction


class TestCastFunction:
    """Unit tests for the Cast transform function."""

    def test_registration(self) -> None:
        """Test that CastFunction is registered correctly."""
        assert TransformFunctionRegistry.get("cast") == CastFunction

    def test_from_dict_single_column(self) -> None:
        """Test creating CastFunction from dict with single column."""
        config = {"function": "cast", "arguments": {"columns": [{"column_name": "age", "cast_type": "integer"}]}}

        function = CastFunction.from_dict(config)

        assert function.model.function == "cast"
        assert len(function.model.arguments.columns) == 1
        assert function.model.arguments.columns[0].column_name == "age"
        assert function.model.arguments.columns[0].cast_type == "integer"

    def test_from_dict_multiple_columns(self) -> None:
        """Test creating CastFunction from dict with multiple columns."""
        config = {
            "function": "cast",
            "arguments": {
                "columns": [
                    {"column_name": "age", "cast_type": "integer"},
                    {"column_name": "price", "cast_type": "decimal(10,2)"},
                    {"column_name": "active", "cast_type": "boolean"},
                ]
            },
        }

        function = CastFunction.from_dict(config)

        assert len(function.model.arguments.columns) == 3
        assert function.model.arguments.columns[0].cast_type == "integer"
        assert function.model.arguments.columns[1].cast_type == "decimal(10,2)"
        assert function.model.arguments.columns[2].cast_type == "boolean"

    def test_transform_returns_callable(self) -> None:
        """Test that transform returns a callable function."""
        config = {"function": "cast", "arguments": {"columns": [{"column_name": "age", "cast_type": "integer"}]}}
        function = CastFunction.from_dict(config)

        transform_func = function.transform()

        assert callable(transform_func)

    def test_invalid_config_missing_columns(self) -> None:
        """Test that invalid config raises appropriate error."""
        config = {"function": "cast", "arguments": {}}

        with pytest.raises(Exception):  # Should fail due to missing columns
            CastFunction.from_dict(config)

    def test_invalid_config_empty_columns(self) -> None:
        """Test that empty columns list is handled."""
        config = {"function": "cast", "arguments": {"columns": []}}
        function = CastFunction.from_dict(config)

        assert len(function.model.arguments.columns) == 0
