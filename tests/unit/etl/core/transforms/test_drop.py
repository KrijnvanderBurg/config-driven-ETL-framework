"""Unit tests for the DropFunction class."""

import pytest

from flint.etl.core.transform import TransformFunctionRegistry
from flint.etl.core.transforms.drop import DropFunction


class TestDropFunction:
    """Unit tests for the Drop transform function."""

    def test_registration(self) -> None:
        """Test that DropFunction is registered correctly."""
        assert TransformFunctionRegistry.get("drop") == DropFunction

    def test_from_dict_valid_config(self) -> None:
        """Test creating DropFunction from valid configuration."""
        config = {"function": "drop", "arguments": {"columns": ["temp_col", "unused_field"]}}

        function = DropFunction.from_dict(config)

        assert function.model.function == "drop"
        assert function.model.arguments.columns == ["temp_col", "unused_field"]

    def test_from_dict_empty_columns(self) -> None:
        """Test creating DropFunction with empty columns list."""
        config = {"function": "drop", "arguments": {"columns": []}}

        function = DropFunction.from_dict(config)

        assert function.model.arguments.columns == []

    def test_transform_returns_callable(self) -> None:
        """Test that transform returns a callable function."""
        config = {"function": "drop", "arguments": {"columns": ["col1"]}}
        function = DropFunction.from_dict(config)

        transform_func = function.transform()

        assert callable(transform_func)

    def test_invalid_config_missing_columns(self) -> None:
        """Test that invalid config raises appropriate error."""
        config = {"function": "drop", "arguments": {}}

        with pytest.raises(Exception):  # Should fail due to missing columns
            DropFunction.from_dict(config)
