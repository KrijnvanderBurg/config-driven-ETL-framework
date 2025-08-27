"""Unit tests for the DropFunction class."""

import pytest

from flint.job.core.transform import TransformFunctionRegistry
from flint.job.core.transforms.drop import DropFunction


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

    def test_transform_passes_correct_arguments(self) -> None:
        """Test that the transform function passes the right arguments to DataFrame.drop()."""
        config = {"function": "drop", "arguments": {"columns": ["col1", "col2"]}}
        function = DropFunction.from_dict(config)
        
        # Create a simple mock that captures the call
        class MockDataFrame:
            def __init__(self):
                self.drop_called_with = None
                
            def drop(self, *args):
                self.drop_called_with = args
                return self  # Return self to simulate DataFrame behavior
        
        mock_df = MockDataFrame()
        transform_func = function.transform()
        
        result = transform_func(mock_df)
        
        assert mock_df.drop_called_with == ("col1", "col2")
        assert result is mock_df

    def test_invalid_config_missing_columns(self) -> None:
        """Test that invalid config raises appropriate error."""
        config = {"function": "drop", "arguments": {}}
        
        with pytest.raises(Exception):  # Should fail due to missing columns
            DropFunction.from_dict(config)
