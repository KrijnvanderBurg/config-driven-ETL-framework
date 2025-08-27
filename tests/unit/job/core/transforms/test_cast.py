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

    def test_transform_calls_withColumn_for_each_cast(self) -> None:
        """Test that transform creates withColumn calls for each column cast."""
        config = {
            "function": "cast",
            "arguments": {
                "columns": [
                    {"column_name": "age", "cast_type": "integer"},
                    {"column_name": "price", "cast_type": "double"},
                ]
            },
        }
        function = CastFunction.from_dict(config)

        # Simple mock that tracks withColumn calls
        class MockDataFrame:
            def __init__(self):
                self.with_column_calls = []
                
            def withColumn(self, col_name, col_expr):
                self.with_column_calls.append((col_name, str(col_expr)))
                return self  # Chain calls
        
        mock_df = MockDataFrame()
        transform_func = function.transform()
        
        result = transform_func(mock_df)
        
        assert len(mock_df.with_column_calls) == 2
        assert mock_df.with_column_calls[0][0] == "age"  # First column name
        assert mock_df.with_column_calls[1][0] == "price"  # Second column name
        assert result is mock_df

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

    def test_transform_with_empty_columns_does_nothing(self) -> None:
        """Test that transform with no columns returns original DataFrame."""
        config = {"function": "cast", "arguments": {"columns": []}}
        function = CastFunction.from_dict(config)
        
        class MockDataFrame:
            def __init__(self):
                self.with_column_calls = []
                
            def withColumn(self, col_name, col_expr):
                self.with_column_calls.append((col_name, col_expr))
                return self
        
        mock_df = MockDataFrame()
        transform_func = function.transform()
        
        result = transform_func(mock_df)
        
        assert len(mock_df.with_column_calls) == 0  # No calls made
        assert result is mock_df
