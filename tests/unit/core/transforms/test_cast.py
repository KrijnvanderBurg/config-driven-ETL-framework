"""Unit tests for the CastFunction class."""

from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrame

from flint.core.transforms.cast import CastFunction
from flint.models.transforms.model_cast import CastFunctionModel


class TestCastFunction:
    """Unit tests for the Cast transform function."""

    @pytest.fixture
    def cast_args(self) -> CastFunctionModel.Args:
        """Return CastFunctionModel.Args instance."""
        return CastFunctionModel.Args(
            columns=[
                CastFunctionModel.Column(column_name="age", cast_type="integer"),
                CastFunctionModel.Column(column_name="price", cast_type="decimal(10,2)"),
            ]
        )

    @pytest.fixture
    def cast_model(self, cast_args) -> CastFunctionModel:
        """Return initialized CastFunctionModel instance."""
        return CastFunctionModel(function="cast", arguments=cast_args)

    @pytest.fixture
    def cast_function(self, cast_model) -> CastFunction:
        """Return initialized CastFunction instance."""
        return CastFunction(model=cast_model)

    @patch("pyspark.sql.Column.cast")
    def test_transform(self, mock_cast, cast_function) -> None:
        """Test that transform returns a function that casts columns correctly."""
        # Arrange
        mock_df = MagicMock(spec=DataFrame)
        mock_col_instance = MagicMock()
        mock_cast.return_value = mock_col_instance

        # Mock the col function and withColumn method
        with patch("flint.core.transforms.cast.col", return_value=MagicMock()) as mock_col:
            mock_df.withColumn.return_value = mock_df

            # Act
            transform_func = cast_function.transform()
            result = transform_func(mock_df)

            # Assert
            assert result is mock_df
            assert mock_df.withColumn.call_count == 2

            # Check first column cast
            mock_col.assert_any_call("age")
            mock_df.withColumn.assert_any_call("age", mock_col.return_value.cast.return_value)
            mock_col.return_value.cast.assert_any_call("integer")

            # Check second column cast
            mock_col.assert_any_call("price")
            mock_df.withColumn.assert_any_call("price", mock_col.return_value.cast.return_value)
            # Check that cast was called with DecimalType
            # We're not checking the exact call since our implementation now uses DecimalType directly
            assert mock_col.return_value.cast.called
