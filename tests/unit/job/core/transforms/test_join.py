"""Unit tests for the JoinFunction class."""

from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame

from flint.job.core.transforms.join import JoinFunction
from flint.job.models.transforms.model_join import JoinFunctionModel
from flint.types import DataFrameRegistry


class TestJoinFunction:
    """Unit tests for the Join transform function."""

    @pytest.fixture
    def join_args_single_column(self) -> JoinFunctionModel.Args:
        """Return JoinFunctionModel.Args instance with a single join column."""
        return JoinFunctionModel.Args(other_upstream_name="extract-orders", on="customer_id", how="inner")

    @pytest.fixture
    def join_args_multi_column(self) -> JoinFunctionModel.Args:
        """Return JoinFunctionModel.Args instance with multiple join columns."""
        return JoinFunctionModel.Args(other_upstream_name="extract-orders", on=["customer_id", "store_id"], how="left")

    @pytest.fixture
    def join_model_single_column(self, join_args_single_column) -> JoinFunctionModel:
        """Return initialized JoinFunctionModel instance with a single join column."""
        return JoinFunctionModel(function="join", arguments=join_args_single_column)

    @pytest.fixture
    def join_model_multi_column(self, join_args_multi_column) -> JoinFunctionModel:
        """Return initialized JoinFunctionModel instance with multiple join columns."""
        return JoinFunctionModel(function="join", arguments=join_args_multi_column)

    @pytest.fixture
    def mock_registry(self) -> DataFrameRegistry:
        """Return a mock DataFrameRegistry."""
        registry = MagicMock(spec=DataFrameRegistry)
        # Set up the registry to return a mock DataFrame when accessed with "extract-orders"
        registry.__getitem__.return_value = MagicMock(spec=DataFrame)
        return registry

    @pytest.fixture
    def join_function_single_column(self, join_model_single_column, mock_registry) -> JoinFunction:
        """Return initialized JoinFunction instance with a single column join."""
        function = JoinFunction(model=join_model_single_column)
        function.data_registry = mock_registry
        return function

    @pytest.fixture
    def join_function_multi_column(self, join_model_multi_column, mock_registry) -> JoinFunction:
        """Return initialized JoinFunction instance with a multi-column join."""
        function = JoinFunction(model=join_model_multi_column)
        function.data_registry = mock_registry
        return function

    def test_transform_single_column(self, join_function_single_column, mock_registry) -> None:
        """Test that transform returns a function that joins DataFrames correctly on a single column."""
        # Arrange
        mock_left_df = MagicMock(spec=DataFrame)
        mock_right_df = MagicMock(spec=DataFrame)
        mock_registry.__getitem__.return_value = mock_right_df

        # Mock the join method
        mock_left_df.join.return_value = MagicMock(spec=DataFrame)

        # Act
        transform_func = join_function_single_column.transform()
        result = transform_func(mock_left_df)

        # Assert
        mock_registry.__getitem__.assert_called_once_with("extract-orders")
        mock_left_df.join.assert_called_once_with(mock_right_df, on="customer_id", how="inner")
        assert result is mock_left_df.join.return_value

    def test_transform_multi_column(self, join_function_multi_column, mock_registry) -> None:
        """Test that transform returns a function that joins DataFrames correctly on multiple columns."""
        # Arrange
        mock_left_df = MagicMock(spec=DataFrame)
        mock_right_df = MagicMock(spec=DataFrame)
        mock_registry.__getitem__.return_value = mock_right_df

        # Mock the join method
        mock_left_df.join.return_value = MagicMock(spec=DataFrame)

        # Act
        transform_func = join_function_multi_column.transform()
        result = transform_func(mock_left_df)

        # Assert
        mock_registry.__getitem__.assert_called_once_with("extract-orders")
        mock_left_df.join.assert_called_once_with(mock_right_df, on=["customer_id", "store_id"], how="left")
        assert result is mock_left_df.join.return_value
