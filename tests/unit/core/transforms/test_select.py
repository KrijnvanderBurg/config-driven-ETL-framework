"""
Unit tests for the SelectFunction transform.
"""

from unittest.mock import MagicMock, patch

from pyspark.sql import DataFrame

from ingestion_framework.core.transform import TransformFunctionRegistry
from ingestion_framework.core.transforms.select import SelectFunction
from ingestion_framework.models.transforms.select import SelectFunctionModel
from ingestion_framework.types import DataFrameRegistry


class TestSelectFunction:
    """
    Unit tests for the SelectFunction class.
    """

    def test_registration(self) -> None:
        """Test that SelectFunction is registered correctly."""
        # Arrange
        registry = TransformFunctionRegistry()

        # Act
        function_class = registry.get("select")

        # Assert
        assert function_class == SelectFunction

    def test_initialization(self) -> None:
        """Test SelectFunction initialization."""
        # Arrange
        model = MagicMock(spec=SelectFunctionModel)

        # Act
        function = SelectFunction(model=model)

        # Assert
        assert function.model == model
        assert callable(function.callable_)

    @patch.object(SelectFunctionModel, "from_dict")
    def test_from_dict(self, mock_from_dict: MagicMock) -> None:
        """Test creating a SelectFunction from a dict."""
        # Arrange
        function_dict = {"function": "select", "arguments": {"columns": ["col1", "col2"]}}

        mock_model = MagicMock(spec=SelectFunctionModel)
        mock_from_dict.return_value = mock_model

        # Act
        function = SelectFunction.from_dict(function_dict)

        # Assert
        assert function.model == mock_model
        mock_from_dict.assert_called_once_with(dict_=function_dict)

    def test_transform_function(self) -> None:
        """Test the transform function."""
        # Arrange
        args_mock = MagicMock()
        args_mock.columns = ["col1", "col2"]

        model = MagicMock(spec=SelectFunctionModel)
        model.arguments = args_mock

        function = SelectFunction(model=model)

        # Create mock DataFrame
        mock_df = MagicMock(spec=DataFrame)
        mock_select_result = MagicMock(spec=DataFrame)
        mock_df.select.return_value = mock_select_result

        # Create mock DataFrameRegistry
        dataframe_registry = DataFrameRegistry()
        dataframe_registry["test_df"] = mock_df

        # Act
        transform_func = function.transform()
        transform_func(dataframe_registry=dataframe_registry, dataframe_name="test_df")

        # Assert
        mock_df.select.assert_called_once_with(*model.arguments.columns)
        assert dataframe_registry["test_df"] == mock_select_result

    def test_transform_integration(self) -> None:
        """Integration test for the transform function with real classes."""
        # Arrange
        function_dict = {"function": "select", "arguments": {"columns": ["col1", "col2"]}}

        # Act - Create function from dict and get transform callable
        function = SelectFunction.from_dict(function_dict)
        transform_func = function.callable_

        # Create mock DataFrame with proper select method
        mock_df = MagicMock()
        mock_select_result = MagicMock()
        mock_df.select.return_value = mock_select_result

        # Create DataFrameRegistry
        dataframe_registry = DataFrameRegistry()
        dataframe_registry["test_df"] = mock_df

        # Apply transform
        transform_func(dataframe_registry=dataframe_registry, dataframe_name="test_df")

        # Assert
        # 1. Verify select was called
        mock_df.select.assert_called_once()
        # 2. Verify the DataFrame in the registry was updated
        assert dataframe_registry["test_df"] == mock_select_result
