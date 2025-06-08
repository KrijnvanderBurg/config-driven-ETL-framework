"""
Unit tests for the CalculateBirthYearFunction transform.
"""

from unittest.mock import MagicMock, patch

from pyspark.sql import DataFrame

from ingestion_framework.core.transform import TransformFunctionRegistry
from ingestion_framework.core.transforms.calculate_birth_year import CalculateBirthYearFunction
from ingestion_framework.models.transforms.model_calculate_birth_year import CalculateBirthYearFunctionModel
from ingestion_framework.types import DataFrameRegistry


class TestCalculateBirthYearFunction:
    """
    Unit tests for the CalculateBirthYearFunction class.
    """

    def test_registration(self) -> None:
        """Test that CalculateBirthYearFunction is registered correctly."""
        # Arrange
        registry = TransformFunctionRegistry()

        # Act
        function_class = registry.get("calculate_birth_year")

        # Assert
        assert function_class == CalculateBirthYearFunction

    def test_initialization(self) -> None:
        """Test CalculateBirthYearFunction initialization."""
        # Arrange
        model = MagicMock(spec=CalculateBirthYearFunctionModel)

        # Act
        function = CalculateBirthYearFunction(model=model)

        # Assert
        assert function.model == model
        # Check that transform() returns a callable function
        assert callable(function.transform())

    @patch.object(CalculateBirthYearFunctionModel, "from_dict")
    def test_from_dict(self, mock_from_dict: MagicMock) -> None:
        """Test creating a CalculateBirthYearFunction from a dict."""
        # Arrange
        function_dict = {
            "function": "calculate_birth_year",
            "arguments": {"current_year": 2025, "age_column": "age", "birth_year_column": "birth_year"},
        }

        mock_model = MagicMock(spec=CalculateBirthYearFunctionModel)
        mock_from_dict.return_value = mock_model

        # Act
        function = CalculateBirthYearFunction.from_dict(function_dict)

        # Assert
        assert function.model == mock_model
        mock_from_dict.assert_called_once_with(dict_=function_dict)

    def test_transform_function(self) -> None:
        """Test the transform function."""
        # Arrange
        args_mock = MagicMock()
        args_mock.current_year = 2025
        args_mock.age_column = "age"
        args_mock.birth_year_column = "birth_year"

        model = MagicMock(spec=CalculateBirthYearFunctionModel)
        model.arguments = args_mock

        function = CalculateBirthYearFunction(model=model)

        # Create mock DataFrame
        mock_df = MagicMock(spec=DataFrame)
        mock_withcolumn_result = MagicMock(spec=DataFrame)
        mock_df.withColumn.return_value = mock_withcolumn_result

        # Create mock DataFrameRegistry
        dataframe_registry = DataFrameRegistry()
        dataframe_registry["test_df"] = mock_df

        # Act
        transform_func = function.transform()
        transform_func(dataframe_registry=dataframe_registry, dataframe_name="test_df")

        # Assert
        mock_df.withColumn.assert_called_once()
        assert dataframe_registry["test_df"] == mock_withcolumn_result

    def test_transform_integration(self) -> None:
        """Integration test for the transform function with real classes."""
        # Arrange
        function_dict = {
            "function": "calculate_birth_year",
            "arguments": {"current_year": 2025, "age_column": "age", "birth_year_column": "birth_year"},
        }

        # Act - Create function from dict and get transform callable
        function = CalculateBirthYearFunction.from_dict(function_dict)
        transform_func = function.transform()

        # Create mock DataFrame with proper withColumn method
        mock_df = MagicMock()
        mock_withcolumn_result = MagicMock()
        mock_df.withColumn.return_value = mock_withcolumn_result

        # Create DataFrameRegistry
        dataframe_registry = DataFrameRegistry()
        dataframe_registry["test_df"] = mock_df

        # Apply transform
        transform_func(dataframe_registry=dataframe_registry, dataframe_name="test_df")

        # Assert
        # 1. Verify withColumn was called
        mock_df.withColumn.assert_called_once()
        # 2. Verify the DataFrame in the registry was updated
        assert dataframe_registry["test_df"] == mock_withcolumn_result
