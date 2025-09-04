"""Unit tests for the DropFunction class."""

from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame

from flint.etl.core.transform import TransformFunctionRegistry
from flint.etl.core.transforms.drop import DropFunction
from flint.etl.models.transforms.model_drop import ARGUMENTS, COLUMNS, FUNCTION, DropFunctionModel
from flint.exceptions import FlintConfigurationKeyError


class TestDropFunction:
    """Unit tests for the DropFunction transform function."""

    @pytest.fixture
    def drop_model_single_column(self) -> DropFunctionModel:
        """Create a real DropFunctionModel with single column for testing."""
        args = DropFunctionModel.Args(columns=["temp_col"])
        return DropFunctionModel(function="drop", arguments=args)

    @pytest.fixture
    def drop_model_multiple_columns(self) -> DropFunctionModel:
        """Create a real DropFunctionModel with multiple columns for testing."""
        args = DropFunctionModel.Args(columns=["temp_col", "unused_field", "calc_intermediate"])
        return DropFunctionModel(function="drop", arguments=args)

    @pytest.fixture
    def drop_model_empty_columns(self) -> DropFunctionModel:
        """Create a real DropFunctionModel with empty columns list for testing."""
        args = DropFunctionModel.Args(columns=[])
        return DropFunctionModel(function="drop", arguments=args)

    def test_registration__drop_function__correctly_registered(self) -> None:
        """Test that DropFunction is registered correctly in the registry."""
        # Act
        registered_class = TransformFunctionRegistry.get("drop")

        # Assert
        assert registered_class == DropFunction

    def test_from_dict__valid_config__creates_function_with_correct_model(self) -> None:
        """Test creating DropFunction from valid configuration dictionary."""
        # Arrange
        config = {FUNCTION: "drop", ARGUMENTS: {COLUMNS: ["temp_col", "unused_field"]}}

        # Act
        function = DropFunction.from_dict(config)

        # Assert
        assert isinstance(function, DropFunction)
        assert function.model.function == "drop"
        assert function.model.arguments.columns == ["temp_col", "unused_field"]

    def test_from_dict__empty_columns__creates_function_with_empty_list(self) -> None:
        """Test creating DropFunction with empty columns list."""
        # Arrange
        config = {FUNCTION: "drop", ARGUMENTS: {COLUMNS: []}}

        # Act
        function = DropFunction.from_dict(config)

        # Assert
        assert function.model.arguments.columns == []

    def test_from_dict__single_column__creates_function_with_single_column(self) -> None:
        """Test creating DropFunction with single column configuration."""
        # Arrange
        config = {FUNCTION: "drop", ARGUMENTS: {COLUMNS: ["single_col"]}}

        # Act
        function = DropFunction.from_dict(config)

        # Assert
        assert function.model.arguments.columns == ["single_col"]

    def test_from_dict__missing_function_key__raises_configuration_error(self) -> None:
        """Test from_dict with missing function key raises FlintConfigurationKeyError."""
        # Arrange
        config = {ARGUMENTS: {COLUMNS: ["col1"]}}

        # Act & Assert
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            DropFunction.from_dict(config)

        assert FUNCTION in str(exc_info.value)

    def test_from_dict__missing_arguments_key__raises_configuration_error(self) -> None:
        """Test from_dict with missing arguments key raises FlintConfigurationKeyError."""
        # Arrange
        config = {FUNCTION: "drop"}

        # Act & Assert
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            DropFunction.from_dict(config)

        assert ARGUMENTS in str(exc_info.value)

    def test_from_dict__missing_columns_key__raises_configuration_error(self) -> None:
        """Test from_dict with missing columns key raises FlintConfigurationKeyError."""
        # Arrange
        config = {FUNCTION: "drop", ARGUMENTS: {}}

        # Act & Assert
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            DropFunction.from_dict(config)

        assert COLUMNS in str(exc_info.value)

    def test_transform__single_column__returns_callable_function(
        self, drop_model_single_column: DropFunctionModel
    ) -> None:
        """Test that transform method returns a callable function."""
        # Arrange
        function = DropFunction(model=drop_model_single_column)

        # Act
        transform_func = function.transform()

        # Assert
        assert callable(transform_func)

    def test_transform__execution__applies_drop_to_dataframe(self, drop_model_single_column: DropFunctionModel) -> None:
        """Test that transform function correctly applies drop operation to DataFrame."""
        # Arrange
        function = DropFunction(model=drop_model_single_column)
        transform_func = function.transform()

        mock_df = MagicMock(spec=DataFrame)
        mock_result_df = MagicMock(spec=DataFrame)
        mock_df.drop.return_value = mock_result_df

        # Act
        result = transform_func(mock_df)

        # Assert
        mock_df.drop.assert_called_once_with("temp_col")
        assert result == mock_result_df

    def test_transform__multiple_columns__applies_drop_with_all_columns(
        self, drop_model_multiple_columns: DropFunctionModel
    ) -> None:
        """Test that transform function correctly handles multiple columns."""
        # Arrange
        function = DropFunction(model=drop_model_multiple_columns)
        transform_func = function.transform()

        mock_df = MagicMock(spec=DataFrame)
        mock_result_df = MagicMock(spec=DataFrame)
        mock_df.drop.return_value = mock_result_df

        # Act
        result = transform_func(mock_df)

        # Assert
        mock_df.drop.assert_called_once_with("temp_col", "unused_field", "calc_intermediate")
        assert result == mock_result_df

    def test_transform__empty_columns__applies_drop_with_no_arguments(
        self, drop_model_empty_columns: DropFunctionModel
    ) -> None:
        """Test that transform function handles empty columns list correctly."""
        # Arrange
        function = DropFunction(model=drop_model_empty_columns)
        transform_func = function.transform()

        mock_df = MagicMock(spec=DataFrame)
        mock_result_df = MagicMock(spec=DataFrame)
        mock_df.drop.return_value = mock_result_df

        # Act
        result = transform_func(mock_df)

        # Assert
        mock_df.drop.assert_called_once_with()  # No arguments passed
        assert result == mock_result_df

    def test_model_cls__attribute__returns_correct_model_class(self) -> None:
        """Test that model_cls attribute references the correct model class."""
        # Act & Assert
        assert DropFunction.model_cls == DropFunctionModel
