"""Unit tests for the CastFunction class."""

from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame

from flint.etl.core.transform import TransformFunctionRegistry
from flint.etl.core.transforms.cast import CastFunction
from flint.etl.models.transforms.model_cast import (
    ARGUMENTS,
    CAST_TYPE,
    COLUMN_NAME,
    COLUMNS,
    FUNCTION,
    CastFunctionModel,
)
from flint.exceptions import FlintConfigurationKeyError


class TestCastFunction:
    """Unit tests for the CastFunction transform function."""

    @pytest.fixture
    def cast_model_single_column(self) -> CastFunctionModel:
        """Create a real CastFunctionModel with single column for testing."""
        column = CastFunctionModel.Column(column_name="age", cast_type="integer")
        args = CastFunctionModel.Args(columns=[column])
        return CastFunctionModel(function="cast", arguments=args)

    @pytest.fixture
    def cast_model_multiple_columns(self) -> CastFunctionModel:
        """Create a real CastFunctionModel with multiple columns for testing."""
        columns = [
            CastFunctionModel.Column(column_name="age", cast_type="integer"),
            CastFunctionModel.Column(column_name="price", cast_type="decimal(10,2)"),
            CastFunctionModel.Column(column_name="active", cast_type="boolean"),
        ]
        args = CastFunctionModel.Args(columns=columns)
        return CastFunctionModel(function="cast", arguments=args)

    def test_registration__cast_function__correctly_registered(self) -> None:
        """Test that CastFunction is registered correctly in the registry."""
        # Act
        registered_class = TransformFunctionRegistry.get("cast")

        # Assert
        assert registered_class == CastFunction

    def test_from_dict__single_column__creates_function_with_correct_model(self) -> None:
        """Test creating CastFunction from dict with single column configuration."""
        # Arrange
        config = {
            FUNCTION: "cast",
            ARGUMENTS: {COLUMNS: [{COLUMN_NAME: "age", CAST_TYPE: "integer"}]},
        }

        # Act
        function = CastFunction.from_dict(config)

        # Assert
        assert isinstance(function, CastFunction)
        assert function.model.function == "cast"
        assert len(function.model.arguments.columns) == 1
        assert function.model.arguments.columns[0].column_name == "age"
        assert function.model.arguments.columns[0].cast_type == "integer"

    def test_from_dict__multiple_columns__creates_function_with_all_columns(self) -> None:
        """Test creating CastFunction from dict with multiple column configurations."""
        # Arrange
        config = {
            FUNCTION: "cast",
            ARGUMENTS: {
                COLUMNS: [
                    {COLUMN_NAME: "age", CAST_TYPE: "integer"},
                    {COLUMN_NAME: "price", CAST_TYPE: "decimal(10,2)"},
                    {COLUMN_NAME: "active", CAST_TYPE: "boolean"},
                ]
            },
        }

        # Act
        function = CastFunction.from_dict(config)

        # Assert
        assert len(function.model.arguments.columns) == 3
        assert function.model.arguments.columns[0].column_name == "age"
        assert function.model.arguments.columns[0].cast_type == "integer"
        assert function.model.arguments.columns[1].column_name == "price"
        assert function.model.arguments.columns[1].cast_type == "decimal(10,2)"
        assert function.model.arguments.columns[2].column_name == "active"
        assert function.model.arguments.columns[2].cast_type == "boolean"

    def test_from_dict__empty_columns_list__creates_function_with_empty_list(self) -> None:
        """Test creating CastFunction from dict with empty columns list."""
        # Arrange
        config = {FUNCTION: "cast", ARGUMENTS: {COLUMNS: []}}

        # Act
        function = CastFunction.from_dict(config)

        # Assert
        assert len(function.model.arguments.columns) == 0

    def test_from_dict__missing_function_key__raises_configuration_error(self) -> None:
        """Test from_dict with missing function key raises FlintConfigurationKeyError."""
        # Arrange
        config = {ARGUMENTS: {COLUMNS: []}}

        # Act & Assert
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            CastFunction.from_dict(config)

        assert FUNCTION in str(exc_info.value)

    def test_from_dict__missing_arguments_key__raises_configuration_error(self) -> None:
        """Test from_dict with missing arguments key raises FlintConfigurationKeyError."""
        # Arrange
        config = {FUNCTION: "cast"}

        # Act & Assert
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            CastFunction.from_dict(config)

        assert ARGUMENTS in str(exc_info.value)

    def test_from_dict__missing_columns_key__raises_configuration_error(self) -> None:
        """Test from_dict with missing columns key raises FlintConfigurationKeyError."""
        # Arrange
        config = {FUNCTION: "cast", ARGUMENTS: {}}

        # Act & Assert
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            CastFunction.from_dict(config)

        assert COLUMNS in str(exc_info.value)

    def test_from_dict__missing_column_name__raises_configuration_error(self) -> None:
        """Test from_dict with missing column_name in column definition raises error."""
        # Arrange
        config = {
            FUNCTION: "cast",
            ARGUMENTS: {COLUMNS: [{CAST_TYPE: "integer"}]},
        }

        # Act & Assert
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            CastFunction.from_dict(config)

        assert COLUMN_NAME in str(exc_info.value)

    def test_from_dict__missing_cast_type__raises_configuration_error(self) -> None:
        """Test from_dict with missing cast_type in column definition raises error."""
        # Arrange
        config = {
            FUNCTION: "cast",
            ARGUMENTS: {COLUMNS: [{COLUMN_NAME: "age"}]},
        }

        # Act & Assert
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            CastFunction.from_dict(config)

        assert CAST_TYPE in str(exc_info.value)

    def test_transform__single_column__returns_callable_function(
        self, cast_model_single_column: CastFunctionModel
    ) -> None:
        """Test that transform method returns a callable function."""
        # Arrange
        function = CastFunction(model=cast_model_single_column)

        # Act
        transform_func = function.transform()

        # Assert
        assert callable(transform_func)

    def test_transform__execution__applies_cast_to_dataframe(self, cast_model_single_column: CastFunctionModel) -> None:
        """Test that transform function correctly applies cast operation to DataFrame."""
        # Arrange
        function = CastFunction(model=cast_model_single_column)
        transform_func = function.transform()

        mock_df = MagicMock(spec=DataFrame)
        mock_col = MagicMock()
        mock_cast_col = MagicMock()
        mock_result_df = MagicMock(spec=DataFrame)

        def mock_col_func(col_name: str) -> MagicMock:
            return mock_col

        # Mock justified: PySpark operations require Spark session and DataFrame setup
        with pytest.MonkeyPatch().context() as mp:
            mp.setattr("flint.etl.core.transforms.cast.col", mock_col_func)
            mock_col.cast.return_value = mock_cast_col
            mock_df.withColumn.return_value = mock_result_df

            # Act
            result = transform_func(mock_df)

            # Assert
            mock_col.cast.assert_called_once_with("integer")
            mock_df.withColumn.assert_called_once_with("age", mock_cast_col)
            assert result == mock_result_df

    def test_transform__multiple_columns__applies_all_casts_sequentially(
        self, cast_model_multiple_columns: CastFunctionModel
    ) -> None:
        """Test that transform function applies multiple casts in sequence."""
        # Arrange
        function = CastFunction(model=cast_model_multiple_columns)
        transform_func = function.transform()

        # Create DataFrames for chaining: df1 -> df2 -> df3
        mock_df1 = MagicMock(spec=DataFrame)
        mock_df2 = MagicMock(spec=DataFrame)
        mock_df3 = MagicMock(spec=DataFrame)
        
        # Set up chaining: each withColumn call returns the next DataFrame
        mock_df1.withColumn.return_value = mock_df2
        mock_df2.withColumn.return_value = mock_df3
        mock_df3.withColumn.return_value = mock_df3

        # Mock justified: PySpark operations require Spark session and DataFrame setup
        with pytest.MonkeyPatch().context() as mp:
            mock_col = MagicMock()
            mock_col.cast.return_value = MagicMock()
            mp.setattr("flint.etl.core.transforms.cast.col", mock_col)

            # Act
            result = transform_func(mock_df1)

            # Assert
            mock_df1.withColumn.assert_called_once()
            mock_df2.withColumn.assert_called_once()
            mock_df3.withColumn.assert_called_once()
            assert result == mock_df3

    def test_model_cls__attribute__returns_correct_model_class(self) -> None:
        """Test that model_cls attribute references the correct model class."""
        # Act & Assert
        assert CastFunction.model_cls == CastFunctionModel
