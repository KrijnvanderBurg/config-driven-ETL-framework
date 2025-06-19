"""Unit tests for the filter transform function."""

from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession

from flint.core.transform import TransformFunctionRegistry
from flint.core.transforms.filter_ import FilterFunction


class TestFilterFunction:
    """Test suite for the filter transform function."""

    def test_registration(self) -> None:
        """Test that the FilterFunction is registered correctly."""
        assert TransformFunctionRegistry.get("filter") == FilterFunction

    def test_initialization(self) -> None:
        """Test that FilterFunction is initialized correctly from a model."""
        # Setup
        model = MagicMock()
        model.arguments.condition = "age > 18"

        # Execute
        function = FilterFunction(model=model)

        # Verify
        assert function.model == model
        assert callable(function.callable_)

    def test_from_dict(self) -> None:
        """Test creating a FilterFunction instance from a dictionary."""
        # Setup
        dict_ = {"function": "filter", "arguments": {"condition": "age > 18"}}

        # Execute
        with patch("flint.models.transforms.model_filter.FilterFunctionModel.from_dict") as mock_from_dict:
            mock_from_dict.return_value.arguments.condition = "age > 18"
            function = FilterFunction.from_dict(dict_=dict_)

        # Verify
        assert function.model == mock_from_dict.return_value

    def test_transform_function(self) -> None:
        """Test that the transform method returns a callable function."""
        # Setup
        model = MagicMock()
        model.arguments.condition = "age > 18"
        function = FilterFunction(model=model)

        # Execute
        callable_ = function.transform()

        # Verify
        assert callable(callable_)

    def test_transform_integration(self) -> None:
        """Test that the transform function filters rows correctly."""
        spark = SparkSession.Builder().getOrCreate()
        data = [("John", 25), ("Jane", 15), ("Bob", 42), ("Alice", 17)]
        df = spark.createDataFrame(data, ["name", "age"])

        # Create model with filter condition
        model = MagicMock()
        model.arguments.condition = df["age"] > 18

        filter_function = FilterFunction(model=model)

        # Execute
        result_df = filter_function.callable_(df=df)

        # Verify
        result_rows = result_df.collect()
        assert len(result_rows) == 2
        assert result_rows[0]["name"] == "John"
        assert result_rows[0]["age"] == 25
        assert result_rows[1]["name"] == "Bob"
        assert result_rows[1]["age"] == 42
