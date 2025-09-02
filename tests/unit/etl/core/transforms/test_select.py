"""Unit tests for the SelectFunction transform."""

from flint.etl.core.transform import TransformFunctionRegistry
from flint.etl.core.transforms.select import SelectFunction


class TestSelectFunction:
    """Unit tests for the SelectFunction class."""

    def test_registration(self) -> None:
        """Test that SelectFunction is registered correctly."""
        assert TransformFunctionRegistry.get("select") == SelectFunction

    def test_from_dict(self) -> None:
        """Test creating SelectFunction from configuration dict."""
        function_dict = {"function": "select", "arguments": {"columns": ["col1", "col2"]}}

        function = SelectFunction.from_dict(function_dict)

        assert function.model.function == "select"
        assert function.model.arguments.columns == ["col1", "col2"]

    def test_transform_function_creation(self) -> None:
        """Test that the transform function is created correctly."""
        function_dict = {"function": "select", "arguments": {"columns": ["name", "age"]}}
        function = SelectFunction.from_dict(function_dict)

        transform_func = function.transform()

        # Verify it's a callable function
        assert callable(transform_func)

        # Verify the configuration is stored correctly
        assert function.model.arguments.columns == ["name", "age"]

    def test_transform_single_column(self) -> None:
        """Test selecting a single column."""
        function_dict = {"function": "select", "arguments": {"columns": ["id"]}}
        function = SelectFunction.from_dict(function_dict)

        transform_func = function.transform()

        assert callable(transform_func)
        assert function.model.arguments.columns == ["id"]
