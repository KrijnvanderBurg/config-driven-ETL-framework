"""Unit tests for the WithColumnFunction class."""

from flint.job.core.transform import TransformFunctionRegistry
from flint.job.core.transforms.withcolumn import WithColumnFunction


class TestWithColumnFunction:
    """Unit tests for the WithColumn transform function."""

    def test_registration(self) -> None:
        """Test that WithColumnFunction is registered correctly."""
        assert TransformFunctionRegistry.get("withColumn") == WithColumnFunction

    def test_from_dict(self) -> None:
        """Test creating WithColumnFunction from configuration dict."""
        function_dict = {
            "function": "withColumn",
            "arguments": {"col_name": "full_name", "col_expr": "concat(first_name, ' ', last_name)"},
        }

        function = WithColumnFunction.from_dict(function_dict)

        assert function.model.function == "withColumn"
        assert function.model.arguments.col_name == "full_name"
        assert function.model.arguments.col_expr == "concat(first_name, ' ', last_name)"

    def test_transform_function_creation(self) -> None:
        """Test that transform function is created correctly."""
        function_dict = {
            "function": "withColumn",
            "arguments": {"col_name": "full_name", "col_expr": "concat(first_name, ' ', last_name)"},
        }
        function = WithColumnFunction.from_dict(function_dict)

        transform_func = function.transform()

        assert callable(transform_func)
        assert function.model.arguments.col_name == "full_name"
        assert function.model.arguments.col_expr == "concat(first_name, ' ', last_name)"

    def test_transform_with_different_column_name(self) -> None:
        """Test with different column name and expression."""
        function_dict = {"function": "withColumn", "arguments": {"col_name": "name", "col_expr": "upper(name)"}}
        function = WithColumnFunction.from_dict(function_dict)

        transform_func = function.transform()

        assert callable(transform_func)
        assert function.model.arguments.col_name == "name"
        assert function.model.arguments.col_expr == "upper(name)"

    def test_transform_with_arithmetic_expression(self) -> None:
        """Test with arithmetic expression."""
        function_dict = {"function": "withColumn", "arguments": {"col_name": "age_plus_one", "col_expr": "age + 1"}}
        function = WithColumnFunction.from_dict(function_dict)

        transform_func = function.transform()

        assert callable(transform_func)
        assert function.model.arguments.col_name == "age_plus_one"
        assert function.model.arguments.col_expr == "age + 1"
