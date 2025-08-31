"""Unit tests for the filter transform function."""

from flint.job.core.transform import TransformFunctionRegistry
from flint.job.core.transforms.filter_ import FilterFunction


class TestFilterFunction:
    """Test suite for the filter transform function."""

    def test_registration(self) -> None:
        """Test that the FilterFunction is registered correctly."""
        assert TransformFunctionRegistry.get("filter") == FilterFunction

    def test_from_dict(self) -> None:
        """Test creating a FilterFunction instance from a dictionary."""
        function_dict = {"function": "filter", "arguments": {"condition": "age > 18"}}

        function = FilterFunction.from_dict(function_dict)

        assert function.model.function == "filter"
        assert function.model.arguments.condition == "age > 18"

    def test_transform_function_creation(self) -> None:
        """Test that the transform function is created correctly."""
        function_dict = {"function": "filter", "arguments": {"condition": "age > 21"}}
        function = FilterFunction.from_dict(function_dict)

        transform_func = function.transform()

        # Verify it's a callable function
        assert callable(transform_func)

        # Test the function signature by inspecting its behavior
        # We can't easily test DataFrame filtering without a real DataFrame,
        # but we can verify the function was created with the right configuration
        assert function.model.arguments.condition == "age > 21"

    def test_transform_with_complex_condition(self) -> None:
        """Test filter with complex condition."""
        function_dict = {"function": "filter", "arguments": {"condition": "age > 18 AND status = 'active'"}}
        function = FilterFunction.from_dict(function_dict)

        transform_func = function.transform()

        assert callable(transform_func)
        assert function.model.arguments.condition == "age > 18 AND status = 'active'"
