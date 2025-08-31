"""Unit tests for the JoinFunction class."""

from flint.job.core.transform import TransformFunctionRegistry
from flint.job.core.transforms.join import JoinFunction
from flint.types import DataFrameRegistry


class TestJoinFunction:
    """Unit tests for the Join transform function."""

    def test_registration(self) -> None:
        """Test that JoinFunction is registered correctly."""
        assert TransformFunctionRegistry.get("join") == JoinFunction

    def test_from_dict_single_column(self) -> None:
        """Test creating JoinFunction from dict with single join column."""
        function_dict = {
            "function": "join",
            "arguments": {"other_upstream_name": "extract-orders", "on": "customer_id", "how": "inner"},
        }

        function = JoinFunction.from_dict(function_dict)

        assert function.model.function == "join"
        assert function.model.arguments.other_upstream_name == "extract-orders"
        assert function.model.arguments.on == "customer_id"
        assert function.model.arguments.how == "inner"

    def test_from_dict_multi_column(self) -> None:
        """Test creating JoinFunction from dict with multiple join columns."""
        function_dict = {
            "function": "join",
            "arguments": {"other_upstream_name": "extract-orders", "on": ["customer_id", "store_id"], "how": "left"},
        }

        function = JoinFunction.from_dict(function_dict)

        assert function.model.arguments.on == ["customer_id", "store_id"]
        assert function.model.arguments.how == "left"

    def test_transform_function_single_column_join(self) -> None:
        """Test transform function creation with single column join."""
        function_dict = {
            "function": "join",
            "arguments": {"other_upstream_name": "extract-orders", "on": "customer_id", "how": "inner"},
        }
        function = JoinFunction.from_dict(function_dict)

        # Set up registry with mock DataFrame
        registry = DataFrameRegistry()
        registry["extract-orders"] = "mock_orders_df"  # Simple string as mock
        function.data_registry = registry

        transform_func = function.transform()

        assert callable(transform_func)
        assert function.model.arguments.on == "customer_id"
        assert function.model.arguments.how == "inner"
        assert function.model.arguments.other_upstream_name == "extract-orders"

    def test_transform_function_multi_column_join(self) -> None:
        """Test transform function creation with multiple column join."""
        function_dict = {
            "function": "join",
            "arguments": {"other_upstream_name": "extract-orders", "on": ["customer_id", "store_id"], "how": "left"},
        }
        function = JoinFunction.from_dict(function_dict)

        # Set up registry
        registry = DataFrameRegistry()
        registry["extract-orders"] = "mock_orders_df"  # Simple string as mock
        function.data_registry = registry

        transform_func = function.transform()

        assert callable(transform_func)
        assert function.model.arguments.on == ["customer_id", "store_id"]
        assert function.model.arguments.how == "left"
