"""Unit tests for the CustomersOrdersBronzeFunction class."""

from flint.job.core.transform import TransformFunctionRegistry
from flint.job.core.transforms.custom.customer_orders import CustomersOrdersBronzeFunction
from flint.types import DataFrameRegistry


class TestCustomersOrdersBronzeFunction:
    """Unit tests for the CustomersOrdersBronze custom transform function."""

    def test_registration(self) -> None:
        """Test that CustomersOrdersBronzeFunction is registered correctly."""
        assert TransformFunctionRegistry.get("customers_orders") == CustomersOrdersBronzeFunction

    def test_from_dict(self) -> None:
        """Test creating CustomersOrdersBronzeFunction from configuration dict."""
        function_dict = {"function": "customers_orders", "arguments": {"amount_minimum": 100}}

        function = CustomersOrdersBronzeFunction.from_dict(function_dict)

        assert function.model.function == "customers_orders"
        assert function.model.arguments.amount_minimum == 100

    def test_transform_function_creation(self) -> None:
        """Test that transform function is created correctly."""
        function_dict = {"function": "customers_orders", "arguments": {"amount_minimum": 250}}
        function = CustomersOrdersBronzeFunction.from_dict(function_dict)

        # Set up registry with orders DataFrame
        registry = DataFrameRegistry()
        registry["extract-orders"] = "mock_orders_df"  # Simple string as mock
        function.data_registry = registry

        transform_func = function.transform()

        assert callable(transform_func)
        assert function.model.arguments.amount_minimum == 250

    def test_transform_function_with_different_amount_minimum(self) -> None:
        """Test transform function with different amount minimum."""
        function_dict = {"function": "customers_orders", "arguments": {"amount_minimum": 500}}
        function = CustomersOrdersBronzeFunction.from_dict(function_dict)

        # Set up registry
        registry = DataFrameRegistry()
        registry["extract-orders"] = "mock_orders_df"  # Simple string as mock
        function.data_registry = registry

        transform_func = function.transform()

        assert callable(transform_func)
        assert function.model.arguments.amount_minimum == 500

    def test_configuration_validation(self) -> None:
        """Test that configuration is validated correctly."""
        function_dict = {"function": "customers_orders", "arguments": {"amount_minimum": 1000}}
        function = CustomersOrdersBronzeFunction.from_dict(function_dict)

        assert function.model.function == "customers_orders"
        assert function.model.arguments.amount_minimum == 1000

    def test_registry_requirement(self) -> None:
        """Test that registry is required for the transform function."""
        function_dict = {"function": "customers_orders", "arguments": {"amount_minimum": 100}}
        function = CustomersOrdersBronzeFunction.from_dict(function_dict)

        # Test that data_registry attribute exists and can be set
        registry = DataFrameRegistry()
        function.data_registry = registry

        assert function.data_registry is registry
