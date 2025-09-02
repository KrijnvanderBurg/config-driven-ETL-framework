"""Unit tests for the DropDuplicatesFunction class."""

from flint.etl.core.transform import TransformFunctionRegistry
from flint.etl.core.transforms.dropduplicates import DropDuplicatesFunction


class TestDropDuplicatesFunction:
    """Unit tests for the DropDuplicates transform function."""

    def test_registration(self) -> None:
        """Test that DropDuplicatesFunction is registered correctly."""
        assert TransformFunctionRegistry.get("dropDuplicates") == DropDuplicatesFunction

    def test_from_dict_with_columns(self) -> None:
        """Test creating DropDuplicatesFunction from dict with specific columns."""
        function_dict = {"function": "dropDuplicates", "arguments": {"columns": ["customer_id", "order_date"]}}

        function = DropDuplicatesFunction.from_dict(function_dict)

        assert function.model.function == "dropDuplicates"
        assert function.model.arguments.columns == ["customer_id", "order_date"]

    def test_from_dict_without_columns(self) -> None:
        """Test creating DropDuplicatesFunction from dict without columns."""
        function_dict = {"function": "dropDuplicates", "arguments": {}}

        function = DropDuplicatesFunction.from_dict(function_dict)

        assert function.model.function == "dropDuplicates"
        assert function.model.arguments.columns is None

    def test_transform_function_with_specific_columns(self) -> None:
        """Test transform function creation with specific columns."""
        function_dict = {"function": "dropDuplicates", "arguments": {"columns": ["customer_id", "order_date"]}}
        function = DropDuplicatesFunction.from_dict(function_dict)

        transform_func = function.transform()

        assert callable(transform_func)
        assert function.model.arguments.columns == ["customer_id", "order_date"]

    def test_transform_function_without_columns(self) -> None:
        """Test transform function creation without columns."""
        function_dict = {"function": "dropDuplicates", "arguments": {"columns": None}}
        function = DropDuplicatesFunction.from_dict(function_dict)

        transform_func = function.transform()

        assert callable(transform_func)
        assert function.model.arguments.columns is None

    def test_transform_function_with_empty_columns_list(self) -> None:
        """Test transform function creation with empty columns list."""
        function_dict = {"function": "dropDuplicates", "arguments": {"columns": []}}
        function = DropDuplicatesFunction.from_dict(function_dict)

        transform_func = function.transform()

        assert callable(transform_func)
        assert function.model.arguments.columns == []
