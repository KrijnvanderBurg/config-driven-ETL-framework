"""Unit tests for the CalculateBirthYearFunction class."""

from flint.job.core.transform import TransformFunctionRegistry
from flint.job.core.transforms.custom.calculate_birth_year import CalculateBirthYearFunction


class TestCalculateBirthYearFunction:
    """Unit tests for the CalculateBirthYear custom transform function."""

    def test_registration(self) -> None:
        """Test that CalculateBirthYearFunction is registered correctly."""
        assert TransformFunctionRegistry.get("calculate_birth_year") == CalculateBirthYearFunction

    def test_from_dict(self) -> None:
        """Test creating CalculateBirthYearFunction from configuration dict."""
        function_dict = {
            "function": "calculate_birth_year",
            "arguments": {"current_year": 2025, "age_column": "age", "birth_year_column": "birth_year"},
        }

        function = CalculateBirthYearFunction.from_dict(function_dict)

        assert function.model.function == "calculate_birth_year"
        assert function.model.arguments.current_year == 2025
        assert function.model.arguments.age_column == "age"
        assert function.model.arguments.birth_year_column == "birth_year"

    def test_transform_function_creation(self) -> None:
        """Test that transform function is created correctly."""
        function_dict = {
            "function": "calculate_birth_year",
            "arguments": {"current_year": 2025, "age_column": "age", "birth_year_column": "birth_year"},
        }
        function = CalculateBirthYearFunction.from_dict(function_dict)

        transform_func = function.transform()

        assert callable(transform_func)
        assert function.model.arguments.current_year == 2025
        assert function.model.arguments.age_column == "age"
        assert function.model.arguments.birth_year_column == "birth_year"

    def test_transform_function_with_different_columns(self) -> None:
        """Test transform function with different column names."""
        function_dict = {
            "function": "calculate_birth_year",
            "arguments": {
                "current_year": 2024,
                "age_column": "person_age",
                "birth_year_column": "calculated_birth_year",
            },
        }
        function = CalculateBirthYearFunction.from_dict(function_dict)

        transform_func = function.transform()

        assert callable(transform_func)
        assert function.model.arguments.current_year == 2024
        assert function.model.arguments.age_column == "person_age"
        assert function.model.arguments.birth_year_column == "calculated_birth_year"
