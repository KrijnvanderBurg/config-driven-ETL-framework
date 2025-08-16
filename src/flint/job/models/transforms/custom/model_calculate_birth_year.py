"""Configuration model for the birth year calculation transform function.

This module defines the data models used to configure birth year calculation
transformations in the ingestion framework. It includes:

- CalculateBirthYearFunctionModel: Main configuration model for birth year calculations
- Args nested class: Container for the calculation parameters

These models provide a type-safe interface for configuring birth year calculations
from configuration files or dictionaries.
"""

from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import FlintConfigurationKeyError
from flint.job.models.model_transform import ARGUMENTS, FUNCTION, FunctionModel

CURRENT_YEAR: Final[str] = "current_year"
AGE_COLUMN: Final[str] = "age_column"
BIRTH_YEAR_COLUMN: Final[str] = "birth_year_column"


@dataclass
class CalculateBirthYearFunctionModel(FunctionModel):
    """Configuration model for birth year calculation transform operations.

    This model defines the structure for configuring a birth year calculation
    transformation, specifying which columns to use, the current year, and
    where to store the results.

    Attributes:
        function: The name of the function to be used (always "calculate_birth_year")
        arguments: Container for the calculation parameters
    """

    function: str
    arguments: "CalculateBirthYearFunctionModel.Args"

    @dataclass
    class Args:
        """Arguments for birth year calculation transform operations.

        Attributes:
            current_year: The reference year to use for the calculation
            age_column: The source column containing the person's age
            birth_year_column: The target column to store the calculated birth year
        """

        current_year: int
        age_column: str
        birth_year_column: str

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """
        Create a CalculateBirthYearFunctionModel from a dictionary.

        Args:
            dict_: The configuration dictionary.

        Returns:
            An initialized CalculateBirthYearFunctionModel.
        """
        try:
            function_name = dict_[FUNCTION]
            arguments_dict = dict_[ARGUMENTS]

            # Process the arguments with default values
            current_year = arguments_dict.get(CURRENT_YEAR, 2025)
            age_column = arguments_dict.get(AGE_COLUMN, "age")
            birth_year_column = arguments_dict.get(BIRTH_YEAR_COLUMN, "birth_year")

            arguments = cls.Args(current_year=current_year, age_column=age_column, birth_year_column=birth_year_column)

        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(function=function_name, arguments=arguments)
