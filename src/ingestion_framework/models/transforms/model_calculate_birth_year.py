"""
Calculate birth year transform function model.
"""

from dataclasses import dataclass
from typing import Any, Final, Self

from ingestion_framework.exceptions import DictKeyError

# Import these locally to avoid circular imports
from ingestion_framework.models.model_transform import ARGUMENTS, FUNCTION, FunctionModel

CURRENT_YEAR: Final[str] = "current_year"
AGE_COLUMN: Final[str] = "age_column"
BIRTH_YEAR_COLUMN: Final[str] = "birth_year_column"


@dataclass
class CalculateBirthYearFunctionModel(FunctionModel):
    """A concrete implementation of calculate birth year function."""

    function: str
    arguments: "CalculateBirthYearFunctionModel.Args"

    @dataclass
    class Args:
        """Arguments for calculate birth year function."""

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
            raise DictKeyError(key=e.args[0], dict_=dict_) from e

        return cls(function=function_name, arguments=arguments)
