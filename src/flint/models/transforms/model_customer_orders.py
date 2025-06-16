"""Configuration model for the column selection transform function.

This module defines the data models used to configure column selection
transformations in the ingestion framework. It includes:

- CustomersOrdersFunctionModel: Main configuration model for select operations
- Args nested class: Container for the selection parameters

These models provide a type-safe interface for configuring column selections
from configuration files or dictionaries.
"""

from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import DictKeyError

# Import these locally to avoid circular imports
from flint.models.model_transform import ARGUMENTS, FUNCTION, FunctionModel

amount_minimum: Final[str] = "amount_minimum"


@dataclass
class CustomersOrdersFunctionModel(FunctionModel):
    """Configuration model for column selection transform operations.

    This model defines the structure for configuring a column selection
    transformation, specifying which columns should be included in the output.

    Attributes:
        function: The name of the function to be used (always "select")
        arguments: Container for the column selection parameters
    """

    function: str
    arguments: "CustomersOrdersFunctionModel.Args"

    @dataclass
    class Args:
        """Arguments for column selection transform operations.

        Attributes:
            columns: List of column names to select from the DataFrame
        """

        amount_minimum: int

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """
        Create a CustomersOrdersFunctionModel from a dictionary.

        Args:
            dict_: The configuration dictionary.

        Returns:
            An initialized CustomersOrdersFunctionModel.
        """
        try:
            function_name = dict_[FUNCTION]
            arguments_dict = dict_[ARGUMENTS]

            # Process the arguments
            amount_minimum = arguments_dict[amount_minimum]
            arguments = cls.Args(amount_minimum=amount_minimum)

        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=dict_) from e

        return cls(function=function_name, arguments=arguments)
