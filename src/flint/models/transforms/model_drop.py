"""Configuration model for the drop transform function.

This module defines the data models used to configure drop
transformations in the ingestion framework. It includes:

- DropFunctionModel: Main configuration model for drop operations
- Args nested class: Container for the drop parameters

These models provide a type-safe interface for configuring column removal
from configuration files or dictionaries.
"""

from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import ConfigurationKeyError
from flint.models.model_transform import ARGUMENTS, FUNCTION, FunctionModel

COLUMNS: Final[str] = "columns"


@dataclass
class DropFunctionModel(FunctionModel):
    """Configuration model for drop transform operations.

    This model defines the structure for configuring a drop
    transformation, specifying which columns to remove from the DataFrame.

    Attributes:
        function: The name of the function to be used (always "drop")
        arguments: Container for the drop parameters
    """

    function: str
    arguments: "DropFunctionModel.Args"

    @dataclass
    class Args:
        """Arguments for drop transform operations.

        Attributes:
            columns: List of column names to drop from the DataFrame
        """

        columns: list[str]

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """
        Create a DropFunctionModel from a dictionary.

        Args:
            dict_: The configuration dictionary.

        Returns:
            An initialized DropFunctionModel.

        Raises:
            ConfigurationKeyError: If required keys are missing from the dictionary
        """
        try:
            function_name = dict_[FUNCTION]
            arguments_dict = dict_[ARGUMENTS]
            columns = arguments_dict[COLUMNS]
            arguments = cls.Args(columns=columns)
            return cls(function=function_name, arguments=arguments)
        except KeyError as e:
            key = e.args[0]
            if key in (FUNCTION, ARGUMENTS):
                raise ConfigurationKeyError(key=key, dict_=dict_) from e
            # Must be missing COLUMNS from arguments_dict
            raise ConfigurationKeyError(key=key, dict_=dict_[ARGUMENTS]) from e
