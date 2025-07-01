"""Configuration model for the dropDuplicates transform function.

This module defines the data models used to configure dropDuplicates
transformations in the ingestion framework. It includes:

- DropDuplicatesFunctionModel: Main configuration model for dropDuplicates operations
- Args nested class: Container for the dropDuplicates parameters

These models provide a type-safe interface for configuring duplicate row removal
from configuration files or dictionaries.
"""

from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import ConfigurationKeyError
from flint.models.model_transform import ARGUMENTS, FUNCTION, FunctionModel

COLUMNS: Final[str] = "columns"


@dataclass
class DropDuplicatesFunctionModel(FunctionModel):
    """Configuration model for dropDuplicates transform operations.

    This model defines the structure for configuring a dropDuplicates
    transformation, specifying which columns to consider when identifying duplicates.

    Attributes:
        function: The name of the function to be used (always "dropDuplicates")
        arguments: Container for the dropDuplicates parameters
    """

    function: str
    arguments: "DropDuplicatesFunctionModel.Args"

    @dataclass
    class Args:
        """Arguments for dropDuplicates transform operations.

        Attributes:
            columns: Optional list of column names to consider when dropping duplicates.
                    If None, all columns are considered.
        """

        columns: list[str] | None = None

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """
        Create a DropDuplicatesFunctionModel from a dictionary.

        Args:
            dict_: The configuration dictionary.

        Returns:
            An initialized DropDuplicatesFunctionModel.

        Raises:
            ConfigurationKeyError: If required keys are missing from the dictionary
        """
        try:
            function_name = dict_[FUNCTION]
            arguments_dict = dict_[ARGUMENTS]

            # Columns is optional for dropDuplicates
            columns = arguments_dict.get(COLUMNS)
            arguments = cls.Args(columns=columns)

        except KeyError as e:
            raise ConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(function=function_name, arguments=arguments)
