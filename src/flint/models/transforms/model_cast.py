"""Configuration model for the column casting transform function.

This module defines the data models used to configure column type casting
transformations in the ingestion framework. It includes:

- CastFunctionModel: Main configuration model for cast operations
- Args nested class: Container for the casting parameters

These models provide a type-safe interface for configuring column type casting
from configuration files or dictionaries.
"""

from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import DictKeyError

# Import these locally to avoid circular imports
from flint.models.model_transform import ARGUMENTS, FUNCTION, FunctionModel

COLUMNS: Final[str] = "columns"
TYPES: Final[str] = "types"


@dataclass
class CastFunctionModel(FunctionModel):
    """Configuration model for column casting transform operations.

    This model defines the structure for configuring a column type casting
    transformation, specifying which columns should be cast to which data types.

    Attributes:
        function: The name of the function to be used (always "cast")
        arguments: Container for the column casting parameters
    """

    function: str
    arguments: "CastFunctionModel.Args"

    @dataclass
    class Args:
        """Arguments for column casting transform operations.

        Attributes:
            columns: Dictionary mapping column names to target data types
        """

        columns: dict[str, str]

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Convert a dictionary to a CastFunctionModel instance.

        This method parses a configuration dictionary and creates a typed
        model instance for the cast transform function.

        Args:
            dict_: Dictionary containing function and arguments keys

        Returns:
            A new CastFunctionModel instance with the specified configuration

        Raises:
            DictKeyError: If required keys are missing from the dictionary
        """
        try:
            function_name = dict_[FUNCTION]
            arguments_dict = dict_[ARGUMENTS]
            columns = arguments_dict[COLUMNS]
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=dict_) from e

        arguments = cls.Args(columns=columns)

        return cls(function=function_name, arguments=arguments)
