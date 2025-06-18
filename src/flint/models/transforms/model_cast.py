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
COLUMN_NAME: Final[str] = "column_name"
CAST_TYPE: Final[str] = "cast_type"


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
    class Column:
        """Single column casting definition.

        Attributes:
            column_name: Name of the column to cast
            cast_type: Target data type to cast the column to
        """

        column_name: str
        cast_type: str

    @dataclass
    class Args:
        """Arguments for column casting transform operations.

        Attributes:
            columns: List of column casting definitions
        """

        columns: list["CastFunctionModel.Column"]

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
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=dict_) from e

        try:
            columns_data = arguments_dict[COLUMNS]
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=arguments_dict) from e

        # Process list of column objects
        columns = []
        for column_data in columns_data:
            try:
                columns.append(cls.Column(column_name=column_data[COLUMN_NAME], cast_type=column_data[CAST_TYPE]))
            except KeyError as e:
                raise DictKeyError(key=e.args[0], dict_=column_data) from e

        arguments = cls.Args(columns=columns)

        return cls(function=function_name, arguments=arguments)
