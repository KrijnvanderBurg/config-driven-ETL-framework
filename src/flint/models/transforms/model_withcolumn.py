"""Configuration model for the withColumn transform function.

This module defines the data models used to configure withColumn
transformations in the ingestion framework. It includes:

- WithColumnFunctionModel: Main configuration model for withColumn operations
- Args nested class: Container for the withColumn parameters

These models provide a type-safe interface for configuring column addition
or replacement from configuration files or dictionaries.
"""

from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import DictKeyError

# Import these locally to avoid circular imports
from flint.models.model_transform import ARGUMENTS, FUNCTION, FunctionModel

COL_NAME: Final[str] = "col_name"
COL_EXPR: Final[str] = "col_expr"


@dataclass
class WithColumnFunctionModel(FunctionModel):
    """Configuration model for withColumn transform operations.

    This model defines the structure for configuring a withColumn
    transformation, specifying the column name and expression.

    Attributes:
        function: The name of the function to be used (always "withColumn")
        arguments: Container for the withColumn parameters
    """

    function: str
    arguments: "WithColumnFunctionModel.Args"

    @dataclass
    class Args:
        """Arguments for withColumn transform operations.

        Attributes:
            col_name: Name of the column to add or replace
            col_expr: Column expression representing the value
        """

        col_name: str
        col_expr: str

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """
        Create a WithColumnFunctionModel from a dictionary.

        Args:
            dict_: The configuration dictionary.

        Returns:
            An initialized WithColumnFunctionModel.
        """
        try:
            function_name = dict_[FUNCTION]
            arguments_dict = dict_[ARGUMENTS]

            col_name = arguments_dict[COL_NAME]
            col_expr = arguments_dict[COL_EXPR]
            arguments = cls.Args(col_name=col_name, col_expr=col_expr)

        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=dict_) from e

        return cls(function=function_name, arguments=arguments)
