"""Configuration model for the filter/where transform function.

This module defines the data models used to configure filter/where
transformations in the ingestion framework. It includes:

- FilterFunctionModel: Main configuration model for filter operations
- Args nested class: Container for the filtering parameters

These models provide a type-safe interface for configuring filter operations
from configuration files or dictionaries.
"""

from dataclasses import dataclass
from typing import Any, Final, Self

from pyspark.sql.column import Column

from flint.etl.models.model_transform import ARGUMENTS, FUNCTION, FunctionModel
from flint.exceptions import FlintConfigurationKeyError

CONDITION: Final[str] = "condition"


@dataclass
class FilterFunctionModel(FunctionModel):
    """Configuration model for filter/where transform operations.

    This model defines the structure for configuring a filter/where
    transformation, specifying the condition to filter rows.

    Attributes:
        function: The name of the function to be used (always "filter")
        arguments: Container for the filter parameters
    """

    function: str
    arguments: "FilterFunctionModel.Args"

    @dataclass
    class Args:
        """Arguments for filter transform operations.

        Attributes:
            condition: Column expression representing the filter condition
        """

        condition: Column

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """
        Create a FilterFunctionModel from a dictionary.

        Args:
            dict_: The configuration dictionary.

        Returns:
            An initialized FilterFunctionModel.
        """
        try:
            function_name = dict_[FUNCTION]
            arguments_dict = dict_[ARGUMENTS]

            condition = arguments_dict[CONDITION]
            arguments = cls.Args(condition=condition)

        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(function=function_name, arguments=arguments)
