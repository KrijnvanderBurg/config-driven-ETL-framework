"""Configuration model for the join transform function.

This module defines the data models used to configure join
transformations in the ingestion framework. It includes:

- JoinFunctionModel: Main configuration model for join operations
- Args nested class: Container for the join parameters

These models provide a type-safe interface for configuring joins
from configuration files or dictionaries.
"""

from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import DictKeyError

# Import these locally to avoid circular imports
from flint.models.model_transform import ARGUMENTS, FUNCTION, FunctionModel

# Constants for keys in the configuration dictionary
OTHER_UPSTREAM_NAME: Final[str] = "other_upstream_name"
ON: Final[str] = "on"
HOW: Final[str] = "how"


@dataclass
class JoinFunctionModel(FunctionModel):
    """Configuration model for join transform operations.

    This model defines the structure for configuring a join
    transformation, specifying the dataframes to join and how to join them.

    Attributes:
        function: The name of the function to be used (always "join")
        arguments: Container for the join parameters
    """

    function: str
    arguments: "JoinFunctionModel.Args"

    @dataclass
    class Args:
        """Arguments for join transform operations.

        Attributes:
            other_upstream_name: Name of the dataframe to join with the current dataframe
            on: Column(s) to join on. Can be a string for a single column or a list of strings for multiple columns
            how: Type of join to perform (inner, outer, left, right, etc.). Defaults to "inner"
        """

        other_upstream_name: str
        on: str | list[str]
        how: str = "inner"

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a JoinFunctionModel instance from a dictionary.

        Args:
            dict_: The configuration dictionary.

        Returns:
            An initialized JoinFunctionModel instance

        Raises:
            DictKeyError: If required keys are missing or if values are invalid
            ValueError: If the function name is not 'join'
        """
        try:
            function_name = dict_[FUNCTION]
            arguments_dict: dict = dict_[ARGUMENTS]

            other_upstream_name = arguments_dict[OTHER_UPSTREAM_NAME]
            on = arguments_dict[ON]
            how = arguments_dict[HOW]

            arguments = cls.Args(
                other_upstream_name=other_upstream_name,
                on=on,
                how=how,
            )

        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=dict_) from e

        return cls(function=function_name, arguments=arguments)
