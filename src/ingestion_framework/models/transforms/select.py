"""
Column transform function.
"""

from dataclasses import dataclass
from typing import Any, Final, Self

from pyspark.sql import functions as f
from pyspark.sql.column import Column

from ingestion_framework.exceptions import DictKeyError

# Import these locally to avoid circular imports
from ingestion_framework.models.transform import ARGUMENTS, FUNCTION, FunctionModel

COLUMNS: Final[str] = "columns"


@dataclass
class SelectFunctionModel(FunctionModel):
    """A concrete implementation of Select functions."""

    function: str
    arguments: "SelectFunctionModel.Args"

    @dataclass
    class Args:
        """Arguments for Select functions."""

        columns: list[Column]

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """
        Create a SelectFunctionModel from a dictionary.

        Args:
            dict_: The configuration dictionary.

        Returns:
            An initialized SelectFunctionModel.
        """
        try:
            function_name = dict_[FUNCTION]
            arguments_dict = dict_[ARGUMENTS]

            # Process the arguments
            columns = []
            for col_name in arguments_dict[COLUMNS]:
                columns.append(f.col(col_name))

            arguments = cls.Args(columns=columns)

        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=dict_) from e

        return cls(function=function_name, arguments=arguments)
