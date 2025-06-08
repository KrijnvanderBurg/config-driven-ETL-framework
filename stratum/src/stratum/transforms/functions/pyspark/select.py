"""
Column transform function.


========================================================================================
PrimePythonPrinciples © 2024 by Krijn van der Burg is licensed under CC BY-SA 4.0

For inquiries contact Krijn van der Burg at https://www.linkedin.com/in/krijnvanderburg/
========================================================================================
"""

from abc import ABC
from collections.abc import Callable
from typing import Any, Final, Self

from pyspark.sql import functions as f
from pyspark.sql.column import Column

from stratum.exceptions import DictKeyError
from stratum.transforms.functions.base import (
    ArgsAbstract,
    ArgsT,
    FunctionAbstract,
    FunctionModelAbstract,
    FunctionModelT,
    FunctionPyspark,
)
from stratum.types import RegistrySingleton

COLUMNS: Final[str] = "columns"


class SelectFunctionModelAbstract(FunctionModelAbstract[ArgsT], ABC):
    """An abstract base class for DataFrame Select functions."""

    class Args(ArgsAbstract, ABC):
        """An abstract base class for arguments of Select functions."""


class SelectFunctionModelPysparkArgs(SelectFunctionModelAbstract.Args):
    """The arguments for PySpark DataFrame Select functions."""

    def __init__(self, columns: list[Column]) -> None:
        self.columns = columns

    @property
    def columns(self) -> list[Column]:
        return self._columns

    @columns.setter
    def columns(self, value: list[Column]) -> None:
        self._columns = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create Args object from a JSON dictionary.

        Args:
            confeti (dict[str, Any]): The JSON dictionary.

        Returns:
            SelectFunctionModelPyspark.Args: The Args object created from the JSON dictionary.
        """
        try:
            columns = []
            for col_name in confeti[COLUMNS]:
                columns.append(f.col(col_name))
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti)

        return cls(columns=columns)


class SelectFunctionModelPyspark(SelectFunctionModelAbstract[SelectFunctionModelPysparkArgs]):
    """A concrete implementation of DataFrame Select functions using PySpark."""

    args_concrete = SelectFunctionModelPysparkArgs


class SelectFunctionAbstract(FunctionAbstract[FunctionModelT], ABC):
    """
    Encapsulates column transformation logic for PySpark DataFrames.

    Attributes:
        model (...): The SelectModel object containing the Selecting information.

    Methods:
        from_confeti(confeti: dict[str, Any]) -> Self: Create SelectTransform object from json.
        transform() -> Callable: Selects column(s) to new type.
    """


class SelectFunctionPyspark(SelectFunctionAbstract[SelectFunctionModelPyspark], FunctionPyspark):
    """
    Encapsulates column transformation logic for PySpark DataFrames.

    Attributes:
        model (...): The SelectModel object containing the Selecting information.

    Methods:
        from_confeti(confeti: dict[str, Any]) -> Self: Create SelectTransform object from json.
        transform() -> Callable: Selects column(s) to new type.
    """

    model_concrete = SelectFunctionModelPyspark

    def transform(self) -> Callable:
        """
        Selects columns with aliases.

        Args:
            columns (Ordereddict[str, Any]): Ordered mapping of column names to aliases.

        Returns:
            (Callable): Function for `DataFrame.transform()`.

        Examples:
            Consider the following DataFrame schema:

            ```
            root
            |-- name: string (nullable = true)
            |-- age: integer (nullable = true)
            ```

            Applying the confeti 'select_with_alias' function:

            ```
            {"function": "select_with_alias", "arguments": {"columns": {"age": "years_old",}}}
            ```

            The resulting DataFrame schema will be:

            ```
            root
            |-- years_old: integer (nullable = true)
            ```
        """

        def f(dataframe_registry: RegistrySingleton, dataframe_name: str) -> None:
            dataframe_registry[dataframe_name] = dataframe_registry[dataframe_name].select(
                *self.model.arguments.columns
            )

        return f
