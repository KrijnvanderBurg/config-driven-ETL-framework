"""
Join transform function.


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

HOW: Final[str] = "how"
OTHER: Final[str] = "other"
ON_COLUMN: Final[str] = "on_column"
ON_EXPRESSION: Final[str] = "on_expression"


class JoinFunctionModelAbstract(FunctionModelAbstract[ArgsT], ABC):
    """An abstract base class for DataFrame join functions."""

    class Args(ArgsAbstract, ABC):
        """An abstract base class for arguments of join functions."""


class JoinFunctionModelPysparkArgs(JoinFunctionModelAbstract.Args):
    """The arguments for PySpark DataFrame join functions."""

    def __init__(self, how: str, other: str, on: list[Column] | list[str] | None) -> None:
        self.how = how
        self.other = other
        self.on = on

    @property
    def how(self) -> str:
        return self._how

    @how.setter
    def how(self, value: str) -> None:
        self._how = value

    @property
    def other(self) -> str:
        return self._other

    @other.setter
    def other(self, value: str) -> None:
        self._other = value

    @property
    def on(self) -> list[Column] | list[str] | None:
        return self._on

    @on.setter
    def on(self, value: list[Column] | list[str] | None) -> None:
        self._on = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create Args object from a JSON dictionary.

        Args:
            confeti (dict[str, Any]): The JSON dictionary.

        Returns:
            JoinFunctionModelPyspark.Args: The Args object created from the JSON dictionary.
        """

        try:
            how: str = confeti[HOW]
            other: str = confeti[OTHER]
            on_column: list[str] | None = confeti.get(ON_COLUMN, None)
            on_expression: str | None = confeti.get(ON_EXPRESSION, None)
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti)

        # The valid join types are defined here:
        # https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.join.html
        # here can see which are aliases:
        # https://github.com/apache/spark/blob/c4085f1f6f5830dbed82a5a29341f2769cdd1ea0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/joinTypes.scala#L38
        # For the sake of understandability, readability, and maintainability,
        #   only one alias is supported for each join type.
        valid_join_types = ["inner", "cross", "full", "left", "right", "left_semi", "left_anti"]

        if how not in valid_join_types:
            raise ValueError(f"Invalid join type {how}, valid join types are: {', '.join(valid_join_types)}")

        if on_column is None and on_expression is None:
            raise ValueError("Either 'on_column' or 'on_expression' must be set.")

        if on_column is not None and on_expression is not None:
            raise ValueError("Only one of 'on_column' or 'on_expression' can be set, not both.")

        on = []
        if on_column is not None:
            for col_name in on_column:
                on.append(col_name)
        elif on_expression is not None:
            on = [f.expr(on_expression)]
            # on = [on_expression]

        return cls(other=other, on=on, how=how)


class JoinFunctionModelPyspark(JoinFunctionModelAbstract[JoinFunctionModelPysparkArgs]):
    """A concrete implementation of DataFrame join functions using PySpark."""

    args_concrete = JoinFunctionModelPysparkArgs


class JoinFunctionAbstract(FunctionAbstract[FunctionModelT], ABC):
    """
    Attributes:
        model (...): The JoinModel object containing the joining information.

    Methods:
        from_confeti(confeti: dict[str, Any]) -> Self: Create JoinTransform object from json.
        transform() -> Callable: Joins dataframes
    """


class JoinFunctionPyspark(JoinFunctionAbstract[JoinFunctionModelPyspark], FunctionPyspark):
    """
    Encapsulates join transformation logic for PySpark DataFrames.

    Attributes:
        model (...): The JoinModel object containing the joining information.

    Methods:
        from_confeti(confeti: dict[str, Any]) -> Self: Create JoinTransform object from json.
        transform() -> Callable: Joins dataframes
    """

    model_concrete = JoinFunctionModelPyspark

    def transform(self) -> Callable:
        """
        Joins dataframes.

        Returns:
            Callable: Function for `DataFrame.transform()`.

        Examples:
            Consider the following DataFrame schema:

            ...
        """

        def f(dataframe_registry: RegistrySingleton, dataframe_name: str) -> None:
            dataframe_registry[dataframe_name] = (
                dataframe_registry[dataframe_name]
                .alias(dataframe_name)
                .join(
                    other=dataframe_registry[self.model.arguments.other].alias(self.model.arguments.other),
                    on=self.model.arguments.on,
                    how=self.model.arguments.how,
                )
            )

        return f
