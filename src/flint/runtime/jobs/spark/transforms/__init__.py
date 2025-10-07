"""Transform function implementations for Spark.

This module imports all available transform functions to register them with the
TransformFunctionRegistry. Each transform function is automatically registered
when imported.
"""

from typing import Annotated

from pydantic import Discriminator

from .cast import CastFunction
from .drop import DropFunction
from .dropduplicates import DropDuplicatesFunction
from .filter import FilterFunction
from .join import JoinFunction
from .select import SelectFunction
from .withcolumn import WithColumnFunction

__all__ = [
    "CastFunction",
    "DropFunction",
    "DropDuplicatesFunction",
    "FilterFunction",
    "JoinFunction",
    "SelectFunction",
    "WithColumnFunction",
]

TransformFunctionSparkUnion = Annotated[
    CastFunction
    | DropFunction
    | DropDuplicatesFunction
    | FilterFunction
    | JoinFunction
    | SelectFunction
    | WithColumnFunction,
    Discriminator("function_type"),
]
