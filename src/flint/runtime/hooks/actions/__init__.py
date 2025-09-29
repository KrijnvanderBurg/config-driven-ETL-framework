"""Transform function implementations for Spark.

This module imports all available transform functions to register them with the
TransformFunctionRegistry. Each transform function is automatically registered
when imported.
"""

from typing import Annotated

from pydantic import Discriminator

from flint.runtime.hooks.actions.move_files import MoveFiles

# __all__ = []

HooksActionsUnion = Annotated[
    MoveFiles | MoveFiles,
    Discriminator("action"),
]
