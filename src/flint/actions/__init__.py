"""HTTP actions and other action implementations for Spark.

This module imports all available action functions to register them with the
HooksActionsUnion. Each action function is automatically registered
when imported.
"""

from typing import Annotated

from pydantic import Discriminator

from flint.actions.http import HttpAction
from flint.actions.move_files import MoveOrCopyJobFiles

# __all__ = []

HooksActionsUnion = Annotated[
    HttpAction | MoveOrCopyJobFiles,
    Discriminator("action"),
]
