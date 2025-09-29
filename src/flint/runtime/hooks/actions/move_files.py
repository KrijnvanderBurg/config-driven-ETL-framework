"""Module for moving files as an action in Flint runtime hooks."""

import os
import shutil

from flint.runtime.hooks.actions.base import ActionBase


class MoveFiles(ActionBase):
    """Action to move files from one location to another.

    Args:
        source (str): The source file path.
        destination (str): The destination file path.
    """

    source: str
    destination: str

    def execute(self) -> None:
        """Execute the move files action."""

        if not os.path.exists(self.source):
            raise FileNotFoundError(f"Source file {self.source} does not exist.")
        shutil.move(self.source, self.destination)
