"""Module for moving files as an action in Flint runtime hooks."""

from typing import Literal

from flint import BaseModel
from flint.actions.base import ActionBase


class Version(BaseModel):
    """Model to handle versioning of files.

    Args:
        enabled (bool): Whether versioning is enabled.
        max_versions (int): The maximum number of versions to keep.
    """

    datetime_format: str
    timestamp_timezone: str


class Checksum(BaseModel):
    """Model to handle file checksum verification.

    Args:
        algorithm (str): The checksum algorithm to use (e.g., 'md5', 'sha256').
        value (str): The expected checksum value.
    """

    algorithm: str
    chunk_size_bytes: int
    verify_checksum_after_transfer: bool


class DuplicateHandling(BaseModel):
    """Model to handle duplicate file scenarios.

    Args:
        action (str): The action to take when a duplicate is found. Options are 'overwrite', 'skip', or 'rename'.
    """

    dedupe_by: list[str]
    on_match: str
    on_mismatch: str
    checksum: Checksum
    version: Version


class MoveOrCopyJobFiles(ActionBase):
    """Action to move files from one location to another.

    Args:
        source (str): The source file path.
        destination (str): The destination file path.
    """

    action: Literal["move_or_copy_job_files"]
    source_location: str
    destination_location: str
    hierarchy_base_path: str

    def _execute(self) -> None:
        """Execute the move files action."""
        pass
