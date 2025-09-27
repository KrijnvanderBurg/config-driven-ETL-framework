"""Tests for FileChannel model.

These tests verify that FileChannel instances can be correctly created from
configuration and function as expected.
"""

from pathlib import Path
from typing import Any

import pytest

from flint.alert.channels.file import FileChannel

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="file_config")
def fixture_file_config(tmp_path: Path) -> dict[str, Any]:
    """Provide a representative file channel configuration."""
    return {
        "name": "file1",
        "description": "a file channel",
        "file_path": tmp_path / "alerts.log",
    }


def test_file_channel_creation__from_config__creates_valid_model(file_config: dict[str, Any]) -> None:
    """Test specifically for the creation process itself."""
    # Act
    ch = FileChannel(**file_config)

    # Assert
    assert ch.channel_id == "file"
    assert ch.name == "file1"
    assert ch.description == "a file channel"
    assert isinstance(ch.file_path, Path)
    assert ch.file_path.name == "alerts.log"


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="file_channel")
def fixture_file_channel(file_config: dict[str, Any]) -> FileChannel:
    """Create FileChannel instance from configuration."""
    return FileChannel(**file_config)


def test_file_channel_properties__after_creation__match_expected_values(file_channel: FileChannel) -> None:
    """Test model properties using the instantiated object fixture.

    Use hard-coded expected values to avoid comparing fixtures directly.
    """
    assert file_channel.name == "file1"
    assert isinstance(file_channel.file_path, Path)
    assert file_channel.file_path.name == "alerts.log"
    # Additional attributes asserted on the fixture
    assert file_channel.channel_id == "file"
    assert file_channel.description == "a file channel"


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #
