"""
Test file handler static class.

| ✓ | Tests
|---|-----------------------------------------------
| ✓ | Test creating a File instance from JSON content.
| ✓ | Test raising FileNotFoundError for a nonexistent file.
| ✓ | Test raising JSONDecodeError for an invalid JSON file.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

import json
from pathlib import Path

import pytest
from datastore.file import File

# ===================================
# =========== File class ============
# ===================================

# ============ Fixtures =============


@pytest.fixture(name="json_content")
def fixture_json_content() -> str:
    """
    Fixture providing sample JSON content.

    Returns:
        str: valid json key:value pair content for file testing.
    """
    return '{"key": "value"}'


@pytest.fixture(name="json_file")
def fixture_json_file(tmpdir, json_content: str) -> str:
    """
    Fixture providing a temporary JSON file with sample content.

    Args:
        tmpdir (str): Temporary directory fixture.
        json_content (str): json content fixture.
    """
    filepath = f"{tmpdir}/test.json"
    with open(file=filepath, mode="w", encoding="utf-8") as f:
        json.dump(json_content, fp=f)
    return filepath


# ============ Fixtures =============


def test_file_from_json(json_content: str, json_file: str) -> None:
    """
    Test creating a File instance from JSON content.

    Args:
        json_content (str): json content fixture.
        json_file (str): json file fixture.
    """
    # Arrange
    filepath = Path(json_file)

    # Assert
    assert json_content == File.from_json(filepath=filepath)


def test_file_from_json_nonexistent_file(tmpdir) -> None:
    """
    Test raising FileNotFoundError for a nonexistent file.

    Args:
        tmpdir (str): Temporary directory fixture.
    """
    # Arrange
    nonexistent_filepath = Path(f"{tmpdir}/nonexistent.json")

    # Act
    with pytest.raises(FileNotFoundError):
        # Assert
        File.from_json(filepath=Path(nonexistent_filepath))


def test_from_json_invalid_file(tmpdir) -> None:
    """
    Test raising JSONDecodeError for an invalid JSON file.

    Args:
        tmpdir (str): Temporary directory fixture.
    """
    # Arrange
    filepath = Path(f"{tmpdir}/test.json")
    with open(file=filepath, mode="w", encoding="utf-8") as f:
        f.write("invalid json string")

    # Act
    with pytest.raises(json.JSONDecodeError):
        # Assert
        File.from_json(filepath=filepath)
