"""
LoadFormat class tests.

| ✓ | Tests
|---|---------------------------------------------
| ✓ | Trip wire test all LoadFormat values equals list of test values.
| ✓ | LoadFormat creation from valid values.
| ✓ | LoadFormat raises ValueError from invalid valus.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from collections.abc import Generator

import pytest
from datastore.load.base import LoadFormat

load_format_values = ["parquet", "json", "csv"]
load_format_invalid_values = ["invalid"]

# ============ Fixtures ============


@pytest.fixture(name="load_format")
def fixture_load_format() -> LoadFormat:
    """
    Fixture for creating LoadFormat from valid value.

    Returns:
        LoadFormat: LoadFormat fixture.
    """
    return LoadFormat("parquet")


@pytest.fixture(name="load_format_matrix", params=load_format_values)
def fixture_load_format_matrix(request) -> Generator[LoadFormat, None, None]:
    """
    Matrix fixture for creating LoadFormat matrix from valid values.

    Args:
        request (pytest.FixtureRequest): Fixture parameter.

    Yields:
        LoadFormat: LoadFormat fixture.
    """
    yield LoadFormat(request.param)


# ============== Tests =============


@pytest.mark.parametrize("valid_value", load_format_values)
def test_load_format_creation(valid_value: str):
    """
    Assert that creating LoadFormat from invalid value raises ValueError.

    Args:
        valid_value (str): Valid values for LoadFormat.
    """
    # Act
    load_format = LoadFormat(valid_value)

    # Assert
    assert isinstance(load_format, LoadFormat)
    assert load_format.value == valid_value


@pytest.mark.parametrize("invalid_value", load_format_invalid_values)
def test_load_format_creation_invalid(invalid_value):
    """
    Assert that creating an LoadFormat from an invalid value raises ValueError.

    Args:
        invalid_value (str): Invalid value for LoadFormat.
    """
    # Assert
    with pytest.raises(ValueError):
        LoadFormat(invalid_value)


def test_load_format_tripwire():
    """
    Assert that all LoadFormat values are equal to list of test values.

    On failure, there is a mismatch between LoadFormat values and test values list.
    """
    # Arrange
    enum_members = [e.value for e in LoadFormat]

    enum_members.sort()
    load_format_values.sort()

    # Assert
    assert enum_members == load_format_values
