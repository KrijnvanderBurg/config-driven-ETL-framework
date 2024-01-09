"""
LoadOperation class tests.

| ✓ | Tests
|---|---------------------------------------------
| ✓ | Trip wire test all LoadOperation values equals list of test values.
| ✓ | LoadOperation creation from valid values.
| ✓ | LoadOperation raises ValueError from invalid valus.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from collections.abc import Generator

import pytest
from datastore.load.base import LoadOperation

load_operation_values = ["complete", "append", "update"]
load_operation_invalid_values = ["invalid"]

# ============ Fixtures ============


@pytest.fixture(name="load_operation")
def fixture_load_operation() -> LoadOperation:
    """
    Fixture for creating LoadOperation from valid value.

    Returns:
        LoadOperation: LoadOperation fixture.
    """
    return LoadOperation("complete")


@pytest.fixture(name="load_operation_matrix", params=load_operation_values)
def fixture_load_operation_matrix(request) -> Generator[LoadOperation, None, None]:
    """
    Matrix fixture for creating LoadOperation matrix from valid values.

    Args:
        request (pytest.FixtureRequest): Fixture parameter.

    Yields:
        LoadOperation: LoadOperation fixture.
    """
    yield LoadOperation(request.param)


# ============== Tests =============


@pytest.mark.parametrize("valid_value", load_operation_values)
def test_load_operation_creation(valid_value: str):
    """
    Assert that creating LoadOperation from invalid value raises ValueError.

    Args:
        valid_value (str): Valid values for LoadOperation.
    """
    # Act
    load_operation = LoadOperation(valid_value)

    # Assert
    assert isinstance(load_operation, LoadOperation)
    assert load_operation.value == valid_value


@pytest.mark.parametrize("invalid_value", load_operation_invalid_values)
def test_load_operation_creation_invalid(invalid_value):
    """
    Assert that creating an LoadOperation from an invalid value raises ValueError.

    Args:
        invalid_value (str): Invalid value for LoadOperation.
    """
    # Assert
    with pytest.raises(ValueError):
        LoadOperation(invalid_value)


def test_load_operation_tripwire():
    """
    Assert that all LoadOperation values are equal to list of test values.

    On failure, there is a mismatch between LoadOperation values and test values list.
    """
    # Arrange
    enum_members = [e.value for e in LoadOperation]

    enum_members.sort()
    load_operation_values.sort()

    # Assert
    assert enum_members == load_operation_values
