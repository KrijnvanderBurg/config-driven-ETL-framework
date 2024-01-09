"""
LoadMethod class tests.

| ✓ | Tests
|---|---------------------------------------------
| ✓ | Trip wire test all LoadMethod values equals list of test values.
| ✓ | LoadMethod creation from valid values.
| ✓ | LoadMethod raises ValueError from invalid valus.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from collections.abc import Generator

import pytest
from datastore.load.base import LoadMethod

load_method_values = ["batch", "streaming"]
load_method_invalid_values = ["invalid"]

# ============ Fixtures ============


@pytest.fixture(name="load_method")
def fixture_load_method() -> LoadMethod:
    """
    Fixture for creating LoadMethod from valid value.

    Returns:
        LoadMethod: LoadMethod fixture.
    """
    return LoadMethod("batch")


@pytest.fixture(name="load_method_matrix", params=load_method_values)
def fixture_load_method_matrix(request) -> Generator[LoadMethod, None, None]:
    """
    Matrix fixture for creating LoadMethod matrix from valid values.

    Args:
        request (pytest.FixtureRequest): Fixture parameter.

    Yields:
        LoadMethod: LoadMethod fixture.
    """
    yield LoadMethod(request.param)


# ============== Tests =============


@pytest.mark.parametrize("valid_value", load_method_values)
def test_load_method_creation(valid_value: str):
    """
    Assert that creating LoadMethod from invalid value raises ValueError.

    Args:
        valid_value (str): Valid values for LoadMethod.
    """
    # Act
    load_method = LoadMethod(valid_value)

    # Assert
    assert isinstance(load_method, LoadMethod)
    assert load_method.value == valid_value


@pytest.mark.parametrize("invalid_value", load_method_invalid_values)
def test_load_method_creation_invalid(invalid_value):
    """
    Assert that creating an LoadMethod from an invalid value raises ValueError.

    Args:
        invalid_value (str): Invalid value for LoadMethod.
    """
    # Assert
    with pytest.raises(ValueError):
        LoadMethod(invalid_value)


def test_load_method_tripwire():
    """
    Assert that all LoadMethod values are equal to list of test values.

    On failure, there is a mismatch between LoadMethod values and test values list.
    """
    # Arrange
    enum_members = [e.value for e in LoadMethod]

    enum_members.sort()
    load_method_values.sort()

    # Assert
    assert enum_members == load_method_values
