"""
ExtracMethod class tests.

| ✓ | Tests
|---|---------------------------------------------
| ✓ | ExtractMethod creation from valid values.
| ✓ | ExtractMethod raises ValueError from invalid valus.
| ✓ | Trip wire test all ExtractMethod values equals list of test values.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from collections.abc import Generator

import pytest
from datastore.extract.base import ExtractMethod

# ============ Fixtures ============

extract_method_values = ["batch", "streaming"]
extract_method_invalid_values = ["invalid"]


@pytest.fixture(name="extract_method")
def fixture_extract_method() -> ExtractMethod:
    """
    Fixture for creating ExtractMethod from valid value.

    Returns:
        ExtractMethod: ExtractMethod fixture.
    """
    return ExtractMethod("batch")


@pytest.fixture(name="extract_method_matrix", params=extract_method_values)
def fixture_extract_method_matrix(request) -> Generator[ExtractMethod, None, None]:
    """
    Matrix fixture for creating ExtractMethod matrix from valid values.

    Args:
        request (pytest.FixtureRequest): Fixture parameter.

    Yields:
        ExtractMethod: ExtractMethod fixture.
    """
    yield ExtractMethod(request.param)


# ============== Tests =============


@pytest.mark.parametrize("valid_value", extract_method_values)
def test_extract_method_creation(valid_value: str):
    """
    Assert that creating ExtractMethod from invalid value raises ValueError.

    Args:
        valid_value (str): Valid values for ExtractMethod.
    """
    extract_method = ExtractMethod(valid_value)
    assert isinstance(extract_method, ExtractMethod)
    assert extract_method.value == valid_value


@pytest.mark.parametrize("invalid_value", extract_method_invalid_values)
def test_extract_method_creation_invalid(invalid_value):
    """
    Assert that creating an ExtractMethod from an invalid value raises ValueError.

    Args:
        invalid_value (str): Invalid value for ExtractMethod.
    """
    # Assert
    with pytest.raises(ValueError):
        ExtractMethod(invalid_value)


def test_extract_method_tripwire():
    """
    Assert that all ExtractMethod values are equal to list of test values.

    On failure, there is a mismatch between ExtractMethod values and test values list.
    """
    # Arrange
    enum_members = [e.value for e in ExtractMethod]

    enum_members.sort()
    extract_method_values.sort()

    # Assert
    assert enum_members == extract_method_values
