"""
ExtractFormat class tests.

| ✓ | Tests
|---|---------------------------------------------
| ✓ | ExtractFormat creation from valid values.
| ✓ | ExtractFormat raises ValueError from invalid valus.
| ✓ | Trip wire test all ExtractFormat values equals list of test values.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from collections.abc import Generator

import pytest
from datastore.extract.base import ExtractFormat

extract_data_format_values = ["parquet", "json", "csv"]
extract_data_format_invalid_values = ["invalid"]

# ============ Fixtures ============


@pytest.fixture(name="extract_data_format")
def fixture_extract_data_format() -> ExtractFormat:
    """
    Fixture for creating ExtractFormat from valid value.

    Returns:
        ExtractFormat: ExtractFormat fixture.
    """
    return ExtractFormat("parquet")


@pytest.fixture(name="extract_data_format_matrix", params=extract_data_format_values)
def fixture_extract_data_format_matrix(request) -> Generator[ExtractFormat, None, None]:
    """
    Matrix fixture for creating ExtractFormat matrix from valid values.

    Args:
        request (pytest.FixtureRequest): Fixture parameter.

    Yields:
        ExtractFormat: ExtractFormat fixture.
    """
    yield ExtractFormat(request.param)


# ============== Tests =============


@pytest.mark.parametrize("valid_value", extract_data_format_values)
def test_extract_data_format_creation(valid_value: str):
    """
    Assert that creating ExtractFormat from invalid value raises ValueError.

    Args:
        valid_value (str): Valid values for ExtractFormat.
    """
    # Act
    extract_data_format = ExtractFormat(valid_value)

    # Assert
    assert isinstance(extract_data_format, ExtractFormat)
    assert extract_data_format.value == valid_value


@pytest.mark.parametrize("invalid_value", extract_data_format_invalid_values)
def test_extract_data_format_creation_invalid(invalid_value):
    """
    Assert that creating an ExtractFormat from an invalid value raises ValueError.

    Args:
        invalid_value (str): Invalid value for ExtractFormat.
    """
    # Assert
    with pytest.raises(ValueError):
        ExtractFormat(invalid_value)


def test_extract_data_format_tripwire():
    """
    Assert that all ExtractFormat values are equal to list of test values.

    On failure, there is a mismatch between ExtractFormat values and test values list.
    """
    # Arrange
    enum_members = [e.value for e in ExtractFormat]

    enum_members.sort()
    extract_data_format_values.sort()

    # Assert
    assert enum_members == extract_data_format_values
