"""
Reader classes tests.


# ==================================
# ======= ReaderFormat class =======
# ==================================

# | ✓ | Tests
# |---|---------------------------------------------
# | ✓ | Enum creation in Fixture from valid strings.
# | ✓ | Raise ValueError for invalid enum creation.


# ==================================
# ======= ReaderType class =========
# ==================================

# | ✓ | Tests
# |---|---------------------------------------------
# | ✓ | Enum creation in Fixture from valid strings.
# | ✓ | Raise ValueError for invalid enum creation.


# ==================================
# ======= ReaderSpec class =========
# ==================================

# | ✓ | Tests
# |---|-----------------------------------------
# | ✓ | Test all attributes are of correct type.


# ==================================
# ========= Reader class ===========
# ==================================

# | ✓ | Tests
# |---|-----------------------------------
# | ✓ | Implement reader ABC in new class.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from collections.abc import Generator

import pytest
from datastore.reader import Reader, ReaderFormat, ReaderSpec, ReaderType
from pyspark.sql.types import StructType

# ==================================
# ======= ReaderFormat class =======
# ==================================

# ============ Fixtures ============


@pytest.fixture(name="reader_format", params=["parquet", "json", "csv"])
def fixture_reader_format(request) -> Generator[ReaderFormat, None, None]:
    """
    Fixture for creating ReaderFormat instances.

    Args:
        request (pytest.FixtureRequest): The fixture request.

    Yields:
        ReaderFormat: The ReaderFormat object based on the fixture request parameter.
    """
    # Assert
    yield ReaderFormat(request.param)


# ============== Tests =================


@pytest.mark.parametrize("invalid_value", ["invalid", False, True, 0, 1])
def test_reader_format_invalid_creation(invalid_value):
    """
    Assert that creating an enum value from an invalid value raises ValueError.

    Args:
        invalid_value (str): invalid values for ReaderFormat.
    """
    with pytest.raises(ValueError):
        # Assert
        ReaderFormat(invalid_value)


# ==================================
# ======= ReaderType class =========
# ==================================

# ============ Fixtures ============


@pytest.fixture(name="reader_type", params=["batch", "streaming"])
def fixture_reader_type(request) -> Generator[ReaderType, None, None]:
    """
    Fixture for creating ReaderType instances.

    Args:
        request (pytest.FixtureRequest): The fixture request.

    Yields:
        ReaderType: The ReaderType object based on the fixture request parameter.
    """
    # Assert
    yield ReaderType(request.param)


# ============== Tests =================


@pytest.mark.parametrize("invalid_value", ["invalid", False, True, 0, 1])
def test_reader_type_invalid_creation(invalid_value):
    """
    Assert that creating an enum value from an invalid value raises ValueError.

    Args:
        invalid_value (str): invalid values for ReaderType.
    """
    with pytest.raises(ValueError):
        # Assert
        ReaderType(invalid_value)


# ==================================
# ======= ReaderSpec class =========
# ==================================


# ============ Fixtures ============


@pytest.fixture(name="reader_options", params=[{}])
def fixture_reader_options(request) -> Generator[dict, None, None]:
    """
    Fixture for execution environment options added to reader.

    Yields:
        (dict): an dicitonary of key:{option name} with value: {option value}.
    """
    yield request.param


@pytest.fixture(name="reader_spec")
def fixture_reader_spec(
    tmpdir,
    reader_type: ReaderType,
    reader_format: ReaderFormat,
    reader_options: dict,
    schema: StructType,
    schema_file: str,
) -> Generator[ReaderSpec, None, None]:
    """
    Fixture for creating a ReaderSpec instance.

    Args:
        tmpdir (str): Temporary directory fixture.
        reader_type (ReaderType): ReaderType fixture.
        reader_format (ReaderFormat): ReaderFormat fixture.
        reader_options (dict): execution environment options.
        schema (StructType): Schema fixture.
        schema_file (str): Filepath to schema file fixture.

    Yields:
        (ReaderSpec): An instance of WriterSpec for testing.
    """
    yield ReaderSpec(
        spec_id="test_spec_id",
        reader_type=reader_type.value,
        reader_format=reader_format.value,
        location=f"{tmpdir}/test.{reader_format.value}",
        options=reader_options,
        schema=schema.json(),
        schema_filepath=schema_file,
    )


# ============= Tests ==============


def test_reader_spec(reader_spec) -> None:
    """
    Assert that all ReaderSpec attributes are of correct type.

    Args:
        reader_spec (ReaderSpec): ReaderSpec fixture.
    """
    # Assert
    assert isinstance(reader_spec.spec_id, str)
    assert isinstance(reader_spec.reader_type, ReaderType)
    assert isinstance(reader_spec.reader_format, ReaderFormat)
    assert isinstance(reader_spec.location, str)
    assert isinstance(reader_spec.options, dict)
    assert isinstance(reader_spec.schema, StructType)


# ==================================
# ========= Reader class ===========
# ==================================

# ============ Tests ===============


def test_reader_abc_implementation(reader_spec) -> None:
    """
    Test implementation of abstract Reader class.

    Args:
        reader_spec (ReaderSpec): ReaderSpec fixture.
    """
    # Arrange
    Reader.__abstractmethods__ = frozenset()

    # Act
    class MockReader(Reader):
        """Mock implementation of Reader class for testing purposes."""

        def read(self):
            """Mock implementation of Reader class for testing purposes."""

    # Assert
    assert isinstance(MockReader(spec=reader_spec), Reader)
