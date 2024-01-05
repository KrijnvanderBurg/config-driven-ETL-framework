"""
Writer classes tests.


# ==================================
# ======= WriterFormat class =======
# ==================================

# | ✓ | Tests
# |---|---------------------------------------------
# | ✓ | Enum creation in Fixture from valid strings.
# | ✓ | Raise ValueError for invalid enum creation.


# ==================================
# ======= WriterType class =========
# ==================================

# | ✓ | Tests
# |---|---------------------------------------------
# | ✓ | Enum creation in Fixture from valid strings.
# | ✓ | Raise ValueError for invalid enum creation.


# ==================================
# ===== WriterOperation class ======
# ==================================

# | ✓ | Tests
# |---|---------------------------------------------
# | ✓ | Enum creation in Fixture from valid strings.
# | ✓ | Raise ValueError for invalid enum creation.


# ==================================
# ======= WriterSpec class =========
# ==================================

# | ✓ | Tests
# |---|-----------------------------------------
# | ✓ | Test all attributes are of correct type.


# ==================================
# ========= Writer class ===========
# ==================================

# | ✓ | Tests
# |---|-----------------------------------
# | ✓ | Implement writer ABC in new class.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from collections.abc import Generator

import pytest
from datastore.writer import Writer, WriterFormat, WriterOperation, WriterSpec, WriterType

# ==================================
# ======= WriterFormat class =======
# ==================================

# ============ Fixtures ============


@pytest.fixture(name="writer_format", params=["parquet", "json", "csv"])
def fixture_writer_format(request) -> Generator[WriterFormat, None, None]:
    """
    Fixture for creating WriterFormat instances.

    Args:
        request (pytest.FixtureRequest): The fixture request.

    Yields:
        WriterFormat: The WriterFormat object based on the fixture request parameter.
    """
    # Assert
    yield WriterFormat(request.param)


# ============ Tests ===============


@pytest.mark.parametrize("invalid_value", ["invalid", False, True, 0, 1])
def test_writer_format_invalid_creation(invalid_value) -> None:
    """
    Assert that creating an enum value from an invalid value raises ValueError.

    Args:
        invalid_value (str): invalid values for WriterOperation.
    """
    with pytest.raises(ValueError):
        # Assert
        WriterFormat(invalid_value)


# ==================================
# ======= WriterType class =========
# ==================================

# ============ Fixtures ============


@pytest.fixture(name="writer_type", params=["batch", "streaming"])
def fixture_writer_type(request) -> Generator[WriterType, None, None]:
    """
    Fixture for creating WriterType instances.

    Args:
        request (pytest.FixtureRequest): The fixture request.

    Yields:
        WriterType: The WriterType object based on the fixture request parameter.
    """
    # Assert
    yield WriterType(request.param)


# ============ Tests ===============


@pytest.mark.parametrize("invalid_value", ["invalid", False, True, 0, 1])
def test_writer_type_invalid_creation(invalid_value) -> None:
    """Assert that creating an enum value from an invalid value raises ValueError.

    Args:
        invalid_value (str): invalid values for WriterOperation.
    """
    with pytest.raises(ValueError):
        # Assert
        WriterType(invalid_value)


# ==================================
# ===== WriterOperation class ======
# ==================================

# ============ Fixtures ============


@pytest.fixture(name="writer_operation", params=["complete", "append", "update"])
def fixture_writer_operation(request) -> Generator[WriterOperation, None, None]:
    """
    Fixture for creating WriterOperation instances.

    Args:
        request (pytest.FixtureRequest): The fixture request.

    Yields:
        WriterOperation: The WriterOperation object based on the fixture request parameter.
    """
    # Assert
    yield WriterOperation(request.param)


# ============ Tests ===============


@pytest.mark.parametrize("invalid_value", ["invalid", False, True, 0, 1])
def test_writer_operation_invalid_creation(invalid_value) -> None:
    """Assert that creating an enum value from an invalid value raises ValueError.

    Args:
        invalid_value (str): invalid values for WriterOperation.
    """
    with pytest.raises(ValueError):
        # Assert
        WriterOperation(invalid_value)


# ==================================
# ======= WriterSpec class =========
# ==================================

# ============ Fixtures ============


@pytest.fixture(name="writer_options", params=[{}])
def fixture_writer_options(request) -> Generator[dict, None, None]:
    """
    Fixture for execution environment options added to writer.

    Args:
        request (pytest.FixtureRequest): The fixture request.

    Yields:
        (dict): an dicitonary of key:{option name} with value: {option value}.
    """

    yield request.param


@pytest.fixture(name="writer_spec")
def fixture_writer_spec(
    tmpdir,
    writer_type: WriterType,
    writer_operation: WriterOperation,
    writer_format: WriterFormat,
    writer_options: dict,
) -> Generator[WriterSpec, None, None]:
    """Fixture for creating a WriterSpec instance.

    Args:
        tmpdir (str): Temporary directory fixture.
        writer_type (WriterType): WriterType fixture.
        writer_operation (WriterOperation): WriterOperation fixture.
        writer_format (WriterFormat): WriterFormat fixture.
        writer_options (dict): execution environment options.

    Yields:
        writer_spec (WriterSpec): An instance of WriterSpec for testing.
    """

    location = f"{tmpdir}/test.{writer_format}"

    if writer_type == WriterType.STREAMING:
        writer_options["checkpointLocation"] = f"{tmpdir}/test/checkpoint"

    yield WriterSpec(
        spec_id="test_spec_id",
        writer_type=writer_type.value,
        writer_operation=writer_operation.value,
        writer_format=writer_format.value,
        location=location,
        options=writer_options,
    )


# ============ Tests ===============


def test_writer_spec(writer_spec) -> None:
    """
    Assert that all WriterSpec attributes are of correct type.

    Args:
        writer_spec (WriterSpec): WriterSpec fixture.
    """
    # Assert
    assert isinstance(writer_spec.spec_id, str)
    assert isinstance(writer_spec.writer_type, WriterType)
    assert isinstance(writer_spec.writer_operation, WriterOperation)
    assert isinstance(writer_spec.writer_format, WriterFormat)
    assert isinstance(writer_spec.location, str)
    assert isinstance(writer_spec.options, dict)


# ==================================
# ========= Writer class ===========
# ==================================

# ============ Tests ===============


def test_writer_abc_implementation(writer_spec, df_empty) -> None:
    """
    Test implementation of abstract Writer class.

    Args:
        writer_spec (WriterSpec): WriterSpec fixture.
        df_empty (DataFrame): Test dataframe with schema but without rows fixture.
    """
    # Arrange
    Writer.__abstractmethods__ = frozenset()

    # Act
    class MockWriter(Writer):
        """Mock implementation of Writer class for testing purposes."""

        def write(self):
            """Mock implementation of Writer class for testing purposes."""

    # Assert
    assert isinstance(MockWriter(spec=writer_spec, dataframe=df_empty), Writer)
