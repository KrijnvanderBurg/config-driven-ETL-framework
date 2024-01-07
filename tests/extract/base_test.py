"""
Extract classes tests.


==================================
====== ExtractFormat class =======
==================================

| ✓ | Tests
|---|---------------------------------------------
| ✓ | Enum creation in Fixture from valid strings.
| ✓ | Raise ValueError for invalid enum creation.


==================================
===== ExtractMethod class ========
==================================

| ✓ | Tests
|---|---------------------------------------------
| ✓ | Enum creation in Fixture from valid strings.
| ✓ | Raise ValueError for invalid enum creation.


==================================
======= ExtractSpec class ========
==================================

| ✓ | Tests
|---|-----------------------------------------
| ✓ | Test all attributes are of correct type.


==================================
========= Extract class ==========
==================================

| ✓ | Tests
|---|-----------------------------------
| ✓ | Implement extract ABC in new class.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from collections.abc import Generator

import pytest
from datastore.extract.base import Extract, ExtractFormat, ExtractMethod, ExtractSpec
from pyspark.sql.types import StructType

# ==================================
# ======= ExtractFormat class =======
# ==================================

# ============ Fixtures ============


@pytest.fixture(name="extract_data_format", params=["parquet", "json", "csv"])
def fixture_extract_format(request) -> Generator[ExtractFormat, None, None]:
    """
    Fixture for creating ExtractFormat instances.

    Args:
        request (pytest.FixtureRequest): The fixture request.

    Yields:
        ExtractFormat: The ExtractFormat object based on the fixture request parameter.
    """
    # Assert
    yield ExtractFormat(request.param)


# ============== Tests =================


@pytest.mark.parametrize("invalid_value", ["invalid", False, True, 0, 1])
def test_extract_format_invalid_creation(invalid_value):
    """
    Assert that creating an enum value from an invalid value raises ValueError.

    Args:
        invalid_value (str): invalid values for ExtractFormat.
    """
    with pytest.raises(ValueError):
        # Assert
        ExtractFormat(invalid_value)


# ==================================
# ======= ExtractMethod class =========
# ==================================

# ============ Fixtures ============


@pytest.fixture(name="extract_method", params=["batch", "streaming"])
def fixture_extract_method(request) -> Generator[ExtractMethod, None, None]:
    """
    Fixture for creating ExtractMethod instances.

    Args:
        request (pytest.FixtureRequest): The fixture request.

    Yields:
        ExtractMethod: The ExtractMethod object based on the fixture request parameter.
    """
    # Assert
    yield ExtractMethod(request.param)


# ============== Tests =================


@pytest.mark.parametrize("invalid_value", ["invalid", False, True, 0, 1])
def test_extract_method_invalid_creation(invalid_value):
    """
    Assert that creating an enum value from an invalid value raises ValueError.

    Args:
        invalid_value (str): invalid values for ExtractMethod.
    """
    with pytest.raises(ValueError):
        # Assert
        ExtractMethod(invalid_value)


# ==================================
# ======= ExtractSpec class =========
# ==================================


# ============ Fixtures ============


@pytest.fixture(name="extract_options", params=[{}])
def fixture_extract_options(request) -> Generator[dict, None, None]:
    """
    Fixture for execution environment options added to extract.

    Yields:
        (dict): an dicitonary of key:{option name} with value: {option value}.
    """
    yield request.param


@pytest.fixture(name="extract_spec_confeti")
def fixture_extract_spec_confeti() -> dict:
    """
    Fixture for extract spec confeti file.

    Returns:
        (dict): a valid confeti dictionary of extract spec.
    """
    # Arrange
    return {
        "spec_id": "extract_spec_id",
        "method": "batch",
        "data_format": "parquet",
        "location": "/test.parquet",
        "options": {},
        "schema": "",
        "schema_filepath": "",
    }


@pytest.fixture(name="extract_spec")
def fixture_extract_spec(
    tmpdir,
    extract_method: ExtractMethod,
    extract_data_format: ExtractFormat,
    extract_options: dict,
    schema: StructType,
    schema_file: str,
) -> Generator[ExtractSpec, None, None]:
    """
    Fixture for creating a ExtractSpec instance.

    Args:
        tmpdir (str): Temporary directory fixture.
        extract_method (ExtractMethod): ExtractMethod fixture.
        extract_data_format (ExtractFormat): ExtractFormat fixture.
        extract_options (dict): execution environment options fixture.
        schema (StructType): Schema fixture.
        schema_file (str): Filepath to schema file fixture.

    Yields:
        (ExtractSpec): An instance of ExtractSpec for testing.
    """
    yield ExtractSpec(
        spec_id="extract_spec_id",
        method=extract_method.value,
        data_format=extract_data_format.value,
        location=f"{tmpdir}/test.{extract_data_format.value}",
        options=extract_options,
        schema=schema.json(),
        schema_filepath=schema_file,
    )


# ============= Tests ==============


def test_extract_spec(extract_spec) -> None:
    """
    Assert that all ExtractSpec attributes are of correct type.

    Args:
        extract_spec (ExtractSpec): ExtractSpec fixture.
    """
    # Assert
    assert isinstance(extract_spec.spec_id, str)
    assert isinstance(extract_spec.method, ExtractMethod)
    assert isinstance(extract_spec.data_format, ExtractFormat)
    assert isinstance(extract_spec.location, str)
    assert isinstance(extract_spec.options, dict)
    assert isinstance(extract_spec.schema, StructType)


def test_extract_spec_from_confeti(extract_spec_confeti) -> None:
    """
    Assert that confeti configuration file instantiates extract spec object.
    """
    # Act
    extract_spec = ExtractSpec.from_confeti(confeti=extract_spec_confeti)

    # Assert
    assert extract_spec.spec_id == "extract_spec_id"
    assert extract_spec.method == ExtractMethod.BATCH
    assert extract_spec.data_format == ExtractFormat.PARQUET
    assert extract_spec.location == "/test.parquet"
    assert extract_spec.options == {}
    assert extract_spec.schema is None


# ==================================
# ========= Extract class ===========
# ==================================

# ============ Tests ===============


def test_extract_abc_implementation(extract_spec) -> None:
    """
    Test implementation of abstract Extract class.

    Args:
        extract_spec (ExtractSpec): ExtractSpec fixture.
    """
    # Arrange
    Extract.__abstractmethods__ = frozenset()

    # Act
    class MockExtract(Extract):
        """Mock implementation of Extract class for testing purposes."""

        def extract(self):
            """Mock implementation of Extract class for testing purposes."""

    # Assert
    assert isinstance(MockExtract(spec=extract_spec), Extract)
