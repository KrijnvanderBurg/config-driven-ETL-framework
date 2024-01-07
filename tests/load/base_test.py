"""
Load classes tests.


==================================
======== LoadFormat class ========
==================================

| ✓ | Tests
|---|---------------------------------------------
| ✓ | Enum creation in Fixture from valid strings.
| ✓ | Raise ValueError for invalid enum creation.


==================================
======= LoadMethod class =========
==================================

| ✓ | Tests
|---|---------------------------------------------
| ✓ | Enum creation in Fixture from valid strings.
| ✓ | Raise ValueError for invalid enum creation.


==================================
====== LoadOperation class =======
==================================

| ✓ | Tests
|---|---------------------------------------------
| ✓ | Enum creation in Fixture from valid strings.
| ✓ | Raise ValueError for invalid enum creation.


==================================
======== LoadSpec class ==========
==================================

| ✓ | Tests
|---|-----------------------------------------
| ✓ | Test all attributes are of correct type.


==================================
========== Load class ============
==================================

| ✓ | Tests
|---|-----------------------------------
| ✓ | Implement load ABC in new class.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from collections.abc import Generator

import pytest
from datastore.load.base import Load, LoadFormat, LoadMethod, LoadOperation, LoadSpec

# ==================================
# ======= LoadFormat class =======
# ==================================

# ============ Fixtures ============


@pytest.fixture(name="load_data_format", params=["parquet", "json", "csv"])
def fixture_load_format(request) -> Generator[LoadFormat, None, None]:
    """
    Fixture for creating LoadFormat instances.

    Args:
        request (pytest.FixtureRequest): The fixture request.

    Yields:
        LoadFormat: The LoadFormat object based on the fixture request parameter.
    """
    # Assert
    yield LoadFormat(request.param)


# ============ Tests ===============


@pytest.mark.parametrize("invalid_value", ["invalid", False, True, 0, 1])
def test_load_format_invalid_creation(invalid_value) -> None:
    """
    Assert that creating an enum value from an invalid value raises ValueError.

    Args:
        invalid_value (str): invalid values for LoadOperation.
    """
    with pytest.raises(ValueError):
        # Assert
        LoadFormat(invalid_value)


# ==================================
# ======= LoadMethod class =========
# ==================================

# ============ Fixtures ============


@pytest.fixture(name="load_method", params=["batch", "streaming"])
def fixture_load_method(request) -> Generator[LoadMethod, None, None]:
    """
    Fixture for creating LoadMethod instances.

    Args:
        request (pytest.FixtureRequest): The fixture request.

    Yields:
        LoadMethod: The LoadMethod object based on the fixture request parameter.
    """
    # Assert
    yield LoadMethod(request.param)


# ============ Tests ===============


@pytest.mark.parametrize("invalid_value", ["invalid", False, True, 0, 1])
def test_load_method_invalid_creation(invalid_value) -> None:
    """Assert that creating an enum value from an invalid value raises ValueError.

    Args:
        invalid_value (str): invalid values for LoadOperation.
    """
    with pytest.raises(ValueError):
        # Assert
        LoadMethod(invalid_value)


# ==================================
# ===== LoadOperation class ======
# ==================================

# ============ Fixtures ============


@pytest.fixture(name="load_operation", params=["complete", "append", "update"])
def fixture_load_operation(request) -> Generator[LoadOperation, None, None]:
    """
    Fixture for creating LoadOperation instances.

    Args:
        request (pytest.FixtureRequest): The fixture request.

    Yields:
        LoadOperation: The LoadOperation object based on the fixture request parameter.
    """
    # Assert
    yield LoadOperation(request.param)


# ============ Tests ===============


@pytest.mark.parametrize("invalid_value", ["invalid", False, True, 0, 1])
def test_load_operation_invalid_creation(invalid_value) -> None:
    """Assert that creating an enum value from an invalid value raises ValueError.

    Args:
        invalid_value (str): invalid values for LoadOperation.
    """
    with pytest.raises(ValueError):
        # Assert
        LoadOperation(invalid_value)


# ==================================
# ======= LoadSpec class =========
# ==================================

# ============ Fixtures ============


@pytest.fixture(name="load_options", params=[{}])
def fixture_load_options(request) -> Generator[dict, None, None]:
    """
    Fixture for execution environment options added to load.

    Args:
        request (pytest.FixtureRequest): The fixture request.

    Yields:
        (dict): an dicitonary of key:{option name} with value: {option value}.
    """

    yield request.param


@pytest.fixture(name="load_spec_confeti")
def fixture_load_spec_confeti() -> Generator[dict, None, None]:
    """
    Fixture for load spec confeti file.

    Yields:
        (dict): a valid confeti dictionary of load spec.
    """
    # Arrange
    yield {
        "spec_id": "load_spec_id",
        "method": "batch",
        "data_format": "parquet",
        "operation": "complete",
        "location": "/test.parquet",
        "options": {},
    }


@pytest.fixture(name="load_spec")
def fixture_load_spec(
    tmpdir,
    load_method: LoadMethod,
    load_operation: LoadOperation,
    load_data_format: LoadFormat,
    load_options: dict,
) -> Generator[LoadSpec, None, None]:
    """Fixture for creating a LoadSpec instance.

    Args:
        tmpdir (str): Temporary directory fixture.
        load_method (LoadMethod): LoadMethod fixture.
        load_operation (LoadOperation): LoadOperation fixture.
        load_data_format (LoadFormat): LoadFormat fixture.
        load_options (dict): execution environment options.

    Yields:
        spec (LoadSpec): An instance of LoadSpec for testing.
    """

    location = f"{tmpdir}/test.{load_data_format}"

    if load_method == LoadMethod.STREAMING:
        load_options["checkpointLocation"] = f"{tmpdir}/test/checkpoint"

    yield LoadSpec(
        spec_id="load_spec_id",
        method=load_method.value,
        operation=load_operation.value,
        data_format=load_data_format.value,
        location=location,
        options=load_options,
    )


# ============ Tests ===============


def test_load_spec(load_spec) -> None:
    """
    Assert that all LoadSpec attributes are of correct type.

    Args:
        load_spec (LoadSpec): LoadSpec fixture.
    """
    # Assert
    assert isinstance(load_spec.spec_id, str)
    assert isinstance(load_spec.method, LoadMethod)
    assert isinstance(load_spec.operation, LoadOperation)
    assert isinstance(load_spec.data_format, LoadFormat)
    assert isinstance(load_spec.location, str)
    assert isinstance(load_spec.options, dict)


def test_load_spec_from_confeti(load_spec_confeti: dict) -> None:
    """
    Assert that confeti configuration file instantiates load spec object.
    """
    # Act
    load_spec = LoadSpec.from_confeti(confeti=load_spec_confeti)

    # Assert
    assert load_spec.spec_id == "load_spec_id"
    assert load_spec.method == LoadMethod.BATCH
    assert load_spec.data_format == LoadFormat.PARQUET
    assert load_spec.location == "/test.parquet"
    assert load_spec.options == {}


# ==================================
# ========= Load class ===========
# ==================================

# ============ Tests ===============


def test_load_abc_implementation(load_spec, df_empty) -> None:
    """
    Test implementation of abstract Load class.

    Args:
        load_spec (LoadSpec): LoadSpec fixture.
        df_empty (DataFrame): Test dataframe with schema but without rows fixture.
    """
    # Arrange
    Load.__abstractmethods__ = frozenset()

    # Act
    class MockLoad(Load):
        """Mock implementation of Load class for testing purposes."""

        def load(self):
            """Mock implementation of Load class for testing purposes."""

    # Assert
    assert isinstance(MockLoad(spec=load_spec, dataframe=df_empty), Load)
