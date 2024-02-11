"""
Load classes tests.

| ✓ | Tests
|---|-----------------------------------------
| ✓ | Test all attributes are of correct type.
| ✓ | Test load spec from confeti are of correct type.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from collections.abc import Generator

import pytest
from datastore.load.base import LoadFormat, LoadMethod, LoadOperation, LoadSpec

# ============ Fixtures ============


@pytest.fixture(name="load_spec")
def fixture_load_spec(
    tmpdir,
    load_method: LoadMethod,
    load_operation: LoadOperation,
    load_format: LoadFormat,
) -> LoadSpec:
    """
    Matrix fixture for creating LoadSpec from valid value.

    Args:
        tmpdir (str): Temporary directory fixture.
        load_method (LoadMethod): LoadMethod fixture.
        load_operation (LoadOperation): LoadOperation fixture.
        load_format (LoadFormat): LoadFormat fixture.

    Returns:
        LoadFormat: LoadFormat fixture.
    """
    return LoadSpec(
        spec_id="bronze-test-load-dev",
        method=load_method.value,
        operation=load_operation.value,
        data_format=load_format.value,
        location=f"{tmpdir}/load.{load_format.value}",
        options={},
    )


@pytest.fixture(name="load_spec_matrix")
def fixture_load_spec_matrix(
    tmpdir,
    load_method_matrix: LoadMethod,
    load_operation_matrix: LoadOperation,
    load_format_matrix: LoadFormat,
) -> Generator[LoadSpec, None, None]:
    """
    Fixture for creating LoadSpec from valid value.

    Args:
        tmpdir (str): Temporary directory fixture.
        load_method_matrix (LoadMethod): LoadMethod fixture.
        load_operation_matrix (LoadOperation): LoadOperation fixture.
        load_format_matrix (LoadFormat): LoadFormat fixture.

    Yields:
        LoadFormat: LoadFormat fixture.
    """
    yield LoadSpec(
        spec_id="bronze-test-load-dev",
        method=load_method_matrix.value,
        operation=load_operation_matrix.value,
        data_format=load_format_matrix.value,
        location=f"{tmpdir}/load.{load_format_matrix.value}",
        options={},
    )


@pytest.fixture(name="load_spec_confeti")
def fixture_load_spec_confeti(
    load_method: LoadMethod,
    load_operation: LoadOperation,
    load_format: LoadFormat,
) -> dict:
    """
    Assert that LoadSpec from_confeti method returns valid LoadSpec.

    Args:
        load_method (LoadMethod): LoadMethod fixture.
        load_operation (LoadOperation): LoadOperation fixture.
        load_format (LoadFormat): LoadFormat fixture.
    """
    # Arrange
    return {
        "spec_id": "bronze-test-load-dev",
        "method": load_method.value,
        "operation": load_operation.value,
        "data_format": load_format.value,
        "location": f"/load.{load_format.value}",
        "options": {},
    }


# ============= Tests ==============


def test_load_spec_attrb_type(load_spec: LoadSpec) -> None:
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
    Assert that LoadSpec from_confeti method returns valid LoadSpec.

    Args:
        load_spec_confeti (dict): LoadSpec confeti fixture.
    """
    # Act
    spec = LoadSpec.from_confeti(confeti=load_spec_confeti)

    # Assert
    assert spec.spec_id == "bronze-test-load-dev"
    assert isinstance(spec.method, LoadMethod)
    assert isinstance(spec.operation, LoadOperation)
    assert isinstance(spec.data_format, LoadFormat)
    assert spec.location == f"/load.{spec.data_format.value}"
    assert spec.options == {}
