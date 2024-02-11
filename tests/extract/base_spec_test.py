"""
Extract classes tests.

| ✓ | Tests
|---|-----------------------------------------
| ✓ | Test all attributes are of correct type.
| ✓ | Test extract spec from confeti are of correct type.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from collections.abc import Generator

import pytest
from datastore.extract.base import ExtractFormat, ExtractMethod, ExtractSpec
from pyspark.sql.types import StructType

# ============ Fixtures ============


@pytest.fixture(name="extract_spec")
def fixture_extract_spec(
    tmpdir,
    extract_method: ExtractMethod,
    extract_data_format: ExtractFormat,
    schema: StructType,
    schema_file: str,
) -> ExtractSpec:
    """
    Fixture for creating ExtractSpec from valid value.

    Args:
        tmpdir (str0): Temporary directory fixture.
        extract_method (ExtractMethod): ExtractMethod fixture.
        extract_data_format (ExtractFormat): ExtractFormat fixture.
        schema (StructType): Schema fixture.
        schema_file (str): Schema filepath fixture.

    Returns:
        ExtractFormat: ExtractFormat fixture.
    """
    return ExtractSpec(
        spec_id="bronze-test-extract-dev",
        method=extract_method.value,
        data_format=extract_data_format.value,
        location=f"{tmpdir}/extract.{extract_data_format.value}",
        options={},
        schema=schema.json(),
        schema_filepath=schema_file,
    )


@pytest.fixture(name="extract_spec_matrix")
def fixture_extract_spec_matrix(
    tmpdir,
    extract_method_matrix: ExtractMethod,
    extract_data_format_matrix: ExtractFormat,
    schema: StructType,
    schema_file: str,
) -> Generator[ExtractSpec, None, None]:
    """
    Matrix fixture for creating ExtractSpec from valid value.

    Args:
        tmpdir (str): Temporary directory fixture.
        extract_method_matrix (ExtractMethod): ExtractMethod fixture.
        extract_data_format_matrix (ExtractFormat): ExtractFormat fixture.
        schema (StructType): Schema fixture.
        schema_file (str): Schema filepath fixture.

    Yields:
        ExtractFormat: ExtractFormat fixture.
    """
    yield ExtractSpec(
        spec_id="bronze-test-extract-dev",
        method=extract_method_matrix.value,
        data_format=extract_data_format_matrix.value,
        location=f"{tmpdir}/extract.{extract_data_format_matrix.value}",
        options={},
        schema=schema.json(),
        schema_filepath=schema_file,
    )


@pytest.fixture(name="extract_spec_confeti")
def fixture_extract_spec_confeti(
    extract_method: ExtractMethod,
    extract_data_format: ExtractFormat,
    schema: StructType,
    schema_file: str,
) -> dict:
    """
    Fixture for creating ExtractSpec from confeti.

    Args:
        extract_method (ExtractMethod): ExtractMethod fixture.
        extract_data_format (ExtractFormat): ExtractFormat fixture.
        schema (StructType): Schema fixture.
        schema_file (str): Schema filepath fixture.
    """
    # Arrange
    return {
        "spec_id": "bronze-test-extract-dev",
        "method": extract_method.value,
        "data_format": extract_data_format.value,
        "location": f"/extract.{extract_data_format.value}",
        "options": {},
        "schema": schema.json(),
        "schema_filepath": schema_file,
    }


# ============= Tests ==============


def test_extract_spec_attrb_type(extract_spec: ExtractSpec) -> None:
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


def test_extract_spec_from_confeti(extract_spec_confeti: dict) -> None:
    """
    Assert that ExtractSpec from_confeti method returns valid ExtractSpec.

    Args:
        extract_spec_confeti (dict): ExtractSpec confeti fixture.
    """
    # Act
    spec = ExtractSpec.from_confeti(confeti=extract_spec_confeti)

    # Assert
    assert spec.spec_id == "bronze-test-extract-dev"
    assert isinstance(spec.method, ExtractMethod)
    assert isinstance(spec.data_format, ExtractFormat)
    assert spec.location == f"/extract.{spec.data_format.value}"
    assert spec.options == {}
    assert isinstance(spec.schema, StructType)
