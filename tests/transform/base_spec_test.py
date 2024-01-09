"""
TransformSpec class tests

| ✓ | Tests
|---|-----------------------------------------
| ✓ | Test all attributes are of correct type.
| ✓ | Test all attributes are of correct value.
| ✓ | Create instance from confeti.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

import pytest
from datastore.transform.base import Transform, TransformSpec

# ============ Fixtures ============


@pytest.fixture(name="transform_spec")
def fixture_transform_spec() -> TransformSpec:
    """
    Fixture for creating a LoadSpec instance.

    Returns:
        (TransformSpec): TransformSpec fixture.
    """

    transform = Transform(function="cast", arguments={"cols": {"age": "LongType"}})
    spec = TransformSpec(spec_id="bronze-test-transform-dev", transforms=[transform])
    return spec


@pytest.fixture(
    name="transform_spec_matrix",
    params=["transform_cast"],
)
def fixture_transform_spec_matrix(fixture_transform_name, request) -> TransformSpec:
    """
    Matrix fixture for creating a LoadSpec instance.

    Returns:
        (TransformSpec): TransformSpec fixture.
    """

    transform = request.getfixturevalue(fixture_transform_name)

    spec = TransformSpec(spec_id="bronze-test-transform-dev", transforms=[transform])
    return spec


@pytest.fixture(name="transform_spec_confeti")
def test_transform_spec_confeti() -> dict:
    """
    Fixture for creating Transform from confeti.
    """
    # Arrange
    return {
        "spec_id": "bronze-test-transform-dev",
        "functions": [
            {"function": "cast", "arguments": {"cols": {"age": "LongType"}}},
        ],
    }


# ============ Tests ===============


def test_transform_spec_attrb_types(transform_spec: TransformSpec) -> None:
    """
    Assert that all TransformSpec attributes are of correct type and value.

    Args:
        transform_spec (TransformSpec): TransformSpec fixture.
    """
    # Assert
    assert isinstance(transform_spec.spec_id, str)
    assert transform_spec.spec_id == "bronze-test-transform-dev"

    for transform in transform_spec.transforms:
        assert isinstance(transform, Transform)


def test_transform_spec_from_confeti(transform_spec_confeti: dict) -> None:
    """
    Assert that TransformSpec from_confeti method returns valid TransformSpec.

    Args:
        transform_spec_confeti (dict): Transform confeti fixture.
    """

    # Act
    spec = TransformSpec.from_confeti(confeti=transform_spec_confeti)

    # Assert
    assert spec.spec_id == "bronze-test-transform-dev"
    assert isinstance(spec, TransformSpec)
