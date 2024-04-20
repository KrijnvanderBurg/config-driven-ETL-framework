"""
Transform class tests.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

import jsonschema
import pytest

from datastore.models.job.transform_spec import TransformSpecAbstract, TransformSpecPyspark
from datastore.models.job.transforms.function_spec import FuncPyspark
from datastore.utils.json_handler import JsonHandler
from tests.unit.models.job.transforms.functions.column_spec_test import CastPyspark

# TransformSpecPyspark fixtures


@pytest.fixture(name="transform_spec_pyspark")
def fixture__transform_spec__pyspark(cast_pyspark: CastPyspark) -> TransformSpecPyspark:
    """
    Fixture for creating TransformSpec from valid value.

    Args:
        cast_pyspark (CastPyspark): CastPyspark instance fixture.

    Returns:
        TransformSpecPyspark: TransformSpecPyspark fixture.
    """
    return TransformSpecPyspark(
        name="bronze-test-transform-dev",
        transforms=[cast_pyspark],
    )


@pytest.fixture(name="transform_spec_confeti_pyspark")
def fixture__transform_spec__confeti__pyspark(cast_confeti_pyspark: dict) -> dict:
    """
    Fixture for creating TransformSpec from confeti.

    Args:
        cast_confeti_pyspark (dict): CastConfetiPyspark instance fixture.

    Returns:
        dict: TransformSpecPyspark confeti fixture.
    """

    return {
        "name": "bronze-test-transform-dev",
        "transforms": [cast_confeti_pyspark],
    }


# TransformSpecPyspark tests


class TestTransformSpecPyspark:
    """
    Test class for TransformSpec class.
    """

    def test__init(self, transform_spec_pyspark: TransformSpecPyspark) -> None:
        """
        Assert that all TransformSpecPyspark attributes are of correct type.

        Args:
            transform_spec_pyspark (TransformSpecPyspark): TransformSpecPyspark instance fixture.
        """
        # Assert
        assert transform_spec_pyspark.name == "bronze-test-transform-dev"
        assert isinstance(transform_spec_pyspark.transforms, list)

    def test__from_confeti(
        self, transform_spec_pyspark: TransformSpecPyspark, transform_spec_confeti_pyspark: dict
    ) -> None:
        """
        Assert that TransformSpec from_confeti method returns valid TransformSpec.

        Args:
            transform_spec_pyspark (TransformSpecPyspark): TransformSpecPyspark instance fixture.
            transform_spec_confeti_pyspark (dict): TransformSpecPyspark confeti fixture.
        """
        # Act
        transform_spec: TransformSpecAbstract = TransformSpecPyspark.from_confeti(
            confeti=transform_spec_confeti_pyspark
        )

        # Assert
        assert transform_spec_pyspark.name == transform_spec.name

        assert isinstance(transform_spec.transforms, list)
        assert all(isinstance(transform, FuncPyspark) for transform in transform_spec.transforms)

    @pytest.mark.parametrize(
        "fixture_and_expected_type",
        [
            ("cast_confeti_pyspark", CastPyspark),
        ],
    )
    def test__from_confeti__factory(self, request: pytest.FixtureRequest, fixture_and_expected_type) -> None:
        """
        Assert that from_confeti returns correct transform function class.

        Args:
            request (Any): Pytest request object.
            fixture_and_expected_type (Tuple[str, Type]): Tuple containing the fixture name and the expected type.
        """
        # Arrange

        fixture_name = fixture_and_expected_type[0]
        expected_type = fixture_and_expected_type[1]

        confeti: dict = {
            "name": "bronze-test-transform-dev",
            "transforms": [request.getfixturevalue(fixture_name)],
        }

        # Act
        transform_spec: TransformSpecAbstract = TransformSpecPyspark.from_confeti(confeti=confeti)

        # Assert
        assert isinstance(transform_spec.transforms[0], expected_type)

    @pytest.mark.parametrize("missing_property", ["name", "transforms"])
    def test__validate_json__missing_required_properties(
        self, missing_property: str, transform_spec_confeti_pyspark: dict
    ):
        """
        Test case for validating that each required property is missing one at a time.

        Args:
            missing_property (str): Name of the missing property.
            transform_spec_confeti_pyspark (dict): TransformSpecPyspark confeti fixture.
        """
        with pytest.raises(jsonschema.ValidationError):  # Assert
            # Act
            del transform_spec_confeti_pyspark[missing_property]
            JsonHandler.validate_json(transform_spec_confeti_pyspark, TransformSpecPyspark.confeti_schema)
