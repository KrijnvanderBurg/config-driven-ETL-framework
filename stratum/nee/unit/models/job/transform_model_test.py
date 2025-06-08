"""
Transform class tests.


==============================================================================
Copyright Krijn van der Burg. All rights reserved.

This software is proprietary and confidential. No reproduction, distribution,
or transmission is allowed without prior written permission. Unauthorized use,
disclosure, or distribution is strictly prohibited.

For inquiries and permission requests, contact Krijn van der Burg at
krijnvdburg@protonmail.com.
==============================================================================
"""

from typing import Any

import pytest

from stratum.models.job.transform_model import TransformModelAbstract, TransformModelPyspark

# TransformModelPyspark fixtures


@pytest.fixture(name="transform_model_pyspark")
def fixture__transform_model__pyspark() -> TransformModelPyspark:
    """
    Fixture for creating TransformModel from valid value.

    Args:
        cast_pyspark (CastPyspark): CastPyspark instance fixture.

    Returns:
        TransformModelPyspark: TransformModelPyspark fixture.
    """
    return TransformModelPyspark(
        name="transform_name",
        upstream_name=["extract_name"],
    )


@pytest.fixture(name="transform_model_confeti_pyspark")
def fixture__transform_model__confeti__pyspark() -> dict[str, Any]:
    """
    Fixture for creating TransformModel from confeti.

    Returns:
        dict[str, Any]: TransformModelPyspark confeti fixture.
    """

    return {
        "name": "transform_name",
        "upstream_name": "extract_name",
        # "functions": [func_confeti_pyspark],
    }


# TransformModelPyspark tests


class TestTransformModelPyspark:
    """
    Test class for TransformModel class.
    """

    def test__init(self, transform_model_pyspark: TransformModelPyspark) -> None:
        """
        Assert that all TransformModelPyspark attributes are of correct type.

        Args:
            transform_model_pyspark (TransformModelPyspark): TransformModelPyspark instance fixture.
        """
        # Assert
        assert transform_model_pyspark.name == "transform_name"
        # assert isinstance(transform_model_pyspark.functions, list)

    def test__from_confeti(
        self,
        transform_model_pyspark: TransformModelPyspark,
        transform_model_confeti_pyspark: dict,
    ) -> None:
        """
        Assert that TransformModel from_confeti method returns valid TransformModel.

        Args:
            transform_model_pyspark (TransformModelPyspark): TransformModelPyspark instance fixture.
            transform_model_confeti_pyspark (dict[str, Any]): TransformModelPyspark confeti fixture.
        """
        # Act
        transform_model: TransformModelAbstract = TransformModelPyspark.from_confeti(
            confeti=transform_model_confeti_pyspark
        )

        # Assert
        assert transform_model_pyspark.name == transform_model.name

    #     assert isinstance(transform_model.functions, list)
    #     assert all(isinstance(transform, FunctionModelPyspark) for transform in transform_model.functions)

    # @pytest.mark.parametrize(
    #     "fixture_and_expected_type",
    #     [
    #         ("cast_confeti_pyspark", CastFunctionModelPyspark),
    #     ],
    # )
    # def test__from_confeti__factory(self, request: pytest.FixtureRequest, fixture_and_expected_type) -> None:
    #     """
    #     Assert that from_confeti returns correct transform function class.

    #     Args:
    #         request (Any): Pytest request object.
    #         fixture_and_expected_type (Tuple[str, Type]): Tuple containing the fixture name and the expected type.
    #     """
    #     # Arrange

    #     fixture_name = fixture_and_expected_type[0]
    #     expected_type = fixture_and_expected_type[1]

    #     confeti: dict = {
    #         "name": "transform_name",
    #         "upstream_name": "extract_name",
    #         "functions": [request.getfixturevalue(fixture_name)],
    #     }

    #     # Act
    #     transform_model: TransformModelAbstract = TransformModelPyspark.from_confeti(confeti=confeti)

    #     # Assert
    #     assert isinstance(transform_model.functions[0], expected_type)

    # @pytest.mark.parametrize("missing_property", ["name", "functions"])
    # def test__validate_json__missing_required_properties(
    #     self, missing_property: str, transform_model_confeti_pyspark: dict
    # ):
    #     """
    #     Test case for validating that each required property is missing one at a time.

    #     Args:
    #         missing_property (str): Name of the missing property.
    #         transform_model_confeti_pyspark (dict[str, Any]): TransformModelPyspark confeti fixture.
    #     """
    #     with pytest.raises(jsonschema.ValidationError):  # Assert
    #         # Act
    #         del transform_model_confeti_pyspark[missing_property]
    #         JsonHandler.validate_json(transform_model_confeti_pyspark, TransformModelPyspark.confeti_schema)
