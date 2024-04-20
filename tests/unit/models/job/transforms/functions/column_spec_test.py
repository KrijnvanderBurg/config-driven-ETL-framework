"""
Column cast class tests.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

import jsonschema
import pytest
from pyspark.sql import types

from datastore.models.job.transform_spec import TransformSpecPyspark
from datastore.models.job.transforms.functions.column_spec import CastPyspark
from datastore.utils.json_handler import JsonHandler

# CastPyspark fixtures


@pytest.fixture(name="cast_pyspark")
def fixture__cast__pyspark() -> CastPyspark:
    """
    Matrix fixture for creating LoadSpec from valid value.

    Returns:
        Cast: Cast fixture.
    """
    # Arrange
    columns: list[CastPyspark.Args.Columns] = [
        CastPyspark.Args.Columns(name="age", data_type=types.StringType()),
    ]
    args: CastPyspark.Args = CastPyspark.Args(columns=columns)
    cast: CastPyspark = CastPyspark(function="cast", arguments=args)

    return cast


@pytest.fixture(name="cast_confeti_pyspark")
def fixture__cast__confeti__pyspark() -> dict:
    """
    Fixture for creating ExtractSpec from confeti.

    Returns:
        dict: ExtractSpecPyspark confeti fixture.
    """
    return {"function": "cast", "arguments": {"columns": {"age": "string"}}}


# CastPyspark tests


class TestCastPyspark:
    """
    Test class for TODO
    """

    def test__init(self, cast_pyspark: CastPyspark) -> None:
        """
        TODO

        Args:
            cast_pyspark (Cast): CastPyspark.Args instance fixture.
        """
        # Assert
        assert cast_pyspark.function == "cast"

        assert isinstance(cast_pyspark.arguments, CastPyspark.Args)

        assert cast_pyspark.arguments.columns[0].name == "age"
        assert isinstance(cast_pyspark.arguments.columns[0].data_type, types.StringType)
        assert isinstance(cast_pyspark.arguments.columns[0], CastPyspark.Args.Columns)

    def test__from_confeti(self, cast_confeti_pyspark: dict) -> None:
        """
        Assert that CastPyspark from_confeti method returns valid CastPyspark.

        Args:
            cast_confeti_pyspark (dict): CastPySPark confeti fixture.
        """
        # Act
        cast_pyspark: CastPyspark = CastPyspark.from_confeti(confeti=cast_confeti_pyspark)

        # Assert
        assert cast_pyspark.function == "cast"

        assert isinstance(cast_pyspark.arguments, CastPyspark.Args)

        assert cast_pyspark.arguments.columns[0].name == "age"
        assert isinstance(cast_pyspark.arguments.columns[0].data_type, types.StringType)
        assert isinstance(cast_pyspark.arguments.columns[0], CastPyspark.Args.Columns)

    @pytest.mark.parametrize("missing_property", ["columns"])
    def test__validate_json__missing_required_properties(
        self, missing_property: str, cast_confeti_pyspark: dict
    ) -> None:
        """
        Test case for validating that each required property is missing one at a time.

        Args:
            missing_property (str): The property to be removed for testing.
            cast_confeti_pyspark (dict): Cast confeti fixture.
        """
        with pytest.raises(jsonschema.ValidationError):  # Assert
            # Act
            del cast_confeti_pyspark["arguments"][missing_property]
            JsonHandler.validate_json(cast_confeti_pyspark, TransformSpecPyspark.confeti_schema)
