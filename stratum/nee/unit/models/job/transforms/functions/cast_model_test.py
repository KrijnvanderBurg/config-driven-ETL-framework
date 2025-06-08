"""
Column cast class tests.

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
from pyspark.sql import types

from stratum.models.job.transforms.functions.cast_model import CastFunctionModelPyspark

# CastPyspark fixtures


@pytest.fixture(name="cast_pyspark")
def fixture__cast__pyspark() -> CastFunctionModelPyspark:
    """
    Matrix fixture for creating LoadModel from valid value.

    Returns:
        Cast: Cast fixture.
    """
    # Arrange
    args = CastFunctionModelPyspark.Args(column_name="age", data_type=types.StringType())
    cast = CastFunctionModelPyspark(function="cast", arguments=args)

    return cast


@pytest.fixture(name="cast_pyspark_confeti")
def fixture__cast__confeti__pyspark() -> dict[str, Any]:
    """
    Fixture for creating ExtractModel from confeti.

    Returns:
        dict[str, Any]: ExtractModelPyspark confeti fixture.
    """
    return {"function": "cast", "arguments": {"column_name": "age", "data_type": "string"}}


# CastPyspark tests


class TestCastFunctionModelPyspark:
    """
    Test class for TODO
    """

    # def test__from_confeti(self, cast_pyspark_confeti: dict[str, Any]) -> None:
    #     """
    #     Assert that CastPyspark from_confeti method returns valid CastPyspark.

    #     Args:
    #         cast_pyspark_confeti (dict[str, Any]): CastPySPark confeti fixture.
    #     """
    #     # Act
    #     cast_pyspark = CastFunctionModelPyspark.from_confeti(confeti=cast_pyspark_confeti)

    #     # Assert
    #     assert cast_pyspark.function == "cast"

    #     assert isinstance(cast_pyspark.arguments, CastFunctionModelPyspark.Args)

    #     assert cast_pyspark.arguments.columns[0].column_name == "age"
    #     assert isinstance(cast_pyspark.arguments.columns[0].data_type, types.StringType)
    #     assert isinstance(cast_pyspark.arguments.columns[0], CastFunctionModelPyspark.Args.Columns)

    # @pytest.mark.parametrize("missing_property", ["columns"])
    # def test__validate_json__missing_required_properties(
    #     self, missing_property: str, cast_pyspark_confeti: dict
    # ) -> None:
    #     """
    #     Test case for validating that each required property is missing one at a time.

    #     Args:
    #         missing_property (str): The property to be removed for testing.
    #         cast_pyspark_confeti (dict[str, Any]): Cast confeti fixture.
    #     """
    #     with pytest.raises(jsonschema.ValidationError):  # Assert
    #         # Act
    #         del cast_pyspark_confeti["arguments"][missing_property]
    #         JsonHandler.validate_json(cast_pyspark_confeti, TransformModelPyspark.confeti_schema)

    # def test__validate_json__additional_properties(self, cast_pyspark_confeti: dict[str, Any]) -> None:
    #     """
    #     Test case for validating that no additional properties are allowed.

    #     Args:
    #         cast_pyspark_confeti (dict[str, Any]): Cast confeti fixture.
    #     """
    #     with pytest.raises(jsonschema.ValidationError):  # Asse1rt
    #         # Act
    #         cast_pyspark_confeti["arguments"]["additional_property"] = "additional_property"
    #         JsonHandler.validate_json(cast_pyspark_confeti, TransformModelPyspark.confeti_schema)

    @pytest.mark.parametrize(
        "mapping_expected_type",
        [
            ("null", types.NullType, {}),
            ("char", types.CharType, {"length": 10}),
            ("string", types.StringType, {}),
            ("varchar", types.VarcharType, {"length": 10}),
            ("binary", types.BinaryType, {}),
            ("boolean", types.BooleanType, {}),
            ("date", types.DateType, {}),
            ("timestamp", types.TimestampType, {}),
            ("timestamp_ntz", types.TimestampNTZType, {}),
            ("decimal", types.DecimalType, {"precision": 10, "scale": 0}),
            ("double", types.DoubleType, {}),
            ("float", types.FloatType, {}),
            ("byte", types.ByteType, {}),
            ("integer", types.IntegerType, {}),
            ("long", types.LongType, {}),
            ("dayTimeInterval", types.DayTimeIntervalType, {"start_field": "day", "end_field": "second"}),
            ("yearMonthInterval", types.YearMonthIntervalType, {"start_field": "year", "end_field": "month"}),
            ("row", types.Row, {}),
            ("short", types.ShortType, {}),
            (
                "array",
                types.ArrayType,
                {"schema": types.ArrayType(elementType=types.StringType()).json()},
            ),
            (
                "map",
                types.MapType,
                {"schema": types.MapType(keyType=types.StringType(), valueType=types.StringType()).json()},
            ),
            (
                "structField",
                types.StructField,
                {"schema": types.StructField("column_name", types.StringType(), True).json()},
            ),
            (
                "struct",
                types.StructType,
                {"schema": types.StructType([types.StructField("column_name", types.StringType(), True)]).json()},
            ),
        ],
    )
    def test__from_confeti(self, mapping_expected_type: tuple) -> None:
        """check if string returns correct types mapping, this is just the model so it doesnt instantiate it yet.
        Aka not instanced yet so length isnt checked, thats in cast itself."""

        # Arrange
        cast_str, cast_type, kwargs = mapping_expected_type

        # Act
        confeti: dict = {
            "function": "cast",
            "arguments": {
                "column_name": "test_column_name",
                "data_type": cast_str,
                "length": kwargs.get("length", None),
                "precision": kwargs.get("precision", None),
                "scale": kwargs.get("scale", None),
                "start_field": kwargs.get("start_field", None),
                "end_field": kwargs.get("end_field", None),
                "schema": kwargs.get("schema", None),
            },
        }

        # Act
        cast = CastFunctionModelPyspark.from_confeti(confeti=confeti)

        assert cast.function == "cast"
        assert isinstance(cast.arguments, CastFunctionModelPyspark.Args)

        assert cast.arguments.column_name == "test_column_name"
        assert isinstance(cast.arguments.data_type, cast_type)

    @pytest.mark.parametrize(
        "mapping",
        ["invalid", "int", "StringType"],
    )
    def test__cast_mapping__unsupported(self, mapping) -> None:
        """test if unsupported mapping raises error"""
        # Arrange
        confeti: dict = {
            "function": "cast",
            "arguments": {"column_name": "age", "data_type": mapping},
        }

        with pytest.raises(ValueError):  # Assert
            # Act
            CastFunctionModelPyspark.from_confeti(confeti)

    def test__cast_mapping__tripwire(self) -> None:
        """
        Tripwire test fails if the list of datatypes in pyspark.sql.types changes.
        The goal is to notify if a datatype is added, removed, or changed. Then, update the CastModel if necessary.
        List as per 18-06-2024.
        """
        # Assert
        assert types.__all__ == [
            "DataType",
            "NullType",
            "CharType",
            "StringType",
            "VarcharType",
            "BinaryType",
            "BooleanType",
            "DateType",
            "TimestampType",
            "TimestampNTZType",
            "DecimalType",
            "DoubleType",
            "FloatType",
            "ByteType",
            "IntegerType",
            "LongType",
            "DayTimeIntervalType",
            "YearMonthIntervalType",
            "Row",
            "ShortType",
            "ArrayType",
            "MapType",
            "StructField",
            "StructType",
        ]
