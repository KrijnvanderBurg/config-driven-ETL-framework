"""
Cast transform class tests.

========================================================================================
PrimePythonPrinciples © 2024 by Krijn van der Burg is licensed under CC BY-SA 4.0

For inquiries contact Krijn van der Burg at https://www.linkedin.com/in/krijnvanderburg/
========================================================================================
"""

from unittest.mock import Mock, patch

import pytest
from pyspark.sql import functions, types

from stratum.engine.job.transforms.functions.cast import CastFunctionPyspark
from stratum.models.job.transforms.functions.cast_model import CastFunctionModelPyspark, CastFunctionModelPysparkArgs
from stratum.types import RegistrySingleton
from tests.conftest import MAX_UNSIGNED_64_BIT, MIN_SIGNED_64_BIT


class TestCastFunctionPyspark:
    """
    Test class for the `CastTransform` class.
    """

    @pytest.mark.parametrize(
        ("data_type"),
        [
            (types.NullType()),
            #
            (types.CharType(length=10)),  # default
            (types.CharType(length=MIN_SIGNED_64_BIT)),
            (types.CharType(length=0)),
            (types.CharType(length=MAX_UNSIGNED_64_BIT)),
            #
            (types.StringType()),  # default
            #
            (types.VarcharType(length=10)),  # default
            (types.VarcharType(length=MIN_SIGNED_64_BIT)),
            (types.VarcharType(length=0)),
            (types.VarcharType(length=MAX_UNSIGNED_64_BIT)),
            #
            (types.BinaryType()),  # default
            #
            (types.BooleanType()),  # default
            #
            (types.DateType()),  # default
            #
            (types.TimestampType()),  # default
            #
            (types.TimestampNTZType()),  # default
            #
            (types.DecimalType()),
            (types.DecimalType(precision=10, scale=0)),  # default
            (types.DecimalType(precision=MIN_SIGNED_64_BIT, scale=MIN_SIGNED_64_BIT)),
            (types.DecimalType(precision=0, scale=0)),
            (types.DecimalType(precision=MAX_UNSIGNED_64_BIT, scale=MAX_UNSIGNED_64_BIT)),
            #
            (types.DoubleType()),  # default
            #
            (types.FloatType()),  # default
            #
            (types.ByteType()),  # default
            #
            (types.IntegerType()),  # default
            #
            (types.LongType()),  # default
            #
            (types.DayTimeIntervalType()),  # default
            (types.DayTimeIntervalType(startField=0, endField=3)),  # default
            (types.DayTimeIntervalType(startField=0, endField=1)),
            (types.DayTimeIntervalType(startField=0, endField=2)),
            (types.DayTimeIntervalType(startField=1, endField=2)),
            (types.DayTimeIntervalType(startField=1, endField=3)),
            (types.DayTimeIntervalType(startField=2, endField=3)),
            #
            (types.YearMonthIntervalType()),  # default
            (types.DayTimeIntervalType(startField=0, endField=1)),  # default
            #
            (types.Row()),  # default
            #
            (types.ShortType()),  # default
        ],
    )
    def test__transform_function_cast(self, data_type) -> None:
        """
        Test case for the `cast` method of the `CastTransform` class.

        This test asserts that the `cast` method correctly applies the cast transformation
        to the specified column in the DataFrame.
        """
        # Arrange
        cast_args = CastFunctionModelPysparkArgs(column_name="test_column", data_type=data_type)
        cast_model: CastFunctionModelPyspark = CastFunctionModelPyspark(function="cast", arguments=cast_args)
        cast_transform: CastFunctionPyspark = CastFunctionPyspark(model=cast_model)

        dataframe_registry = RegistrySingleton()
        dataframe_registry["dataframe_name"] = Mock()

        col_mock = Mock()

        with patch.object(functions, "col", return_value=col_mock):
            cast_func = cast_transform.transform()
            cast_func(dataframe_registry, "dataframe_name")

            # dataframe_registry["dataframe_name"].withColumn.assert_called_once_with(col_mock)
            col_mock.cast.assert_called_once_with(data_type)
