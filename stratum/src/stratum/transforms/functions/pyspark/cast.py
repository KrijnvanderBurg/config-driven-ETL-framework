"""
Column transform function.


========================================================================================
PrimePythonPrinciples © 2024 by Krijn van der Burg is licensed under CC BY-SA 4.0

For inquiries contact Krijn van der Burg at https://www.linkedin.com/in/krijnvanderburg/
========================================================================================
"""

import json
from abc import ABC
from collections.abc import Callable
from typing import Any, Final, Self

from pyspark.sql import functions, types

from stratum.exceptions import DictKeyError
from stratum.transforms.functions.base import (
    ArgsAbstract,
    ArgsT,
    FunctionAbstract,
    FunctionModelAbstract,
    FunctionModelT,
    FunctionPyspark,
)
from stratum.types import RegistrySingleton

COLUMN_NAME: Final[str] = "column_name"
DATA_TYPE: Final[str] = "data_type"
LENGTH: Final[str] = "length"
PRECISION: Final[str] = "precision"
SCALE: Final[str] = "scale"
START_FIELD: Final[str] = "start_field"
END_FIELD: Final[str] = "end_field"
SCHEMA: Final[str] = "schema"

LENGTH_KW: Final[str] = "length"
PRECISION_KW: Final[str] = "precision"
SCALE_KW: Final[str] = "scale"
START_FIELD_KW: Final[str] = "startField"
END_FIELD_KW: Final[str] = "endField"


class InvalidYearMonthIntervalValue(Exception):
    """Custom exception for invalid year-month interval value."""

    def __init__(self, field_name: str, valid_values: list[str]) -> None:
        self.field_name = field_name
        self.valid_values = valid_values
        super().__init__(f"Invalid `{field_name}` value, valid values are [{', '.join(valid_values)}]")

    def __str__(self) -> str:
        return f"Invalid `{self.field_name}` value, valid values are [{', '.join(self.valid_values)}]"


class CastFunctionModelAbstract(FunctionModelAbstract[ArgsT], ABC):
    """An abstract base class for column casting functions."""

    class Args(ArgsAbstract, ABC):
        """An abstract base class for arguments of column casting functions."""

        def __init__(self, column_name: str, data_type) -> None:
            """Initialize Columns with the modelified name and data type."""
            self.column_name = column_name
            self.data_type = data_type

        @property
        def column_name(self) -> str:
            return self._column_name

        @column_name.setter
        def column_name(self, value: str) -> None:
            self._column_name = value

        @property
        def data_type(self):
            return self._data_type

        @data_type.setter
        def data_type(self, value) -> None:
            self._data_type = value

        data_type_mapping: dict[str, Any]
        day_time_internal_type_mapping: dict[str, int]
        year_month_interval_type_mapping: dict[str, int]


class CastFunctionModelPysparkArgs(CastFunctionModelAbstract.Args):
    """The arguments for PySpark column casting functions."""

    data_type_mapping = {
        "null": types.NullType,
        "char": types.CharType,  # required param length
        "string": types.StringType,
        "varchar": types.VarcharType,  # required param length
        "binary": types.BinaryType,
        "boolean": types.BooleanType,
        "date": types.DateType,
        "timestamp": types.TimestampType,
        "timestamp_ntz": types.TimestampNTZType,
        "decimal": types.DecimalType,  # optional params precision, scale
        "double": types.DoubleType,
        "float": types.FloatType,
        "byte": types.ByteType,
        "integer": types.IntegerType,
        "long": types.LongType,
        "dayTimeInterval": types.DayTimeIntervalType,  # optional params startfield, endfield
        "yearMonthInterval": types.YearMonthIntervalType,  # optional params startfield, endfield
        "row": types.Row,
        "short": types.ShortType,
        "array": types.ArrayType,  # required param elementType, optional param containsNull
        "map": types.MapType,  # required param keyType, valueType; optional param valueContainsNull
        "structField": types.StructField,  # required params name, dataType; optional params nullable, metadata
        "struct": types.StructType,  # optional param fields
    }
    day_time_internal_type_mapping = {"day": 0, "hour": 1, "minute": 2, "second": 3}
    year_month_interval_type_mapping = {"year": 0, "month": 1}

    @staticmethod
    def _process_char(confeti: dict[str, Any]) -> types.CharType:
        length = confeti[LENGTH]

        kwargs = {}
        kwargs[LENGTH_KW] = length

        return types.CharType(**kwargs)

    @staticmethod
    def _process_varchar(confeti: dict[str, Any]) -> types.VarcharType:
        length = confeti[LENGTH]

        kwargs = {}
        kwargs[LENGTH_KW] = length

        return types.VarcharType(**kwargs)

    @staticmethod
    def _process_decimal(confeti: dict[str, int]) -> types.DecimalType:
        kwargs = {}

        if confeti.get(PRECISION, None):
            kwargs[PRECISION_KW] = confeti[PRECISION]

        if confeti.get(SCALE, None):
            kwargs[SCALE_KW] = confeti[SCALE]

        return types.DecimalType(**kwargs)

    @staticmethod
    def _process_day_time_interval(
        confeti: dict[str, Any], day_time_internal_type_mapping: dict[str, int]
    ) -> types.DayTimeIntervalType:
        kwargs = {}

        if confeti.get(START_FIELD, None):
            if confeti[START_FIELD] not in day_time_internal_type_mapping.keys():
                raise ValueError(
                    f"Invalid `start_field` value, available `start_field` values are: "
                    f"{', '.join(day_time_internal_type_mapping.keys())}"
                )

            kwargs[START_FIELD_KW] = day_time_internal_type_mapping[confeti[START_FIELD]]

        if confeti.get(END_FIELD, None):
            if confeti[START_FIELD] not in day_time_internal_type_mapping.keys():
                raise ValueError(
                    f"Invalid `end_field` value, available `end_field` values are: "
                    f"{', '.join(day_time_internal_type_mapping.keys())}"
                )

            kwargs[END_FIELD_KW] = day_time_internal_type_mapping[confeti[START_FIELD]]

        return types.DayTimeIntervalType(**kwargs)

    @staticmethod
    def _process_year_month_interval(
        confeti: dict[str, Any], year_month_interval_type_mapping: dict[str, int]
    ) -> types.YearMonthIntervalType:
        kwargs = {}

        if confeti.get(START_FIELD, None):
            if confeti[START_FIELD] not in year_month_interval_type_mapping.keys():
                raise InvalidYearMonthIntervalValue(
                    field_name=START_FIELD, valid_values=list(year_month_interval_type_mapping.keys())
                )

            kwargs[START_FIELD_KW] = year_month_interval_type_mapping[confeti[START_FIELD]]

        if confeti.get(END_FIELD, None):
            if confeti[END_FIELD] not in year_month_interval_type_mapping.keys():
                raise InvalidYearMonthIntervalValue(
                    field_name=END_FIELD, valid_values=list(year_month_interval_type_mapping.keys())
                )

            kwargs[END_FIELD_KW] = year_month_interval_type_mapping[confeti[END_FIELD]]

        return types.YearMonthIntervalType(**kwargs)

    @staticmethod
    def _process_from_json(
        confeti: dict[str, Any],
        data_type: type[types.ArrayType | types.MapType | types.StructField | types.StructType],
    ) -> types.ArrayType | types.MapType | types.StructField | types.StructType:
        """
        Process the column properties from JSON and convert them to the specified data type.

        Args:
            confeti (dict[str, Any]): The column properties as a dictionary.
            data_type (types.ArrayType | types.MapType | types.StructField | types.StructType):
                The target data type for conversion.

        Returns:
            types.ArrayType | types.MapType | types.StructField | types.StructType:
                The converted data type.
        """
        try:
            schema_parsed = json.loads(s=confeti[SCHEMA])
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti)
        return data_type.fromJson(json=schema_parsed)

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create Args object from a Confeti dictionary.

        Args:
            confeti (dict[str, Any]): The Confeti dictionary.

        Returns:
            Args: The Args object created from the Confeti dictionary.

        Raises:
            ValueError: If the casting data type is not recognized.
            NotImplementedError: If the casting data type is not implemented.
        """
        try:
            column_name = confeti[COLUMN_NAME]
            data_type = confeti[DATA_TYPE]
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti)

        if data_type not in cls.data_type_mapping.keys():
            raise ValueError(
                f"The casting data type {data_type} is not recognised, "
                f"available data types are: {cls.data_type_mapping.keys()}."
            )

        data_type_concrete = cls.data_type_mapping[data_type]

        if data_type_concrete in [
            types.NullType,
            types.StringType,
            types.BinaryType,
            types.BooleanType,
            types.DateType,
            types.TimestampType,
            types.TimestampNTZType,
            types.DoubleType,
            types.FloatType,
            types.ByteType,
            types.IntegerType,
            types.LongType,
            types.Row,
            types.ShortType,
        ]:
            data_type_concrete = data_type_concrete()

        elif data_type_concrete == types.CharType:
            data_type_concrete = cls._process_char(confeti)

        elif data_type_concrete == types.VarcharType:
            data_type_concrete = cls._process_varchar(confeti)

        elif data_type_concrete == types.DecimalType:
            data_type_concrete = cls._process_decimal(confeti)

        elif data_type_concrete == types.DayTimeIntervalType:
            data_type_concrete = cls._process_day_time_interval(
                confeti,
                cls.day_time_internal_type_mapping,
            )

        elif data_type_concrete in [types.YearMonthIntervalType]:
            data_type_concrete = cls._process_year_month_interval(
                confeti,
                cls.year_month_interval_type_mapping,
            )

        elif data_type_concrete in [types.ArrayType, types.MapType, types.StructField, types.StructType]:
            data_type_concrete = cls._process_from_json(confeti, data_type_concrete)

        else:
            raise NotImplementedError

        return cls(column_name=column_name, data_type=data_type_concrete)


class CastFunctionModelPyspark(CastFunctionModelAbstract[CastFunctionModelPysparkArgs]):
    """A concrete implementation of column casting functions using PySpark."""

    args_concrete = CastFunctionModelPysparkArgs


class CastFunctionAbstract(FunctionAbstract[FunctionModelT], ABC):
    """
    Encapsulates column transformation logic for PySpark DataFrames.

    Attributes:
        model (...): The CastModel object containing the casting information.

    Methods:
        from_confeti(confeti: dict[str, Any]) -> Self: Create CastTransform object from json.
        transform() -> Callable: Casts column(s) to new type.
    """


class CastFunctionPyspark(CastFunctionAbstract[CastFunctionModelPyspark], FunctionPyspark):
    """
    Encapsulates column transformation logic for PySpark DataFrames.

    Attributes:
        model (...): The CastModel object containing the casting information.

    Methods:
        from_confeti(confeti: dict[str, Any]) -> Self: Create CastTransform object from json.
        transform() -> Callable: Casts column(s) to new type.
    """

    model_concrete = CastFunctionModelPyspark

    def transform(self) -> Callable:
        """
        Casts column(s) to new type.

        Returns:
            Callable: Function for `DataFrame.transform()`.

        Examples:
            Consider the following DataFrame schema:

            ```
            root
            |-- name: string (nullable = true)
            |-- age: integer (nullable = true)
            ```

            Applying the confeti 'cast' function:

            ```
            {"columns": {"age": "StringType"}}
            ```

            The resulting DataFrame schema will be:

            ```
            root
            |-- name: string (nullable = true)
            |-- age: string (nullable = true)
            ```
        """

        def f(dataframe_registry: RegistrySingleton, dataframe_name: str) -> None:
            dataframe_registry[dataframe_name] = dataframe_registry[dataframe_name].withColumn(
                self.model.arguments.column_name,
                functions.col(self.model.arguments.column_name).cast(self.model.arguments.data_type),
            )

        return f
