"""
Column transform function.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from abc import ABC

from pyspark.sql import types

from datastore.models.job.transforms.function_spec import FuncAbstract, FuncPyspark
from datastore.utils.json_handler import JsonHandler


class CastAbstract(FuncAbstract, ABC):
    """An abstract base class for column casting functions."""

    class Args(FuncAbstract.ArgsAbstract, ABC):
        """An abstract base class for arguments of column casting functions."""

        class Columns(ABC):
            """An abstract base class for column specification."""

            def __init__(self, name: str, data_type):
                """Initialize Columns with the specified name and data type."""
                self.name: str = name
                self.data_type = data_type

        column_concrete = Columns
        cast_type_mapping: dict
        confeti_schema: dict = {
            "type": "object",
            "properties": {
                "columns": {
                    "type": "object",
                    "additionalProperties": {"type": "string", "enum": ["string", "int", "long"]},
                }
            },
            "required": ["columns"],
        }

        def __init__(self, columns: list):
            """Initialize Args with the specified columns."""
            self.columns: list = columns

        @classmethod
        def from_confeti(cls, confeti: dict):
            """
            Create Args object from a Confeti dictionary.

            Args:
                confeti (dict): The Confeti dictionary.

            Returns:
                Args: The Args object created from the Confeti dictionary.

            Raises:
                NotImplementedError: If the casting data type is not recognized or supported.
            """
            JsonHandler.validate_json(json=confeti, schema=cls.confeti_schema)

            columns_dict: dict = confeti["columns"]
            columns: list = []

            for name, data_type in columns_dict.items():
                if data_type in cls.cast_type_mapping.keys():
                    data_type_concrete = cls.cast_type_mapping[data_type]()
                    column = cls.column_concrete(name=name, data_type=data_type_concrete)
                    columns.append(column)
                else:
                    raise NotImplementedError(f"The casting data type {data_type} is not recognised or supported.")

            return cls(columns=columns)

    args_concrete = Args


class CastPyspark(CastAbstract, FuncPyspark):
    """A concrete implementation of column casting functions using PySpark."""

    class Args(CastAbstract.Args):
        """The arguments for PySpark column casting functions."""

        class Columns(CastAbstract.Args.Columns):
            """The column specification for PySpark column casting functions."""

        column_concrete = Columns
        cast_type_mapping: dict = {
            "string": types.StringType,
            "int": types.IntegerType,
            "long": types.LongType,
        }

    args_concrete = Args
