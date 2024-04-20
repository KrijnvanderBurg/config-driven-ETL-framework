"""
Data transform module.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from abc import ABC

from datastore.models.job.transforms.function_spec import FuncAbstract
from datastore.models.job.transforms.functions.column_spec import CastPyspark
from datastore.utils.json_handler import JsonHandler


class TransformSpecAbstract(ABC):
    """
    Specification for data transformation.

    Args:
        name (str): The ID of the transformation specification.
        transforms (list): List of transformation functions.
    """

    SUPPORTED_FUNCTIONS: dict
    confeti_schema: dict = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "transforms": {"type": "array"},
        },
        "required": ["name", "transforms"],
    }

    def __init__(self, name: str, transforms: list[FuncAbstract]):
        self.name: str = name
        self.transforms: list[FuncAbstract] = transforms

    @classmethod
    def from_confeti(cls, confeti: dict):
        """
        Get the transformation specifications from a dictionary.

        Args:
            confeti (dict): The dictionary containing the transformation specification.

        Returns:
            TransformSpecAbstract: An instance of the transformation specification.

        Example:
            >>> "transform": {
            >>>     "name": "bronze-test-transform-dev",
            >>>     "transforms": [
            >>>         {"function": "cast", "arguments": {"columns": {"age": "LongType"}}},
            >>>         // etc.
            >>>     ],
            >>> },
        """
        JsonHandler.validate_json(json=confeti, schema=cls.confeti_schema)

        name = confeti["name"]
        transforms: list[FuncAbstract] = []

        for function_confeti in confeti.get("transforms", []):
            function_name: str = function_confeti["function"]

            if function_name in cls.SUPPORTED_FUNCTIONS.keys():
                func_concrete = cls.SUPPORTED_FUNCTIONS[function_name]
                func = func_concrete.from_confeti(confeti=function_confeti)
                transforms.append(func)
                continue

            raise NotImplementedError(f"Function {function_name} is not supported.")

        return cls(name=name, transforms=transforms)


class TransformSpecPyspark(TransformSpecAbstract):
    """
    Specification for PySpark data transformation.

    Examples:
        >>> df = spark.createDataFrame(data=[("Alice", 27), ("Bob", 32),], schema=["name", "age"])
        >>> dict = {"function": "cast", "arguments": {"columns": {"age": "StringType",}}}
        >>> transform = TransformFunction.from_dict(dict=dict)
        >>> df = df.transform(func=transform).printSchema()
        root
        |-- name: string (nullable = true)
        |-- age: string (nullable = true)
    """

    SUPPORTED_FUNCTIONS: dict = {
        "cast": CastPyspark,
    }
