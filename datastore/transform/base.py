"""
Data transform module.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass

from datastore.transform.functions.column_cast import ColumnCast
from pyspark.sql import DataFrame

TRANSFORM_FUNCTIONS = {
    "cast": ColumnCast.cast,
}


@dataclass
class TransformFunction:
    """
    Specification for Transform.

    Args:
        function (str): function name to execute.
        arguments (dict): arguments to pass to the function.
    """

    def __init__(self, function: str, arguments: dict):
        # if function not in TRANSFORM_FUNCTIONS:
        #     raise error
        # TODO custom error
        self.function: Callable = TRANSFORM_FUNCTIONS[function](**arguments)

    @classmethod
    def from_confeti(cls, confeti: dict):
        """Get the Transform from confeti.

        Returns:
            TransformFunction: Transform instance.
        """
        return cls(**confeti)


@dataclass
class TransformSpec:
    """
    Transform specification.

    Args:
        spec_id (str): ID of the terminate specification
        transforms: list of `Callable` to execute.
    """

    def __init__(self, spec_id: str, transforms: list[TransformFunction]):
        self.spec_id: str = spec_id
        self.transforms: list[TransformFunction] = transforms

    @classmethod
    def from_confeti(cls, confeti: dict):
        """
        Get the transform specifications from confeti.

        Returns:
            TransformSpec: transform instance.
        """
        transforms: list[TransformFunction] = []
        for transform in confeti.get("transforms", []):
            t: TransformFunction = TransformFunction.from_confeti(confeti=transform)
            transforms.append(t)

        return cls(spec_id=confeti["spec_id"], transforms=transforms)


class TransformStrategy(ABC):
    """Abstract Transform class."""

    def __init__(self, spec: TransformSpec, dataframe: DataFrame):
        """
        Construct Transform instance.

        Args:
            spec (TransformSpec): specification for transforming data.
            dataframe (DataFrame): DataFrame to Transform.
        """
        self.spec: TransformSpec = spec
        self.dataframe: DataFrame = dataframe

    @abstractmethod
    def transform(self) -> DataFrame:
        """
        Abstract Transform method.

        Raises:
            NotImplementedError: This method must be implemented by the subclass.
        """
        raise NotImplementedError
