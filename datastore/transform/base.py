"""
Data transform module.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""
from dataclasses import dataclass

from datastore.transform.column_cast import ColumnCast


@dataclass
class Transform:
    """
    Specification for Transform.

    Args:
        function (str): function name to execute.
        arguments (dict): arguments to pass to the function.
    """

    def __init__(self, function: str, arguments: dict):
        self.function: str = function
        self.arguments: dict = arguments

    @classmethod
    def from_confeti(cls, confeti: dict):
        """Get the Transform from confeti.

        Returns:
            TransformFunction: Transform instance.
        """
        return cls(**confeti)


TRANSFORM_FUNCTIONS = {
    "cast": ColumnCast.cast,
}


@dataclass
class TransformSpec:
    """Transform specification."""

    def __init__(self, spec_id: str, transforms: list[Transform]):
        """
        TransformSpec

        Args:
            spec_id (str): ID of the terminate specification
            transforms: list of `TransformFunction` to execute.
        """
        self.spec_id: str = spec_id
        self.transforms: list[Transform] = transforms

    @classmethod
    def from_confeti(cls, confeti: dict):
        """`
        Get the transform specifications from confeti.

        Returns:
            TransformSpec: transform instance.
        """

        transforms: list[Transform] = []
        for transform in confeti.get("transforms", []):
            transforms.append(Transform.from_confeti(confeti=transform))

        return cls(spec_id=confeti["spec_id"], transforms=transforms)
