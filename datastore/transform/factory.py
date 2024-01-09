"""
Transform factory.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from abc import ABC
from collections.abc import Callable

from datastore.transform.base import TRANSFORM_FUNCTIONS, Transform


class TransformFactory(ABC):
    """Abstract class representing a factory for creating data transformer."""

    @classmethod
    def get(cls, transform: Transform) -> Callable:
        """
        Get a transform function instace based on transform function specification via factory pattern.

        Args:
            spec (TransformFunction): ...
            dataframe (DataFrame): ...

        Returns:
            Transform function to execute.

        Raises:
            NotImplementedError: If the specified transform function is not implemented.
        """
        if transform.function in TRANSFORM_FUNCTIONS:
            return TRANSFORM_FUNCTIONS[transform.function](**transform.arguments)

        raise NotImplementedError(f"The transform function {transform.function} is not implemented.")
