"""
Transform Strategy.

This module provides an abstract strategy class `TransformStrategy` for creating instances of data transforms.
The transform implementations are located in the `datastore.transform` module.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from abc import ABC

from datastore.transform.base import TransformSpec, TransformStrategy
from datastore.transform.basic import TransformBasic
from pyspark.sql import DataFrame


class TransformContext(ABC):
    """Abstract class representing a strategy context for creating data extracts."""

    @classmethod
    def get(cls, spec: TransformSpec, df: DataFrame) -> TransformStrategy:
        """
        Get a Transform instance based on the Transform specification using the strategy pattern.

        Args:
            spec (TransformSpec): Transform specification to transform data.
            df (DataFrame): DataFrame to Transform.

        Returns:
            Transform: An instance of a data Transform.

        Raises:
            NotImplementedError: If the specified transform provider is not supported.
        """
        if True:  # pylint: disable=using-constant-test
            return TransformBasic(spec=spec, df=df)

        raise NotImplementedError("The transform strategy is not supported.")
