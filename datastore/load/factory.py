"""
IO Load Interface and Factory.

This module provides an abstract factory class `LoadFactory` for creating instances of data loads.
The load implementations are located in the `datastore.loads` module.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from abc import ABC

from datastore.load.base import LOAD_FILES_FORMAT, Load, LoadSpec
from datastore.load.file import LoadFile
from pyspark.sql import DataFrame


class LoadFactory(ABC):
    """Abstract class representing a factory for creating data loads."""

    @classmethod
    def get(cls, spec: LoadSpec, dataframe: DataFrame) -> Load:
        """
        Get a data load instance based on the load specification via factory pattern.

        Args:
            spec (LoadSpec): Load specification to write data.
            dataframe (DataFrame): DataFrame to be written.

        Returns:
            Load: An instance of a data load.

        Raises:
            NotImplementedError: If the specified load format is not implemented.
        """
        if spec.data_format in LOAD_FILES_FORMAT:
            return LoadFile(spec=spec, dataframe=dataframe)

        raise NotImplementedError(f"The load spec format {spec.data_format} is not implemented.")
