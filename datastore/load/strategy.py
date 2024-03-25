"""
IO Load Interface and Strategy.

This module provides an abstract strategy class `LoadStrategy` for creating instances of data loads.
The load implementations are located in the `datastore.loads` module.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from abc import ABC

from datastore.load.base import LoadFormat, LoadSpec, LoadStrategy
from datastore.load.file import LoadFile
from pyspark.sql import DataFrame


class LoadContext(ABC):
    """Abstract class representing a strategy context for creating data loads."""

    @classmethod
    def get(cls, spec: LoadSpec, df: DataFrame) -> LoadStrategy:
        """
        Get a load instance based on the load specification via strategy pattern.

        Args:
            spec (LoadSpec): Load specification to write data.
            df (DataFrame): DataFrame to be written.

        Returns:
            Load: An instance of a data load.

        Raises:
            NotImplementedError: If the specified load format is not implemented.
        """
        strategy = {
            LoadFormat.PARQUET: LoadFile(spec=spec, df=df),
            LoadFormat.JSON: LoadFile(spec=spec, df=df),
            LoadFormat.CSV: LoadFile(spec=spec, df=df),
        }

        extract_strategy = LoadFormat(spec.data_format)

        if extract_strategy in strategy.keys():
            return strategy[extract_strategy]

        raise NotImplementedError(f"Load strategy {spec.data_format} is not supported.")
