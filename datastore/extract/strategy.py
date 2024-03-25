"""
IO Extract Interface and Strategy.

This module provides an abstract strategy class `ExtractStrategy` for creating instances of data extracts.
Extract implementations are located in the `datastore.extract` module.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from abc import ABC

from datastore.extract.base import ExtractFormat, ExtractSpec, ExtractStrategy
from datastore.extract.file import ExtractFile


class ExtractContext(ABC):
    """Abstract class representing a strategy context for creating data extracts."""

    @classmethod
    def get(cls, spec: ExtractSpec) -> ExtractStrategy:
        """
        Get an extract instance based on the extract specification using the strategy pattern.

        Args:
            spec (ExtractSpec): Extract specification to extract data.

        Returns:
            Extract: An instance of a data extract.

        Raises:
            NotImplementedError: If the specified extract format is not supported.
        """
        strategy = {
            ExtractFormat.PARQUET: ExtractFile(spec=spec),
            ExtractFormat.JSON: ExtractFile(spec=spec),
            ExtractFormat.CSV: ExtractFile(spec=spec),
        }

        extract_strategy = ExtractFormat(spec.data_format)

        if extract_strategy in strategy.keys():
            return strategy[extract_strategy]

        raise NotImplementedError(f"Extract strategy {spec.data_format.value} is not supported.")
