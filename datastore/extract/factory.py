"""
IO Extract Interface and Factory.

This module provides an abstract factory class `ExtractFactory` for creating instances of data extracts.
Extract implementations are located in the `datastore.extracts` module.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from abc import ABC

from datastore.extract.base import READER_FORMAT_FILES, Extract, ExtractSpec
from datastore.extract.file import ExtractFile


class ExtractFactory(ABC):
    """Abstract class representing a factory for creating data extracts."""

    @classmethod
    def get(cls, spec: ExtractSpec) -> Extract:
        """
        Get a data extract instance based on the extract specification using the factory pattern.

        Args:
            spec (ExtractSpec): Extract specification to extract data.

        Returns:
            Extract: An instance of a data extract.

        Raises:
            NotImplementedError: If the specified extract format is not supported.
        """
        if spec.data_format in READER_FORMAT_FILES:
            return ExtractFile(spec=spec)

        raise NotImplementedError(f"The requested extract spec format {spec.data_format.value} is not supported.")
