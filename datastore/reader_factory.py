"""
IO Reader Interface and Factory.

This module provides an abstract factory class `ReaderFactory` for creating instances of data readers.
Reader implementations are located in the `datastore.readers` module.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from abc import ABC

from datastore.reader import READER_FORMAT_FILES, Reader, ReaderSpec
from datastore.readers.reader_file import ReaderFile


class ReaderFactory(ABC):
    """Abstract class representing a factory for creating data readers."""

    @classmethod
    def get(cls, spec: ReaderSpec) -> Reader:
        """
        Get a data reader instance based on the reader specification using the factory pattern.

        Args:
            spec (ReaderSpec): Reader specification to read data.

        Returns:
            Reader: An instance of a data reader.

        Raises:
            NotImplementedError: If the specified reader format is not supported.
        """
        if spec.reader_format in READER_FORMAT_FILES:
            return ReaderFile(spec=spec)

        raise NotImplementedError(f"The requested reader spec format {spec.reader_format.value} is not supported.")
