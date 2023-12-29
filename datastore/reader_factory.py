"""
IO reader interface and factory, reader implementations are in module store.io.reader.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from abc import ABC

from datastore.reader import Reader, ReaderFormat, ReaderSpec
from datastore.readers.parquet_reader import ParquetReader


class ReaderFactory(ABC):
    """Class for reader factory."""

    @classmethod
    def get(cls, spec: ReaderSpec) -> Reader:
        """
        Get data by reader specification using factory pattern.

        Args:
            spec (ReaderSpec): reader specification to read data.

        Returns:
            A dataframe containing the data.
        """
        if spec.reader_format == ReaderFormat.PARQUET.value:
            return ParquetReader(spec=spec)

        raise NotImplementedError(f"The requested reader spec format {spec.reader_format} is not supported.")
