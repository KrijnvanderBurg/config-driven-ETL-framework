"""
IO Writer Interface and Factory.

This module provides an abstract factory class `WriterFactory` for creating instances of data writers.
The writer implementations are located in the `datastore.writers` module.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from abc import ABC

from datastore.writer import WRITER_FORMAT_FILES, Writer, WriterSpec
from datastore.writers.writer_file import WriterFile
from pyspark.sql import DataFrame


class WriterFactory(ABC):
    """Abstract class representing a factory for creating data writers."""

    @classmethod
    def get(cls, spec: WriterSpec, dataframe: DataFrame) -> Writer:
        """
        Get a data writer instance based on the writer specification via factory pattern.

        Args:
            spec (WriterSpec): Writer specification to write data.
            dataframe (DataFrame): DataFrame to be written.

        Returns:
            Writer: An instance of a data writer.
        Raises:
            NotImplementedError: If the specified writer format is not implemented.
        """
        if spec.writer_format in WRITER_FORMAT_FILES:
            return WriterFile(spec=spec, dataframe=dataframe)

        raise NotImplementedError(f"The writer spec format {spec.writer_format} is not implemented.")
