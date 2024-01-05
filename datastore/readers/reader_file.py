"""
This module provides a `ReaderFile` class for reading data from File files.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from datastore.reader import Reader, ReaderSpec, ReaderType
from datastore.spark import Spark
from pyspark.sql import DataFrame


class ReaderFile(Reader):
    """Reader implementation for File files."""

    def __init__(self, spec: ReaderSpec):
        """
        Construct ReaderFile instance.

        Args:
            spec (ReaderSpec): Input specification.
        """
        super().__init__(spec)

    def read(self) -> DataFrame:
        """
        Read File data.

        Returns:
            DataFrame: A dataframe containing the data from the files.
        """
        if self.spec.reader_type == ReaderType.BATCH:
            return self._read_batch()

        if self.spec.reader_type == ReaderType.STREAMING:
            return self._read_streaming()

        raise NotImplementedError(
            f"The read format {self.spec.reader_format.value} "
            f"and type {self.spec.reader_type.value} combination is not supported."
        )

    def _read_batch(self) -> DataFrame:
        """
        Read from file in batch mode.

        Returns:
            DataFrame: A dataframe containing the data from batch reading files.
        """
        return Spark.session.read.load(
            path=self.spec.location,
            format=self.spec.reader_format.value,
            schema=self.spec.schema,
            **self.spec.options,
        )

    def _read_streaming(self) -> DataFrame:
        """
        Read from file in streaming mode.

        Returns:
            DataFrame: A dataframe containing the data from streaming reading files.
        """
        return Spark.session.readStream.load(
            path=self.spec.location,
            format=self.spec.reader_format.value,
            schema=self.spec.schema,
            **self.spec.options,
        )
