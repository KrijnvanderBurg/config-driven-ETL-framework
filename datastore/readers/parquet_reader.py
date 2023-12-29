"""
Module to define behaviour to read from files.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from datastore.reader import Reader, ReaderSpec, ReaderType
from datastore.spark import Spark
from pyspark.sql import DataFrame


class ParquetReader(Reader):
    """Read parquet files."""

    def __init__(self, spec: ReaderSpec):
        """
        Construct ParquetReader instance.

        Args:
            reader_spec: input specification.
        """
        super().__init__(spec)

    def read(self) -> DataFrame:
        """
        Read parquet data.

        Returns:
            A dataframe containing the data from the files.
        """
        if self._spec.reader_type == ReaderType.BATCH.value:
            df = Spark.session.read.load(
                path=self._spec.location,
                format=self._spec.reader_format,
            )

            return df

        if self._spec.reader_type == ReaderType.STREAMING.value:
            df = Spark.session.readStream.load(
                path=self._spec.location,
                format=self._spec.reader_format,
            )

            return df

        raise NotImplementedError(
            f"The read format {self._spec.reader_format} "
            f"and type {self._spec.reader_type} combination is not supported."
        )
