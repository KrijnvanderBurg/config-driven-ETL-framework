"""
This module provides a `ExtractFile` class for extracting data from File files.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from datastore.extract.base import Extract, ExtractMethod, ExtractSpec
from datastore.spark import Spark
from pyspark.sql import DataFrame


class ExtractFile(Extract):
    """Extract implementation for File files."""

    def __init__(self, spec: ExtractSpec):
        """
        Construct ExtractFile instance.

        Args:
            spec (ExtractSpec): Input specification.
        """
        super().__init__(spec)

    def extract(self) -> DataFrame:
        """
        Read File data.

        Returns:
            DataFrame: A dataframe containing the data from the files.
        """
        if self.spec.method == ExtractMethod.BATCH:
            return self._extract_batch()

        if self.spec.method == ExtractMethod.STREAMING:
            return self._extract_streaming()

        raise NotImplementedError(
            f"The extract format {self.spec.data_format.value} "
            f"and type {self.spec.method.value} combination is not supported."
        )

    def _extract_batch(self) -> DataFrame:
        """
        Read from file in batch mode.

        Returns:
            DataFrame: A dataframe containing the data from batch extracting files.
        """
        return Spark.session.read.load(
            path=self.spec.location,
            format=self.spec.data_format.value,
            schema=self.spec.schema,
            **self.spec.options,
        )

    def _extract_streaming(self) -> DataFrame:
        """
        Read from file in streaming mode.

        Returns:
            DataFrame: A dataframe containing the data from streaming extracting files.
        """
        return Spark.session.readStream.load(
            path=self.spec.location,
            format=self.spec.data_format.value,
            schema=self.spec.schema,
            **self.spec.options,
        )
