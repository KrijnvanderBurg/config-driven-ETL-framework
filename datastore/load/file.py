"""
This module provides a `LoadFile` class for writing data to File files.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from datastore.load.base import LoadMethod, LoadSpec, LoadStrategy
from pyspark.sql import DataFrame
from pyspark.sql.streaming.query import StreamingQuery


class LoadFile(LoadStrategy):
    """
    `Load` implementation for files.

    Args:
        spec (LoadSpec): Load specification.
        dataframe (DataFrame): DataFrame to be written.
    """

    def __init__(self, spec: LoadSpec, dataframe: DataFrame):
        super().__init__(spec, dataframe)

    def load(self) -> StreamingQuery | None:
        """
        Write data to File.

        Returns:
        - For streaming mode (LoadMethod.STREAMING):
            StreamingQuery: Represents the ongoing query.
        - For batch mode (LoadMethod.BATCH):
            None: Indicates successful completion.

        Raises:
            NotImplementedError: If the specified load format and type combination is not supported.
        """
        if self.spec.method == LoadMethod.BATCH:
            self._load_batch()
            return None

        if self.spec.method == LoadMethod.STREAMING:
            return self._load_streaming()

        raise NotImplementedError(
            f"The load format {self.spec.data_format.value} "
            f"and type {self.spec.method.value} combination is not supported."
        )

    def _load_batch(self) -> None:
        """
        Write to file in batch mode.
        """
        return self.dataframe.write.save(
            path=self.spec.location, format=self.spec.data_format.value, **self.spec.options
        )

    def _load_streaming(self) -> StreamingQuery:
        """
        Write to file in streaming mode.

        Returns:
            StreamingQuery: Represents the ongoing streaming query.
        """

        return self.dataframe.writeStream.start(
            path=self.spec.location,
            format=self.spec.data_format.value,
            outputMode=self.spec.operation.value,
            **self.spec.options,
        )
