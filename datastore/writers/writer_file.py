"""
This module provides a `WriterFile` class for writing data to File files.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from datastore.writer import Writer, WriterSpec, WriterType
from pyspark.sql import DataFrame
from pyspark.sql.streaming.query import StreamingQuery


class WriterFile(Writer):
    """
    `Writer` implementation for File files.
    """

    def __init__(self, spec: WriterSpec, dataframe: DataFrame):
        """
        Construct WriterFile instance.

        Args:
            spec (WriterSpec): Writer specification.
            dataframe (DataFrame): DataFrame to be written.
        """
        super().__init__(spec, dataframe)

    def write(self) -> StreamingQuery | None:
        """
        Write data to File.

        Returns:
        - For streaming mode (WriterType.STREAMING):
            StreamingQuery: Represents the ongoing query.
        - For batch mode (WriterType.BATCH):
            None: Indicates successful completion.

        Raises:
            NotImplementedError: If the specified writer format and type combination is not supported.
        """
        if self.spec.writer_type == WriterType.BATCH:
            self._write_batch()
            return None

        if self.spec.writer_type == WriterType.STREAMING:
            return self._write_streaming()

        raise NotImplementedError(
            f"The writer format {self.spec.writer_format.value} "
            f"and type {self.spec.writer_type.value} combination is not supported."
        )

    def _write_batch(self) -> None:
        """
        Write to file in batch mode.
        """
        return self.dataframe.write.save(
            path=self.spec.location, format=self.spec.writer_format.value, **self.spec.options
        )

    def _write_streaming(self) -> StreamingQuery:
        """
        Write to file in streaming mode.

        Returns:
            StreamingQuery: Represents the ongoing streaming query.
        """

        return self.dataframe.writeStream.start(
            path=self.spec.location,
            format=self.spec.writer_format.value,
            outputMode=self.spec.writer_operation.value,
            **self.spec.options,
        )
