"""Module to define behaviour to read from files."""
from pyspark.sql import DataFrame

from datastore.spark import Spark
from datastore.reader import ReaderSpec, ReaderType
from datastore.reader import Reader


class ParquetReader(Reader):
    """Read parquet files."""

    def __init__(self, spec : ReaderSpec):
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
        if self._spec.read_type == ReaderType.BATCH.value:
            df = Spark.session.read.load(
                path=self._spec.location,
                format=self._spec.data_format,
            )

            return df
        
        elif self._spec.read_type == ReaderType.STREAMING.value:
            df = Spark.session.readStream.load(
                path=self._spec.location,
                format=self._spec.data_format,
            )

            return df
        
        else:
            raise NotImplementedError(f"The read format {self._spec.reader_format} and type {self._spec.reader_type} combination is not supported.")
