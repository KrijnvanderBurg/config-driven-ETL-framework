# """
# IO Load Interface and Strategy.

# This module provides an abstract strategy class `LoadStrategy` for creating instances of data loads.
# The load implementations are located in the `datastore.loads` module.


# Copyright (c) Krijn van der Burg.

# This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
# Attribution-NonCommercial-NoDerivs 4.0 International License.
# See the accompanying LICENSE file for details,
# or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
# """

# from abc import ABC, abstractmethod

# from pyspark.sql import DataFrame
# from pyspark.sql.streaming import StreamingQuery

# from datastore.models.job.load_spec import LoadFormat, LoadMethod, LoadSpecPyspark


# class LoadStrategy(ABC):
#     """Abstract Load class."""

#     def __init__(self, spec: LoadSpecPyspark, df: DataFrame):
#         """
#         Construct Load instance.

#         Args:
#             spec (LoadSpecPyspark): Load specification for writing data.
#             df (DataFrame): DataFrame to load.
#         """
#         self.spec: LoadSpecPyspark = spec
#         self.df: DataFrame = df

#     @abstractmethod
#     def load(self) -> StreamingQuery | None:
#         """
#         Abstract load method.

#         Raises:
#             NotImplementedError: This method must be implemented by the subclass.
#         """
#         raise NotImplementedError


# class LoadFile(LoadStrategy):
#     """
#     `Load` implementation for files.

#     Args:
#         spec (LoadSpecPyspark): Load specification.
#         df (DataFrame): DataFrame to be written.
#     """

#     def __init__(self, spec: LoadSpecPyspark, df: DataFrame):
#         super().__init__(spec, df)

#     def load(self) -> StreamingQuery | None:
#         """
#         Write data to File.

#         Returns:
#         - For streaming mode (LoadMethod.STREAMING):
#             StreamingQuery: Represents the ongoing query.
#         - For batch mode (LoadMethod.BATCH):
#             None: Indicates successful completion.

#         Raises:
#             NotImplementedError: If the specified load format and type combination is not supported.
#         """
#         if self.spec.method == LoadMethod.BATCH:
#             self._load_batch()
#             return None

#         if self.spec.method == LoadMethod.STREAMING:
#             return self._load_streaming()

#         raise NotImplementedError(
#             f"The load format {self.spec.data_format.value} "
#             f"and type {self.spec.method.value} combination is not supported."
#         )

#     def _load_batch(self) -> None:
#         """
#         Write to file in batch mode.
#         """
#         return self.df.write.save(path=self.spec.location, format=self.spec.data_format.value, **self.spec.options)

#     def _load_streaming(self) -> StreamingQuery:
#         """
#         Write to file in streaming mode.

#         Returns:
#             StreamingQuery: Represents the ongoing streaming query.
#         """

#         return self.df.writeStream.start(
#             path=self.spec.location,
#             format=self.spec.data_format.value,
#             outputMode=self.spec.operation.value,
#             **self.spec.options,
#         )


# class LoadContext(ABC):
#     """Abstract class representing a strategy context for creating data loads."""

#     @classmethod
#     def get(cls, spec: LoadSpecPyspark, df: DataFrame) -> LoadStrategy:
#         """
#         Get a load instance based on the load specification via strategy pattern.

#         Args:
#             spec (LoadSpecPyspark): Load specification to write data.
#             df (DataFrame): DataFrame to be written.

#         Returns:
#             Load: An instance of a data load.

#         Raises:
#             NotImplementedError: If the specified load format is not implemented.
#         """
#         strategy = {
#             LoadFormat.PARQUET: LoadFile,
#             LoadFormat.JSON: LoadFile,
#             LoadFormat.CSV: LoadFile,
#         }

#         extract_strategy = LoadFormat(spec.data_format)

#         if extract_strategy in strategy.keys():
#             return strategy[extract_strategy](spec=spec, df=df)

#         raise NotImplementedError(f"Load strategy {spec.data_format} is not supported.")
