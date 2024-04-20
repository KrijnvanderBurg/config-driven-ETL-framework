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

# from datastore.models.job.extract_spec import ExtractFormat, ExtractMethod, ExtractSpecAbstract
# from datastore.engine.bridge.pyspark.utils.spark_handler import SparkHandler


# class AbstractExtract(ABC):
#     """Extract abstract class."""

#     def __init__(self, spec: ExtractSpecAbstract):
#         """
#         Construct extract instance.

#         Args:
#             spec (ExtractSpecAbstract): extract specification for extracting data.
#         """
#         self.spec: ExtractSpecAbstract = spec

#     @abstractmethod
#     def extract(self) -> DataFrame:
#         """
#         Abstract extract method.

#         Returns:
#             A df extract according to the extract specification.
#         """
#         raise NotImplementedError


# class ExtractFile(AbstractExtract):
#     """
#     Extract implementation for files.
#     """

#     def extract(self) -> DataFrame:
#         """
#         Read File data.

#         Returns:
#             DataFrame: A df containing the data from the files.
#         """
#         if self.spec.method == ExtractMethod.BATCH:
#             return self._extract_batch()

#         if self.spec.method == ExtractMethod.STREAMING:
#             return self._extract_streaming()

#         raise NotImplementedError(
#             f"The extract format {self.spec.data_format.value} "
#             f"and type {self.spec.method.value} combination is not supported."
#         )

#     def _extract_batch(self) -> DataFrame:
#         """
#         Read from file in batch mode.

#         Returns:
#             DataFrame: A df containing the data from batch extracting files.
#         """
#         return SparkHandler.session.read.load(
#             path=self.spec.location,
#             format=self.spec.data_format.value,
#             schema=self.spec.schema,
#             **self.spec.options,
#         )

#     def _extract_streaming(self) -> DataFrame:
#         """
#         Read from file in streaming mode.

#         Returns:
#             DataFrame: A df containing the data from streaming extracting files.
#         """
#         return SparkHandler.session.readStream.load(
#             path=self.spec.location,
#             format=self.spec.data_format.value,
#             schema=self.spec.schema,
#             **self.spec.options,
#         )


# class Extract:
#     """Strategy context for creating data extracts."""

#     strategy = {
#         ExtractFormat.PARQUET: ExtractFile,
#         ExtractFormat.JSON: ExtractFile,
#         ExtractFormat.CSV: ExtractFile,
#     }

#     @classmethod
#     def factory(cls, spec: ExtractSpecAbstract) -> AbstractExtract:
#         """
#         Get an extract instance based on the extract specification using the strategy pattern.

#         Args:
#             spec (ExtractSpecAbstract): Extract specification to extract data.

#         Returns:
#             Extract: An instance of a data extract.

#         Raises:
#             NotImplementedError: If the specified extract format is not supported.
#         """

#         extract_strategy = ExtractFormat(spec.data_format)

#         if extract_strategy in cls.strategy.keys():
#             return cls.strategy[extract_strategy](spec=spec)

#         raise NotImplementedError(f"Extract strategy {spec.data_format.value} is not supported.")
