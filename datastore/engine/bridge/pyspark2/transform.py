# """
# Transform Strategy.

# This module provides an abstract strategy class `TransformStrategy` for creating instances of data transforms.
# The transform implementations are located in the `datastore.transform` module.


# Copyright (c) Krijn van der Burg.

# This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
# Attribution-NonCommercial-NoDerivs 4.0 International License.
# See the accompanying LICENSE file for details,
# or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
# """

# from abc import ABC, abstractmethod

# from pyspark.sql import DataFrame

# from datastore.models.transform import TransformSpecAbstract


# class TransformStrategy(ABC):
#     """Abstract Transform class."""

#     def __init__(self, spec: TransformSpecAbstract, df: DataFrame):
#         """
#         Construct Transform instance.

#         Args:
#             spec (TransformSpecAbstract): specification for transforming data.
#             df (DataFrame): DataFrame to Transform.
#         """
#         self.spec: TransformSpecAbstract = spec
#         self.df: DataFrame = df

#     @abstractmethod
#     def transform(self) -> DataFrame:
#         """
#         Abstract Transform method.

#         Raises:
#             NotImplementedError: This method must be implemented by the subclass.
#         """
#         raise NotImplementedError


# class TransformBasic(TransformStrategy):
#     """
#     Transform class.

#     Args:
#         spec (TransformSpecAbstract): Transform specification.
#         df (DataFrame): DataFrame to be transformed.
#     """

#     def __init__(self, spec: TransformSpecAbstract, df: DataFrame):
#         super().__init__(spec=spec, df=df)

#     def transform(self) -> DataFrame:
#         """
#         Apply all transform functions on df.
#         """
#         if not self.spec:
#             return self.df

#         for transform in self.spec.transforms:
#             self.df = self.df.transform(func=transform.function)

#         return self.df


# class TransformContext(ABC):
#     """Abstract class representing a strategy context for creating data extracts."""

#     @classmethod
#     def get(cls, spec: TransformSpecAbstract, df: DataFrame) -> TransformStrategy:
#         """
#         Get a Transform instance based on the Transform specification using the strategy pattern.

#         Args:
#             spec (TransformSpecAbstract): Transform specification to transform data.
#             df (DataFrame): DataFrame to Transform.

#         Returns:
#             Transform: An instance of a data Transform.

#         Raises:
#             NotImplementedError: If the specified transform provider is not supported.
#         """
#         if True:  # pylint: disable=using-constant-test
#             return TransformBasic(spec=spec, df=df)

#         raise NotImplementedError("The transform strategy is not supported.")
