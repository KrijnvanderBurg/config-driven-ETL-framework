"""
Data transform module.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from datastore.transform.base import TransformSpec, TransformStrategy
from pyspark.sql import DataFrame


class TransformBasic(TransformStrategy):
    """
    Transform class.

    Args:
        spec (TransformSpec): Transform specification.
        dataframe (DataFrame): DataFrame to be transformed.
    """

    def __init__(self, spec: TransformSpec, dataframe: DataFrame):
        super().__init__(spec=spec, dataframe=dataframe)

    def transform(self) -> DataFrame:
        """
        Apply all transform functions on dataframe.
        """
        if not self.spec:
            return self.dataframe

        for transform in self.spec.transforms:
            self.dataframe = self.dataframe.transform(func=transform.function)

        return self.dataframe
