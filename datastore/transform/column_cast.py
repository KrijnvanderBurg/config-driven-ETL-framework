"""Cast

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from collections.abc import Callable

import pyspark.sql.types as spark_types
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


class ColumnCast:
    """
    Class encapsulate column casting logic.
    """

    @classmethod
    def cast(cls, cols: dict[str, str]) -> Callable:
        """
        Cast column(s) to new type.

        Args:
            cols (dict): dict with columns and new cast types.

        Returns:
            (def): callable function for transform.
        """

        def f(df: DataFrame) -> DataFrame:
            for column, cast_type in cols.items():
                attr = getattr(spark_types, cast_type)()
                df = df.withColumn(column, col(column).cast(attr))
            return df

        return f
