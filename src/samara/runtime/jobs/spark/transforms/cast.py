"""Column casting transform function.

This module provides a transform function for casting columns to specific
data types in a DataFrame, allowing for type conversion operations in the ETL pipeline.

The CastFunction is registered with the TransformFunctionRegistry under
the name 'cast', making it available for use in configuration files.
"""

from collections.abc import Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from samara.runtime.jobs.models.transforms.model_cast import CastFunctionModel
from samara.runtime.jobs.spark.transforms.base import FunctionSpark


class CastFunction(CastFunctionModel, FunctionSpark):
    """Function that casts columns to specified data types in a DataFrame.

    This transform function allows for changing the data type of specific columns
    in a DataFrame, similar to the CAST statement in SQL. It's useful for
    ensuring data types match the expected format for downstream processing or
    for correcting data type issues after import.

    Attributes:
        function_type: The name of the function (always "cast")
        arguments: Container for the column casting parameters
    """

    def transform(self) -> Callable:
        """Apply the column casting transformation to the DataFrame.

        This method extracts the column casting configuration from the model
        and returns a function that will apply these type conversions to
        a DataFrame when called.

        Returns:
            A callable function that takes a DataFrame and returns a new
            DataFrame with the specified column type conversions applied.
        """

        def __f(df: DataFrame) -> DataFrame:
            """Apply column type conversions to the DataFrame.

            Args:
                df: Input DataFrame containing columns to be cast

            Returns:
                DataFrame with the specified column type conversions applied
            """
            result = df
            for column in self.arguments.columns:
                # Apply the cast operation based on the data type
                result = result.withColumn(column.column_name, col(column.column_name).cast(column.cast_type))
            return result

        return __f
