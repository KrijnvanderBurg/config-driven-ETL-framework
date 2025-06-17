"""Column casting transform function.

This module provides a transform function for casting columns to specific
data types in a DataFrame, allowing for type conversion operations in the ETL pipeline.

The CastFunction is registered with the TransformFunctionRegistry under
the name 'cast', making it available for use in configuration files.
"""

from collections.abc import Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from flint.core.transform import Function, TransformFunctionRegistry
from flint.models.transforms.model_cast import CastFunctionModel


@TransformFunctionRegistry.register("cast")
class CastFunction(Function[CastFunctionModel]):
    """Function that casts columns to specified data types in a DataFrame.

    This transform function allows for changing the data type of specific columns
    in a DataFrame, similar to the CAST statement in SQL. It's useful for
    ensuring data types match the expected format for downstream processing or
    for correcting data type issues after import.

    The function is configured using a CastFunctionModel that specifies
    which columns to cast and to which data types.

    Supported PySpark data types include all standard Spark SQL types as strings:
    - "string"
    - "integer", "int"
    - "long", "bigint"
    - "float"
    - "double"
    - "boolean", "bool"
    - "date"
    - "timestamp"
    - "decimal(10,2)" - where 10 is precision and 2 is scale

    Attributes:
        model: Configuration model specifying which columns to cast
        model_cls: The concrete model class used for configuration
        data_registry: Shared registry for accessing and storing DataFrames

    Example:
        ```json
        {
            "function": "cast",
            "arguments": {
                "columns": {
                    "age": "int",
                    "price": "decimal(10,2)",
                    "is_active": "boolean",
                    "created_at": "timestamp"
                }
            }
        }
        ```

    Note that you must use the exact type name as recognized by Spark SQL. The cast
    is applied directly using the DataFrame.withColumn().cast() method.
    """

    model_cls = CastFunctionModel

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
            for column_name, data_type in self.model.arguments.columns.items():
                # Apply the cast operation based on the data type
                result = result.withColumn(column_name, col(column_name).cast(data_type))
            return result

        return __f
