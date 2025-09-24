"""WithColumn transform function.

This module provides a transform function for adding or replacing columns in a DataFrame
based on an expression, enabling column manipulation in the ETL pipeline.

The WithColumnFunction is registered with the TransformFunctionRegistry under
the name 'withColumn', making it available for use in configuration files.
"""

from collections.abc import Callable
from typing import Literal

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

from flint.runtime.jobs.models.transforms.model_withcolumn import WithColumnArgs
from flint.runtime.jobs.spark.function import Function


class WithColumnFunction(Function):
    """Function that adds or replaces a column in a DataFrame.

    This transform function allows for adding a new column or replacing an existing
    column in a DataFrame based on an expression. It's useful for derived columns,
    data transformations, or adding calculated fields.

    The function is configured using a WithColumnFunctionModel that specifies
    the column name and expression.

    Attributes:
        function: The name of the function (always "withColumn")
        model: Configuration model specifying the column name and expression
        model_cls: The concrete model class used for configuration
        data_registry: Shared registry for accessing and storing DataFrames

    Example:
        ```json
        {
            "function": "withColumn",
            "arguments": {
                "col_name": "full_name",
                "col_expr": "concat(first_name, ' ', last_name)"
            }
        }
        ```
    """

    function: Literal["withColumn"]
    arguments: WithColumnArgs

    def transform(self) -> Callable:
        """Apply the withColumn transformation to the DataFrame.

        This method extracts the column name and expression from the model
        and applies it to the DataFrame, adding or replacing the specified column.

        Returns:
            A callable function that performs the column addition/replacement when applied
            to a DataFrame

        Examples:
            Consider the following DataFrame:

            ```
            +----------+---------+---+
            |first_name|last_name|age|
            +----------+---------+---+
            |John      |Doe      |25 |
            |Jane      |Smith    |30 |
            +----------+---------+---+
            ```

            Applying the withColumn function:

            ```
            {
                "function": "withColumn",
                "arguments": {
                    "col_name": "full_name",
                    "col_expr": "concat(first_name, ' ', last_name)"
                }
            }
            ```

            The resulting DataFrame will be:

            ```
            +----------+---------+---+---------+
            |first_name|last_name|age|full_name|
            +----------+---------+---+---------+
            |John      |Doe      |25 |John Doe |
            |Jane      |Smith    |30 |Jane Smith|
            +----------+---------+---+---------+
            ```
        """

        def __f(df: DataFrame) -> DataFrame:
            return df.withColumn(self.arguments.col_name, expr(self.arguments.col_expr))

        return __f
