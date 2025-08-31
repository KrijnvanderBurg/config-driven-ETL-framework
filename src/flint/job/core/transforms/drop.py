"""Drop transform function.

This module provides a transform function for removing columns from a DataFrame,
enabling column pruning in the ETL pipeline.

The DropFunction is registered with the TransformFunctionRegistry under
the name 'drop', making it available for use in configuration files.
"""

from collections.abc import Callable

from pyspark.sql import DataFrame

from flint.job.core.transform import Function, TransformFunctionRegistry
from flint.job.models.transforms.model_drop import DropFunctionModel


@TransformFunctionRegistry.register("drop")
class DropFunction(Function[DropFunctionModel]):
    """Function that removes specified columns from a DataFrame.

    This transform function allows for dropping columns from a DataFrame,
    helping to remove unnecessary data or clean up interim calculation columns.

    The function is configured using a DropFunctionModel that specifies
    which columns to remove from the DataFrame.

    Attributes:
        model: Configuration model specifying which columns to drop
        model_cls: The concrete model class used for configuration
        data_registry: Shared registry for accessing and storing DataFrames

    Example:
        ```json
        {
            "function": "drop",
            "arguments": {
                "columns": ["temp_col", "unused_field"]
            }
        }
        ```
    """

    model_cls = DropFunctionModel

    def transform(self) -> Callable:
        """Apply the column drop transformation to the DataFrame.

        This method extracts the columns to drop from the model
        and applies the drop operation to the DataFrame, removing the specified columns.

        Returns:
            A callable function that performs the column removal when applied
            to a DataFrame

        Examples:
            Consider the following DataFrame:

            ```
            +----+-------+---+--------+
            |id  |name   |age|temp_col|
            +----+-------+---+--------+
            |1   |John   |25 |xyz     |
            |2   |Jane   |30 |abc     |
            +----+-------+---+--------+
            ```

            Applying the drop function:

            ```
            {"function": "drop", "arguments": {"columns": ["temp_col"]}}
            ```

            The resulting DataFrame will be:

            ```
            +----+-------+---+
            |id  |name   |age|
            +----+-------+---+
            |1   |John   |25 |
            |2   |Jane   |30 |
            +----+-------+---+
            ```
        """

        def __f(df: DataFrame) -> DataFrame:
            return df.drop(*self.model.arguments.columns)

        return __f
