"""Drop transform function.

This module provides a transform function for removing columns from a DataFrame,
enabling column pruning in the ETL pipeline.

The DropFunction is registered with the TransformFunctionRegistry under
the name 'drop', making it available for use in configuration files.
"""

from collections.abc import Callable

from pyspark.sql import DataFrame

from samara.runtime.jobs.models.transforms.model_drop import DropFunctionModel
from samara.runtime.jobs.spark.transforms.base import FunctionSpark


class DropFunction(DropFunctionModel, FunctionSpark):
    """Function that drops specified columns from a DataFrame.

    This transform function allows for removing unwanted columns from
    a DataFrame, helping to reduce memory usage and focus on relevant data.

    Attributes:
        function: The name of the function (always "drop")
        arguments: Container for the drop parameters
    """

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
            return df.drop(*self.arguments.columns)

        return __f
