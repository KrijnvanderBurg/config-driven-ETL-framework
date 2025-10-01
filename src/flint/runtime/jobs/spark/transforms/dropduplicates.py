"""Drop duplicates transform function.

This module provides a transform function for removing duplicate rows from a DataFrame,
enabling data deduplication in the ETL pipeline.

The DropDuplicatesFunction is registered with the TransformFunctionRegistry under
the name 'dropduplicates', making it available for use in configuration files.
"""

from collections.abc import Callable
from typing import Literal

from pyspark.sql import DataFrame

from flint.runtime.jobs.models.transforms.model_dropduplicates import DropDuplicatesArgs
from flint.runtime.jobs.spark.function import Function


class DropDuplicatesFunction(Function):
    """Function that removes duplicate rows from a DataFrame.

    This transform function allows for removing duplicate rows based on all columns
    or a subset of columns, helping to clean and deduplicate data.

    Attributes:
        function: The name of the function (always "dropduplicates")
        arguments: Container for the dropDuplicates parameters
    """

    function: Literal["dropduplicates"]
    arguments: DropDuplicatesArgs

    def transform(self) -> Callable:
        """Apply the duplicate removal transformation to the DataFrame.

        This method extracts the columns to consider from the model
        and applies the dropDuplicates operation to the DataFrame, removing duplicate rows.
        If no columns are specified, all columns are considered when identifying duplicates.

        Returns:
            A callable function that performs the duplicate removal when applied
            to a DataFrame

        Examples:
            Consider the following DataFrame:

            ```
            +----+-------+---+
            |id  |name   |age|
            +----+-------+---+
            |1   |John   |25 |
            |2   |Jane   |30 |
            |3   |John   |25 |
            |4   |Bob    |40 |
            +----+-------+---+
            ```

            Applying the dropDuplicates function with columns ["name", "age"]:

            ```
            {"function": "dropDuplicates", "arguments": {"columns": ["name", "age"]}}
            ```

            The resulting DataFrame will be:

            ```
            +----+-------+---+
            |id  |name   |age|
            +----+-------+---+
            |1   |John   |25 |
            |2   |Jane   |30 |
            |4   |Bob    |40 |
            +----+-------+---+
            ```
        """

        def __f(df: DataFrame) -> DataFrame:
            columns: list[str] = self.arguments.columns
            if columns:
                return df.dropDuplicates(columns)
            return df.dropDuplicates()

        return __f
