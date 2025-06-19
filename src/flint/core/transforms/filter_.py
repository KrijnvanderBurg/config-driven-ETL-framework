"""Filter/where transform function.

This module provides a transform function for filtering rows in a DataFrame
based on a specified condition, enabling WHERE-like functionality in the ETL pipeline.

The FilterFunction is registered with the TransformFunctionRegistry under
the name 'filter', making it available for use in configuration files.
"""

from collections.abc import Callable

from pyspark.sql import DataFrame

from flint.core.transform import Function, TransformFunctionRegistry
from flint.models.transforms.model_filter import FilterFunctionModel


@TransformFunctionRegistry.register("filter")
class FilterFunction(Function[FilterFunctionModel]):
    """Function that filters rows from a DataFrame based on a condition.

    This transform function allows for filtering rows from a DataFrame based on a
    specified condition, similar to the WHERE clause in SQL. It's useful for
    removing rows that don't meet certain criteria.

    The function is configured using a FilterFunctionModel that specifies
    the condition to be applied.

    Attributes:
        model: Configuration model specifying the filter condition
        model_cls: The concrete model class used for configuration
        data_registry: Shared registry for accessing and storing DataFrames

    Example:
        ```json
        {
            "function": "filter",
            "arguments": {
                "condition": "age > 18"
            }
        }
        ```
    """

    model_cls = FilterFunctionModel

    def transform(self) -> Callable:
        """Apply the filter transformation to the DataFrame.

        This method extracts the filter condition from the model
        and applies it to the DataFrame, returning only the rows that
        satisfy the condition.

        Returns:
            A callable function that performs the filtering when applied
            to a DataFrame

        Examples:
            Consider the following DataFrame:

            ```
            +----+-------+---+
            |id  |name   |age|
            +----+-------+---+
            |1   |John   |25 |
            |2   |Jane   |17 |
            |3   |Bob    |42 |
            |4   |Alice  |15 |
            +----+-------+---+
            ```

            Applying the filter function with condition "age > 18":

            ```
            {"function": "filter", "arguments": {"condition": "age > 18"}}
            ```

            The resulting DataFrame will be:

            ```
            +----+-------+---+
            |id  |name   |age|
            +----+-------+---+
            |1   |John   |25 |
            |3   |Bob    |42 |
            +----+-------+---+
            ```
        """

        def __f(df: DataFrame) -> DataFrame:
            return df.filter(self.model.arguments.condition)

        return __f
