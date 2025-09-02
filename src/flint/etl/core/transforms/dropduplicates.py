"""DropDuplicates transform function.

This module provides a transform function for removing duplicate rows from a DataFrame,
enabling data deduplication in the ETL pipeline.

The DropDuplicatesFunction is registered with the TransformFunctionRegistry under
the name 'dropDuplicates', making it available for use in configuration files.
"""

from collections.abc import Callable

from pyspark.sql import DataFrame

from flint.etl.core.transform import Function, TransformFunctionRegistry
from flint.etl.models.transforms.model_dropduplicates import DropDuplicatesFunctionModel


@TransformFunctionRegistry.register("dropDuplicates")
class DropDuplicatesFunction(Function[DropDuplicatesFunctionModel]):
    """Function that removes duplicate rows from a DataFrame.

    This transform function allows for removing duplicate rows from a DataFrame,
    optionally considering only specific columns when identifying duplicates.
    It's useful for data cleansing and ensuring uniqueness in datasets.

    The function is configured using a DropDuplicatesFunctionModel that optionally
    specifies which columns to consider when determining duplicates.

    Attributes:
        model: Configuration model specifying which columns to consider for deduplication
        model_cls: The concrete model class used for configuration
        data_registry: Shared registry for accessing and storing DataFrames

    Example:
        ```json
        {
            "function": "dropDuplicates",
            "arguments": {
                "columns": ["customer_id", "order_date"]
            }
        }
        ```
    """

    model_cls = DropDuplicatesFunctionModel

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
            columns: list[str] | None = self.model.arguments.columns
            if columns:
                return df.dropDuplicates(columns)
            return df.dropDuplicates()

        return __f
