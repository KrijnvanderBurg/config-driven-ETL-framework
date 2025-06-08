"""Column selection transform function.

This module provides a transform function for selecting specific columns
from a DataFrame, allowing for projection operations in the ETL pipeline.

The SelectFunction is registered with the TransformFunctionRegistry under
the name 'select', making it available for use in configuration files.
"""

from collections.abc import Callable

from pyspark.sql import DataFrame

from ingestion_framework.core.transform import Function, TransformFunctionRegistry
from ingestion_framework.models.transforms.model_select import SelectFunctionModel


@TransformFunctionRegistry.register("select")
class SelectFunction(Function[SelectFunctionModel]):
    """Function that selects specified columns from a DataFrame.

    This transform function allows for projecting specific columns from
    a DataFrame, similar to the SELECT statement in SQL. It's useful for
    filtering out unnecessary columns and focusing only on the data needed
    for downstream processing.

    The function is configured using a SelectFunctionModel that specifies
    which columns to include in the output.

    Attributes:
        model: Configuration model specifying which columns to select
        _model: The concrete model class used for configuration
        data_registry: Shared registry for accessing and storing DataFrames

    Example:
        ```json
        {
            "function": "select",
            "arguments": {
                "columns": ["id", "name", "age"]
            }
        }
        ```
    """

    _model = SelectFunctionModel

    def transform(self) -> Callable:
        """Apply the column selection transformation to the DataFrame.

        This method extracts the column selection configuration from the model
        and applies it to the DataFrame, returning only the specified columns.
        It supports selecting specific columns by name.

        Returns:
            A callable function that performs the column selection when applied
            to a DataFrame

        Examples:
            Consider the following DataFrame schema:

            ```
            root
            |-- name: string (nullable = true)
            |-- age: integer (nullable = true)
            ```

            Applying the dict_ 'select_with_alias' function:

            ```
            {"function": "select_with_alias", "arguments": {"columns": {"age": "years_old",}}}
            ```

            The resulting DataFrame schema will be:

            ```
            root
            |-- years_old: integer (nullable = true)
            ```
        """

        def __f(df: DataFrame) -> DataFrame:
            return df.select(*self.model.arguments.columns)

        return __f
