"""Column selection transform function.

This module provides a transform function for selecting specific columns
from a DataFrame, allowing for projection operations in the ETL pipeline.

The SelectFunction is registered with the TransformFunctionRegistry under
the name 'select', making it available for use in configuration files.
"""

import logging
from collections.abc import Callable
from typing import Literal

from pyspark.sql import DataFrame

from flint.runtime.jobs.models.transforms.model_select import SelectArgs
from flint.runtime.jobs.spark.function import Function
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class SelectFunction(Function):
    """Function that selects specified columns from a DataFrame.

    This transform function allows for projecting specific columns from
    a DataFrame, similar to the SELECT statement in SQL. It's useful for
    filtering out unnecessary columns and focusing only on the data needed
    for downstream processing.

    The function is configured using a SelectFunctionModel that specifies
    which columns to include in the output.

    Attributes:
        function: The name of the function (always "select")
        model: Configuration model specifying which columns to select
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

    function_type: Literal["select"]
    arguments: SelectArgs

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
        logger.debug("Creating select transform for columns: %s", self.arguments.columns)

        def __f(df: DataFrame) -> DataFrame:
            logger.debug("Applying select transform - input columns: %s", df.columns)
            logger.debug("Selecting columns: %s", self.arguments.columns)

            result_df = df.select(*self.arguments.columns)
            logger.info(
                "Select transform completed - selected %d columns from %d", len(result_df.columns), len(df.columns)
            )
            logger.debug("Selected columns: %s", result_df.columns)
            return result_df

        return __f
