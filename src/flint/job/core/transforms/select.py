"""Column selection transform function.

This module provides a transform function for selecting specific columns
from a DataFrame, allowing for projection operations in the ETL pipeline.

The SelectFunction is registered with the TransformFunctionRegistry under
the name 'select', making it available for use in configuration files.
"""

import logging
from collections.abc import Callable

from pyspark.sql import DataFrame

from flint.job.core.transform import Function, TransformFunctionRegistry
from flint.job.models.transforms.model_select import SelectFunctionModel
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


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
        model_cls: The concrete model class used for configuration
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

    model_cls = SelectFunctionModel

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
        logger.debug("Creating select transform for columns: %s", self.model.arguments.columns)

        def __f(df: DataFrame) -> DataFrame:
            logger.debug("Applying select transform - input columns: %s", df.columns)
            logger.debug("Selecting columns: %s", self.model.arguments.columns)

            result_df = df.select(*self.model.arguments.columns)
            logger.info(
                "Select transform completed - selected %d columns from %d", len(result_df.columns), len(df.columns)
            )
            logger.debug("Selected columns: %s", result_df.columns)
            return result_df

        return __f
