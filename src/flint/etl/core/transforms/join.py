"""Join transform function.

This module provides a transform function for joining DataFrames
in the ETL pipeline, allowing data from multiple sources to be combined.

The JoinFunction is registered with the TransformFunctionRegistry under
the name 'join', making it available for use in configuration files.
"""

import logging
from collections.abc import Callable

from pyspark.sql import DataFrame

from flint.etl.core.transform import Function, TransformFunctionRegistry
from flint.etl.models.transforms.model_join import JoinFunctionModel
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


@TransformFunctionRegistry.register("join")
class JoinFunction(Function[JoinFunctionModel]):
    """Function that joins DataFrames.

    This transform function allows for joining the current DataFrame with another
    DataFrame from the registry. It supports different join types (inner, outer, left, right)
    and can join on one or multiple columns.

    The function is configured using a JoinFunctionModel that specifies
    which DataFrame to join with, the join columns, and the join type.

    Attributes:
        model: Configuration model specifying join parameters
        model_cls: The concrete model class used for configuration
        data_registry: Shared registry for accessing and storing DataFrames

    Example:
        ```json
        {
            "function": "join",
            "arguments": {
                "other_upstream_name": "extract-orders",
                "on": "customer_id",
                "how": "inner"
            }
        }
        ```

    For multi-column joins:
        ```json
        {
            "function": "join",
            "arguments": {
                "other_upstream_name": "extract-orders",
                "on": ["customer_id", "store_id"],
                "how": "left"
            }
        }
        ```
    """

    model_cls = JoinFunctionModel

    def transform(self) -> Callable:
        """Apply the join transformation to the DataFrame.

        This method extracts the join configuration from the model
        and returns a callable function that performs the join operation.

        Returns:
            A callable function that applies the join operation to a DataFrame
        """
        logger.debug(
            "Creating join transform - other: %s, on: %s, how: %s",
            self.model.arguments.other_upstream_name,
            self.model.arguments.on,
            self.model.arguments.how,
        )

        def __f(df: DataFrame) -> DataFrame:
            logger.debug("Applying join transform")

            # Get the right DataFrame from the registry
            right_df = self.data_registry[self.model.arguments.other_upstream_name]
            logger.debug(
                "Retrieved right DataFrame: %s (columns: %s)",
                self.model.arguments.other_upstream_name,
                right_df.columns,
            )

            # Get the join type
            join_type = self.model.arguments.how
            # Get the join columns
            join_on = self.model.arguments.on

            logger.debug("Performing join - left: %d rows, right: %d rows", df.count(), right_df.count())
            logger.debug("Join parameters - on: %s, how: %s", join_on, join_type)

            # Perform the join operation
            result_df = df.join(right_df, on=join_on, how=join_type)
            result_count = result_df.count()

            logger.info("Join transform completed - result: %d rows, join type: %s", result_count, join_type)

            return result_df

        return __f
