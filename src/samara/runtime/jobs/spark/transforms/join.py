"""Join transform function.

This module provides a transform function for joining DataFrames
in the ETL pipeline, allowing data from multiple sources to be combined.

The JoinFunction is registered with the TransformFunctionRegistry under
the name 'join', making it available for use in configuration files.
"""

import logging
from collections.abc import Callable

from pyspark.sql import DataFrame
from samara.runtime.jobs.models.transforms.model_join import JoinFunctionModel
from samara.runtime.jobs.spark.function import Function
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class JoinFunction(JoinFunctionModel, Function):
    """Function that joins DataFrames.

    This transform function allows for joining the current DataFrame with another
    DataFrame from the registry.

    Attributes:
        function: The name of the function (always "join")
        arguments: Container for the join parameters
    """

    def transform(self) -> Callable:
        """Apply the join transformation to the DataFrame.

        This method extracts the join configuration from the model
        and returns a callable function that performs the join operation.

        Returns:
            A callable function that applies the join operation to a DataFrame
        """
        logger.debug(
            "Creating join transform - other: %s, on: %s, how: %s",
            self.arguments.other_upstream_id,
            self.arguments.on,
            self.arguments.how,
        )

        def __f(df: DataFrame) -> DataFrame:
            logger.debug("Applying join transform")

            # Get the right DataFrame from the registry
            right_df = self.data_registry[self.arguments.other_upstream_id]
            logger.debug(
                "Retrieved right DataFrame: %s (columns: %s)",
                self.arguments.other_upstream_id,
                right_df.columns,
            )

            # Get the join type
            join_type = self.arguments.how
            # Get the join columns
            join_on = self.arguments.on

            logger.debug("Performing join - left: %d rows, right: %d rows", df.count(), right_df.count())
            logger.debug("Join parameters - on: %s, how: %s", join_on, join_type)

            # Perform the join operation
            result_df = df.join(right_df, on=join_on, how=join_type)
            result_count = result_df.count()

            logger.info("Join transform completed - result: %d rows, join type: %s", result_count, join_type)

            return result_df

        return __f
