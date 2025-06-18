"""Join transform function.

This module provides a transform function for joining DataFrames
in the ETL pipeline, allowing data from multiple sources to be combined.

The JoinFunction is registered with the TransformFunctionRegistry under
the name 'join', making it available for use in configuration files.
"""

from collections.abc import Callable

from pyspark.sql import DataFrame

from flint.core.transform import Function, TransformFunctionRegistry
from flint.models.transforms.model_join import JoinFunctionModel


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

        def __f(df: DataFrame) -> DataFrame:
            # Get the right DataFrame from the registry
            right_df = self.data_registry[self.model.arguments.other_upstream_name]

            # Get the join type
            join_type = self.model.arguments.how

            # Get the join columns
            join_on = self.model.arguments.on

            # Perform the join operation
            result_df = df.join(right_df, on=join_on, how=join_type)

            return result_df

        return __f
