from collections.abc import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from flint.core.transform import Function, TransformFunctionRegistry
from flint.models.transforms.model_customer_orders import CustomersOrdersFunctionModel


@TransformFunctionRegistry.register("customers_orders")
class CustomersOrdersBronzeFunction(Function[CustomersOrdersFunctionModel]):
    """Function that joins customers and orders, performs type casting,
    and calculates aggregations.

    Attributes:
        model: Configuration model specifying transformation parameters
        model_cls: The concrete model class used for configuration
        data_registry: Shared registry for accessing and storing DataFrames

    Example:
        ```json
        { "function": "customers_orders", "arguments": { "amount_minimum": 100 } }
        ```
    """

    model_cls = CustomersOrdersFunctionModel

    def transform(self) -> Callable:
        """Apply transformations to customers and orders DataFrames.

        This method:
        1. Casts data types appropriately
        2. Joins customers and orders data
        3. Calculates customer spending aggregates
        4. Filters for large orders

        Returns:
            A callable that performs the transformations
        """

        def __f(df: DataFrame) -> DataFrame:
            customers_df = df  # df is data_registry["extract-customers"]
            orders_df = self.data_registry["extract-orders"]

            # Cast data types
            customers_df = customers_df.withColumn("customer_id", F.col("customer_id").cast("integer"))
            customers_df = customers_df.withColumn("signup_date", F.to_date(F.col("signup_date")))
            orders_df = orders_df.withColumn("order_date", F.to_date(F.col("order_date")))

            # Join datasets
            combined_df = customers_df.join(orders_df, "customer_id", "inner")

            # Filter
            large_orders = combined_df.filter(F.col("amount") > self.model.arguments.amount_minimum)

            return large_orders

        return __f
