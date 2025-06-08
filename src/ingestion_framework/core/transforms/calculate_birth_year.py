"""
Calculate birth year transform function.

This module provides a transformation function that calculates a person's birth year
based on their age and the current year.
"""

from collections.abc import Callable

from pyspark.sql import functions as F

from ingestion_framework.core.transform import Function, TransformFunctionRegistry
from ingestion_framework.models.transforms.model_calculate_birth_year import CalculateBirthYearFunctionModel

# Import these locally to avoid circular imports
from ingestion_framework.types import DataFrameRegistry


@TransformFunctionRegistry.register("calculate_birth_year")
class CalculateBirthYearFunction(Function[CalculateBirthYearFunctionModel]):
    """
    Encapsulates birth year calculation logic for DataFrames.

    Attributes:
        model (CalculateBirthYearFunctionModel): The model object containing the calculation parameters.

    Methods:
        from_dict(dict_: dict[str, Any]) -> Self: Create CalculateBirthYearFunction object from json.
        transform() -> Callable: Calculates birth year from age.
    """

    model_concrete = CalculateBirthYearFunctionModel

    def transform(self) -> Callable:
        """
                Calculates birth year based on age.

                Returns:
                    (Callable): Function for `DataFrame.transform()`.

                Examples:
                    Consider the following DataFrame schema:

                    ```
                    root
                    |-- name: string (nullable = true)
                    |-- age: integer (nullable = true)
                    ```

                    Applying the 'calculate_birth_year' function:

        ```
                    {
                        "function": "calculate_birth_year",
                        "arguments": {
                            "current_year": 2025,
                            "age_column": "age",
                            "birth_year_column": "birth_year"
                        }
                    }
                    ```

                    The resulting DataFrame schema will be:

                    ```
                    root
                    |-- name: string (nullable = true)
                    |-- age: integer (nullable = true)
                    |-- birth_year: integer (nullable = true)
                    ```
        """

        def __f(dataframe_registry: DataFrameRegistry, dataframe_name: str) -> None:
            dataframe_registry[dataframe_name] = dataframe_registry[dataframe_name].withColumn(
                self.model.arguments.birth_year_column,
                F.lit(self.model.arguments.current_year) - F.col(self.model.arguments.age_column),
            )

        return __f
