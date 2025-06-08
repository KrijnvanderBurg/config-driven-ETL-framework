# """
# Recipe for calculating birth year based on age.

# This module provides a transformation recipe that calculates a person's birth year
# based on their age and the current year.
# """

# from typing import Any, Callable

# from pyspark.sql import functions as F

# from ingestion_framework.pyspark.transforms.recipes.base import RecipePyspark
# from ingestion_framework.types import DataFramePysparkRegistry


# class CalculateBirthYearRecipe(RecipePyspark):
#     """
#     Recipe for calculating birth year based on age.

#     Arguments:
#         current_year (int): The current year to use for calculation
#         age_column (str): The name of the column containing age values
#         birth_year_column (str): The name of the column to store calculated birth years
#     """

#     def __init__(self, arguments: dict[str, Any]) -> None:
#         """
#         Initialize the recipe with arguments.

#         Args:
#             arguments (dict[str, Any]): Configuration for the recipe
#         """
#         super().__init__(arguments)
#         self.current_year = arguments.get("current_year", 2023)
#         self.age_column = arguments.get("age_column", "age")
#         self.birth_year_column = arguments.get("birth_year_column", "birth_year")

#     @classmethod
#     def from_dict(cls, dict_: dict[str, Any]) -> "CalculateBirthYearRecipe":
#         """
#         Create a recipe instance from configuration.

#         Args:
#             dict_ (dict[str, Any]): Configuration dictionary

#         Returns:
#             CalculateBirthYearRecipe: Configured recipe instance
#         """
#         arguments = dict_.get("arguments", {})
#         return cls(arguments=arguments)

#     def transform(self) -> Callable:
#         """
#         Create a callable for calculating birth year.

#         Returns:
#             Callable: A callable that adds birth year to a DataFrame.
#         """
#         current_year = self.current_year
#         age_column = self.age_column
#         birth_year_column = self.birth_year_column

#         def __f(dataframe_registry: DataFramePysparkRegistry, dataframe_name: str) -> None:
#             """
#             Apply the birth year calculation to the dataframe.

#             Args:
#                 dataframe_registry (DataFramePysparkRegistry): Registry containing dataframes
#                 dataframe_name (str): Name of the dataframe to transform
#             """
#             df = dataframe_registry[dataframe_name]

#             # Calculate birth year by subtracting age from current year
#             df_transformed = df.withColumn(birth_year_column, F.lit(current_year) - F.col(age_column))

#             # Update the registry with transformed dataframe
#             dataframe_registry[dataframe_name] = df_transformed

#         return __f
