"""Configuration model for the withColumn transform function.

This module defines the data models used to configure withColumn
transformations in the ingestion framework. It includes:

- WithColumnFunctionModel: Main configuration model for withColumn operations
- WithColumnArgs: Container for the withColumn parameters

These models provide a type-safe interface for configuring column addition
or replacement from configuration files or dictionaries.
"""

from flint.runtime.jobs.models.model_transform import ArgsModel, FunctionModel


class WithColumnArgs(ArgsModel):
    """Arguments for withColumn transform operations.

    Attributes:
        col_name: Name of the column to add or replace
        col_expr: Column expression representing the value
    """

    col_name: str
    col_expr: str


class WithColumnFunctionModel(FunctionModel[WithColumnArgs]):
    """Configuration model for withColumn transform operations.

    This model defines the structure for configuring a withColumn
    transformation, specifying the column name and expression.

    Attributes:
        function: The name of the function to be used (always "withColumn")
        arguments: Container for the withColumn parameters
    """

    function: str
    arguments: WithColumnArgs
