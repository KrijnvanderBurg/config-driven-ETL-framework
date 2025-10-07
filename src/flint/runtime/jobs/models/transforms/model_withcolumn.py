"""Configuration model for the withColumn transform function.

This module defines the data models used to configure withColumn
transformations in the ingestion framework. It includes:

- WithColumnFunctionModel: Main configuration model for withColumn operations
- WithColumnArgs: Container for the withColumn parameters

These models provide a type-safe interface for configuring column addition
or replacement from configuration files or dictionaries.
"""

from typing import Literal

from pydantic import Field

from flint.runtime.jobs.models.model_transform import ArgsModel, FunctionModel


class WithColumnArgs(ArgsModel):
    """Arguments for withColumn transform operations.

    Attributes:
        col_name: Name of the column to add or replace
        col_expr: Column expression representing the value
    """

    col_name: str = Field(..., description="Name of the column to add or replace", min_length=1)
    col_expr: str = Field(..., description="Column expression representing the value", min_length=1)


class WithColumnFunctionModel(FunctionModel[WithColumnArgs]):
    """Configuration model for withColumn transform operations.

    This model defines the structure for configuring a withColumn
    transformation, specifying the column name and expression.

    Attributes:
        function_type: The name of the function to be used (always "withColumn")
        arguments: Container for the withColumn parameters
    """

    function_type: Literal["withColumn"] = "withColumn"
    arguments: WithColumnArgs = Field(..., description="Container for the withColumn parameters")
