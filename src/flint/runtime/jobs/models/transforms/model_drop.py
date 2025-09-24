"""Configuration model for the drop transform function.

This module defines the data models used to configure drop
transformations in the ingestion framework. It includes:

- DropFunctionModel: Main configuration model for drop operations
- DropArgs: Container for the drop parameters

These models provide a type-safe interface for configuring column removal
from configuration files or dictionaries.
"""

from flint.runtime.jobs.models.model_transform import ArgsModel, FunctionModel


class DropArgs(ArgsModel):
    """Arguments for drop transform operations.

    Attributes:
        columns: List of column names to drop from the DataFrame
    """

    columns: list[str]


class DropFunctionModel(FunctionModel[DropArgs]):
    """Configuration model for drop transform operations.

    This model defines the structure for configuring a drop
    transformation, specifying which columns to remove from the DataFrame.

    Attributes:
        function: The name of the function to be used (always "drop")
        arguments: Container for the drop parameters
    """

    function: str
    arguments: DropArgs
