"""Configuration model for the dropDuplicates transform function.

This module defines the data models used to configure dropDuplicates
transformations in the ingestion framework. It includes:

- DropDuplicatesFunctionModel: Main configuration model for dropDuplicates operations
- DropDuplicatesArgs: Container for the dropDuplicates parameters

These models provide a type-safe interface for configuring duplicate row removal
from configuration files or dictionaries.
"""

from typing import Literal

from pydantic import Field
from samara.runtime.jobs.models.model_transform import ArgsModel, FunctionModel


class DropDuplicatesArgs(ArgsModel):
    """Arguments for dropDuplicates transform operations.

    Attributes:
        columns: List of column names to consider when dropping duplicates.
                If empty list, all columns are considered.
    """

    columns: list[str] = Field(
        ...,
        description=(
            "List of column names to consider when dropping duplicates. If empty list, all columns are considered."
        ),
    )


class DropDuplicatesFunctionModel(FunctionModel[DropDuplicatesArgs]):
    """Configuration model for dropDuplicates transform operations.

    This model defines the structure for configuring a dropDuplicates
    transformation, specifying which columns to consider when identifying duplicates.

    Attributes:
        function_type: The name of the function to be used (always "dropDuplicates")
        arguments: Container for the dropDuplicates parameters
    """

    function_type: Literal["dropDuplicates"] = "dropDuplicates"
    arguments: DropDuplicatesArgs = Field(..., description="Container for the dropDuplicates parameters")
