"""Configuration model for the filter/where transform function.

This module defines the data models used to configure filter/where
transformations in the ingestion framework. It includes:

- FilterFunctionModel: Main configuration model for filter operations
- FilterArgs: Container for the filtering parameters

These models provide a type-safe interface for configuring filter operations
from configuration files or dictionaries.
"""

from pydantic import Field

from flint.runtime.jobs.models.model_transform import ArgsModel, FunctionModel


class FilterArgs(ArgsModel):
    """Arguments for filter transform operations.

    Attributes:
        condition: String expression representing the filter condition
    """

    condition: str = Field(..., description="String expression representing the filter condition", min_length=1)


class FilterFunctionModel(FunctionModel[FilterArgs]):
    """Configuration model for filter/where transform operations.

    This model defines the structure for configuring a filter/where
    transformation, specifying the condition to filter rows.

    Attributes:
        function_type: The name of the function to be used (always "filter")
        arguments: Container for the filter parameters
    """

    function_type: str = "filter"
    arguments: FilterArgs = Field(..., description="Container for the filter parameters")
