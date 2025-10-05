"""Configuration model for the column casting transform function.

This module defines the data models used to configure column type casting
transformations in the ingestion framework. It includes:

- CastFunctionModel: Main configuration model for cast operations
- Args nested class: Container for the casting parameters

These models provide a type-safe interface for configuring column type casting
from configuration files or dictionaries.
"""

from pydantic import Field

from flint import BaseModel
from flint.runtime.jobs.models.model_transform import ArgsModel, FunctionModel


class CastColumn(BaseModel):
    """Single column casting definition.

    Attributes:
        column_name: Name of the column to cast
        cast_type: Target data type to cast the column to
    """

    column_name: str = Field(..., description="Name of the column to cast", min_length=1)
    cast_type: str = Field(..., description="Target data type to cast the column to", min_length=1)


class CastArgs(ArgsModel):
    """Arguments container for column casting operations.

    Attributes:
        columns: List of column casting definitions
    """

    columns: list[CastColumn] = Field(..., description="List of column casting definitions")


class CastFunctionModel(FunctionModel[CastArgs]):
    """Configuration model for column casting transform operations.

    This model defines the structure for configuring a column type casting
    transformation, specifying which columns should be cast to which data types.

    Attributes:
        function_type: The name of the function to be used (always "cast")
        arguments: Container for the column casting parameters
    """

    function_type: str = "cast"
    arguments: CastArgs = Field(..., description="Container for the column casting parameters")
