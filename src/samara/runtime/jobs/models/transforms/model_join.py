"""Configuration model for the join transform function.

This module defines the data models used to configure join
transformations in the ingestion framework. It includes:

- JoinFunctionModel: Main configuration model for join operations
- JoinArgs: Container for the join parameters

These models provide a type-safe interface for configuring joins
from configuration files or dictionaries.
"""

import logging
from typing import Literal, Self

from pydantic import Field, ValidationInfo, model_validator
from samara.runtime.jobs.models.model_transform import ArgsModel, FunctionModel
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class JoinArgs(ArgsModel):
    """Arguments for join transform operations.

    Attributes:
        other_upstream_id: Identifier of the dataframe to join with the current dataframe
        on: Column(s) to join on. Can be a string for a single column or a list of strings for multiple columns
        how: Type of join to perform (inner, outer, left, right, etc.). Defaults to "inner"
    """

    other_upstream_id: str = Field(
        ..., description="Identifier of the dataframe to join with the current dataframe", min_length=1
    )
    on: str | list[str] = Field(
        ...,
        description=(
            "Column(s) to join on. Can be a string for a single column or a list of strings for multiple columns"
        ),
    )
    how: str = Field(default="inner", description="Type of join to perform (inner, outer, left, right, etc.)")


class JoinFunctionModel(FunctionModel[JoinArgs]):
    """Configuration model for join transform operations.

    This model defines the structure for configuring a join
    transformation, specifying the dataframes to join and how to join them.

    Attributes:
        function_type: The name of the function to be used (always "join")
        arguments: Container for the join parameters
    """

    function_type: Literal["join"] = "join"
    arguments: JoinArgs = Field(..., description="Container for the join parameters")

    @model_validator(mode="after")
    def validate_other_upstream_id(self, info: ValidationInfo) -> Self:
        """Validate other_upstream_id references in join functions.

        Ensures that the join function references a valid upstream ID that exists
        and is defined before the current transform.

        Args:
            info: Validation context information containing valid upstream IDs.

        Returns:
            Self: The validated instance.

        Raises:
            ValueError: If invalid other_upstream_id reference is found.
        """
        # Get validation context containing valid upstream IDs and transform ID
        if info.context is None:
            # No context provided, skip validation (used in isolated model creation)
            return self
        
        valid_upstream_ids = info.context.get("valid_upstream_ids")
        transform_id = info.context.get("transform_id")
        job_id = info.context.get("job_id", "unknown")
        
        if valid_upstream_ids is None or transform_id is None:
            # Context doesn't have required IDs, skip validation
            return self
        
        other_upstream_id = self.arguments.other_upstream_id
        
        # Check if other_upstream_id references the transform itself
        if other_upstream_id == transform_id:
            raise ValueError(
                f"Join function in transform '{transform_id}' references itself as other_upstream_id "
                f"in job '{job_id}'. A join cannot reference its own transform id."
            )
        
        # Check if other_upstream_id exists in valid upstream IDs
        if other_upstream_id not in valid_upstream_ids:
            raise ValueError(
                f"Join function in transform '{transform_id}' references other_upstream_id "
                f"'{other_upstream_id}' in job '{job_id}' which either does not exist or is defined "
                f"later in the transforms list. other_upstream_id must reference an existing extract or "
                f"a transform that appears before this one."
            )
        
        return self
