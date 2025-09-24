"""Configuration model for the join transform function.

This module defines the data models used to configure join
transformations in the ingestion framework. It includes:

- JoinFunctionModel: Main configuration model for join operations
- JoinArgs: Container for the join parameters

These models provide a type-safe interface for configuring joins
from configuration files or dictionaries.
"""

import logging

from flint.runtime.jobs.models.model_transform import ArgsModel, FunctionModel
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class JoinArgs(ArgsModel):
    """Arguments for join transform operations.

    Attributes:
        other_upstream_name: Name of the dataframe to join with the current dataframe
        on: Column(s) to join on. Can be a string for a single column or a list of strings for multiple columns
        how: Type of join to perform (inner, outer, left, right, etc.). Defaults to "inner"
    """

    other_upstream_name: str
    on: str | list[str]
    how: str = "inner"


class JoinFunctionModel(FunctionModel[JoinArgs]):
    """Configuration model for join transform operations.

    This model defines the structure for configuring a join
    transformation, specifying the dataframes to join and how to join them.

    Attributes:
        function: The name of the function to be used (always "join")
        arguments: Container for the join parameters
    """

    function: str
    arguments: JoinArgs
