"""Configuration model for the column selection transform function.

This module defines the data models used to configure column selection
transformations in the ingestion framework. It includes:

- SelectFunctionModel: Main configuration model for select operations
- Args nested class: Container for the selection parameters

These models provide a type-safe interface for configuring column selections
from configuration files or dictionaries.
"""

import logging
from dataclasses import dataclass
from typing import Any, Final, Self

from pyspark.sql.column import Column

from flint.exceptions import DictKeyError
from flint.models.model_transform import ARGUMENTS, FUNCTION, FunctionModel
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

COLUMNS: Final[str] = "columns"


@dataclass
class SelectFunctionModel(FunctionModel):
    """Configuration model for column selection transform operations.

    This model defines the structure for configuring a column selection
    transformation, specifying which columns should be included in the output.

    Attributes:
        function: The name of the function to be used (always "select")
        arguments: Container for the column selection parameters
    """

    function: str
    arguments: "SelectFunctionModel.Args"

    @dataclass
    class Args:
        """Arguments for column selection transform operations.

        Attributes:
            columns: List of column names to select from the DataFrame
        """

        columns: list[Column]

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """
        Create a SelectFunctionModel from a dictionary.

        Args:
            dict_: The configuration dictionary.

        Returns:
            An initialized SelectFunctionModel.
        """
        logger.debug("Creating SelectFunctionModel from dictionary: %s", dict_)

        try:
            function_name = dict_[FUNCTION]
            arguments_dict = dict_[ARGUMENTS]

            columns = arguments_dict[COLUMNS]
            logger.debug("Parsed select function - name: %s, columns: %s", function_name, columns)

            arguments = cls.Args(columns=columns)

        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=dict_) from e

        model = cls(function=function_name, arguments=arguments)
        logger.info("Successfully created SelectFunctionModel with %d columns", len(columns))
        return model
