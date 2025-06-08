"""
Module to take care of creating a singleton of the execution environment class.


==============================================================================
Copyright Krijn van der Burg. All rights reserved.

This software is proprietary and confidential. No reproduction, distribution,
or transmission is allowed without prior written permission. Unauthorized use,
disclosure, or distribution is strictly prohibited.

For inquiries and permission requests, contact Krijn van der Burg at
krijnvdburg@protonmail.com.
==============================================================================
"""

import json
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, Generic, TypeVar

from pyspark.sql.types import StructType

from stratum.utils.file_handler import FileHandler, FileHandlerContext
from stratum.utils.log_handler import set_logger

logger: logging.Logger = set_logger(__name__)

SchemaT = TypeVar("SchemaT", bound=StructType)


class SchemaHandlerAbstract(Generic[SchemaT], ABC):
    @staticmethod
    @abstractmethod
    def schema_factory(schema: str) -> SchemaT: ...

    @staticmethod
    @abstractmethod
    def from_json(schema: str) -> SchemaT: ...

    @staticmethod
    @abstractmethod
    def from_file(filepath: str) -> SchemaT: ...


class SchemaHandlerPyspark(SchemaHandlerAbstract):
    @staticmethod
    def schema_factory(schema: str) -> StructType | None:
        """
        Get the appropriate schema handler based on the schema.

        Args:
            schema (str): The schema attribute string.

        Returns:
            StructType: The schema handler object.

        Raises:
            NotImplementedError: If the schema value is not recognized or not supported.
        """
        if schema == "":
            return None

        supported_extensions: dict[str, Callable[[str], StructType]] = {
            ".json": SchemaHandlerPyspark.from_file,
        }

        for key, callable_ in supported_extensions.items():
            if schema.endswith(key):
                return callable_(schema)

        if FileHandler.is_json(schema):
            return SchemaHandlerPyspark.from_json(schema=schema)

        raise NotImplementedError(f"No schema handling strategy recognised or supported for value: {schema}")

    @staticmethod
    def from_dict(schema: dict[str, Any]) -> StructType:
        """
        Read JSON schema string.

        Args:
            schema (str): schema json.

        Returns:
            StructType: The test JSON schema.

        Raises:
            ValueError: If there's an error decoding the JSON schema.
        """
        try:
            return StructType.fromJson(json=schema)
        except json.JSONDecodeError as e:
            # Using ValueError instead of JSONDecodeError due to complexity in supplying additional arguments.
            raise ValueError(f"Error decoding JSON schema '{schema}': {e}") from e

    @staticmethod
    def from_json(schema: str) -> StructType:
        """
        Read JSON schema string.

        Args:
            schema (str): schema json.

        Returns:
            StructType: The test JSON schema.

        Raises:
            ValueError: If there's an error decoding the JSON schema.
        """
        try:
            json_content = json.loads(s=schema)
            return SchemaHandlerPyspark.from_dict(schema=json_content)
        except json.JSONDecodeError as e:
            # Using ValueError instead of JSONDecodeError due to complexity in supplying additional arguments.
            raise ValueError(f"Error decoding JSON schema '{schema}': {e}") from e

    @staticmethod
    def from_file(filepath: str) -> StructType:
        """
        Read JSON schema file.

        Args:
            filepath (str): path to schema file.

        Returns:
            StructType: The test JSON schema.

        Raises:
            FileNotFoundError: If the schema file is not found.
            PermissionError: If permission is denied for accessing the schema file.
            ValueError: If there's an error decoding the JSON schema.
        """

        file_handler: FileHandler = FileHandlerContext.factory(filepath=filepath)
        file_content = file_handler.read()
        schema = SchemaHandlerPyspark.from_dict(schema=file_content)
        return schema
