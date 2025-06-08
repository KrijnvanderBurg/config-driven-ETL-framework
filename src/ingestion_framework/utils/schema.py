"""
Schema handling utilities for working with PySpark schemas.

This module provides a factory implementation for handling schemas from different
sources like JSON strings, dictionaries, and files with a common interface.
"""

import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path

from pyspark.sql.types import StructType

from ingestion_framework.utils.file import FileHandler, FileHandlerContext
from ingestion_framework.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class SchemaHandler(ABC):
    """
    Base schema handler class for creating PySpark schemas.
    """

    @staticmethod
    @abstractmethod
    def parse(schema) -> StructType:
        """
        Create a PySpark schema from the source.
        This method should be overridden by subclasses.

        Returns:
            StructType: The PySpark schema.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.
        """


class SchemaDictHandler(SchemaHandler):
    """Handles schema creation from dictionaries."""

    @staticmethod
    def parse(schema: dict) -> StructType:
        """
        Converts a dictionary to StructType schema.

        Returns:
            StructType: The PySpark schema.

        Raises:
            ValueError: If there's an error creating the schema.
        """
        try:
            return StructType.fromJson(json=schema)
        except Exception as e:
            raise ValueError(f"Failed to convert dictionary to schema: {e}") from e


class SchemaStringHandler(SchemaHandler):
    """Handles schema creation from JSON strings."""

    @staticmethod
    def parse(schema: str) -> StructType:
        """
        Parses a JSON string into a PySpark schema.

        Returns:
            StructType: The PySpark schema.

        Raises:
            ValueError: If there's an error parsing the JSON schema.
        """
        try:
            parsed_json = json.loads(s=schema)
            return SchemaDictHandler.parse(schema=parsed_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON schema format: {e}") from e


class SchemaFilepathHandler(SchemaHandler):
    """Handles schema creation from files."""

    @staticmethod
    def parse(schema: Path) -> StructType:
        """
        Loads a schema from a file.

        Returns:
            StructType: The PySpark schema.

        Raises:
            FileNotFoundError: If the schema file is not found.
            PermissionError: If permission is denied for accessing the schema file.
            ValueError: If there's an error parsing the file content.
        """
        file_handler: FileHandler = FileHandlerContext.from_filepath(filepath=schema)
        file_content = file_handler.read()
        return SchemaDictHandler.parse(schema=file_content)
