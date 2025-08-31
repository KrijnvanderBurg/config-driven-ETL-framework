"""Schema handling utilities for working with PySpark schemas.

This module provides utilities for creating and managing PySpark schemas from
different source formats. It includes:

- Abstract base SchemaHandler class defining the schema handling interface
- Concrete implementations for different schema sources (JSON, dictionary, file)
- Factory pattern for dynamically selecting appropriate schema handlers
- Helper methods for schema validation and conversion

The schema utilities are primarily used in extraction and loading operations
where data structure definitions are required.
"""

import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from pyspark.sql.types import StructType

from flint.utils.file import FileHandler, FileHandlerContext
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class SchemaHandler(ABC):
    """Abstract base class for PySpark schema handling operations.

    Defines the common interface for creating PySpark StructType schemas
    from various source formats. All concrete schema handlers should
    inherit from this class and implement the required abstract method.
    """

    @staticmethod
    @abstractmethod
    def parse(schema: Any) -> StructType:
        """Create a PySpark schema from the provided source.

        Args:
            schema: The schema definition in a format specific to the handler
                implementation (could be string, dict, file path, etc.).
                Type: Any - varies depending on the specific handler implementation.

        Returns:
            StructType: The PySpark schema.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.
        """


class SchemaDictHandler(SchemaHandler):
    """Schema handler for creating PySpark schemas from dictionaries.

    This handler converts a Python dictionary representation of a schema
    into a PySpark StructType schema. The dictionary should follow the
    structure expected by the PySpark schema parser.

    Example:
        ```python
        schema_dict = {
            "fields": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "name", "type": "string", "nullable": True}
            ]
        }
        schema = SchemaDictHandler.parse(schema_dict)
        ```
    """

    @staticmethod
    def parse(schema: dict) -> StructType:
        """Convert a dictionary representation to a PySpark StructType schema.

        Args:
            schema: Dictionary containing the schema definition following
                PySpark's schema JSON format with fields array

        Returns:
            A fully configured PySpark StructType schema

        Raises:
            ValueError: If the dictionary format is invalid or cannot be
                converted to a valid schema
        """
        logger.debug("Parsing schema from dictionary with keys: %s", list(schema.keys()))

        try:
            struct_type = StructType.fromJson(json=schema)
            field_count = len(struct_type.fields)
            logger.info("Successfully parsed schema from dictionary - %d fields", field_count)
            logger.debug("Schema fields: %s", [f.name for f in struct_type.fields])
            return struct_type
        except (ValueError, TypeError, KeyError) as e:
            raise ValueError(f"Failed to convert dictionary to schema: {e}") from e


class SchemaStringHandler(SchemaHandler):
    """Schema handler for creating PySpark schemas from JSON strings.

    This handler converts a JSON string representation of a schema
    into a PySpark StructType schema. The string should contain a valid
    JSON object that follows the structure expected by the PySpark schema parser.

    Example:
        ```python
        schema_str = '''
        {
            "fields": [
                {"name": "id", "type": "integer", "nullable": false},
                {"name": "name", "type": "string", "nullable": true}
            ]
        }
        '''
        schema = SchemaStringHandler.parse(schema_str)
        ```
    """

    @staticmethod
    def parse(schema: str) -> StructType:
        """Convert a JSON string representation to a PySpark StructType schema.

        Args:
            schema: String containing a valid JSON schema definition

        Returns:
            A fully configured PySpark StructType schema

        Raises:
            ValueError: If the JSON string is invalid or cannot be
                converted to a valid schema
        """
        logger.debug("Parsing schema from JSON string (length: %d)", len(schema))

        try:
            logger.debug("Parsing JSON string to dictionary")
            parsed_json = json.loads(s=schema)
            logger.debug("Successfully parsed JSON string")

            result = SchemaDictHandler.parse(schema=parsed_json)
            logger.info("Successfully parsed schema from JSON string")
            return result

        except json.JSONDecodeError as e:
            error_msg = f"Invalid JSON schema format: {e}"
            logger.error("JSON parsing failed for schema string: %s", error_msg)
            raise ValueError(error_msg) from e


class SchemaFilepathHandler(SchemaHandler):
    """Schema handler for creating PySpark schemas from files.

    This handler reads a schema definition from a file (typically JSON)
    and converts it into a PySpark StructType schema. It uses the appropriate
    file handler for reading the file based on its extension.

    Example:
        ```python
        from pathlib import Path
        schema = SchemaFilepathHandler.parse(Path("schema.json"))
        ```
    """

    @staticmethod
    def parse(schema: Path) -> StructType:
        """Create a PySpark schema from a file.

        Reads the schema definition from a file and converts it to a PySpark
        StructType schema. The file is expected to contain a valid schema
        definition in a format supported by one of the file handlers.

        Args:
            schema: Path object pointing to the schema definition file

        Returns:
            A fully configured PySpark StructType schema

        Raises:
            FileNotFoundError: If the schema file doesn't exist
            PermissionError: If there's no permission to read the file
            ValueError: If the file content can't be parsed as a valid schema
        """
        logger.info("Parsing schema from file: %s", schema)

        try:
            logger.debug("Creating file handler for schema file: %s", schema)
            file_handler: FileHandler = FileHandlerContext.from_filepath(filepath=schema)

            logger.debug("Reading schema file content")
            file_content = file_handler.read()

            logger.debug("Converting file content to schema")
            result = SchemaDictHandler.parse(schema=file_content)

            logger.info("Successfully parsed schema from file: %s", schema)
            return result

        except (FileNotFoundError, PermissionError, NotImplementedError) as e:
            logger.error("File access error reading schema file %s: %s", schema, str(e))
            raise e
