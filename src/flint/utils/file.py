"""File handling utilities for reading and validating configuration files.

This module provides a factory implementation for handling different file formats
like JSON, YAML, etc. with a common interface. It includes:

- Abstract base FileHandler class defining the file handling interface
- Concrete implementations for different file formats (JSON, YAML, etc.)
- Factory pattern for dynamically selecting appropriate file handlers
- Validation utilities to ensure files exist and have correct format

The file handlers are primarily used for loading ETL pipeline configurations,
but can be used for any structured data file reading needs.
"""

import json
import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import yaml

from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class FileHandler(ABC):
    """Abstract base class for file handling operations.

    Provides a common interface for file operations like checking existence,
    reading content, and validating format across different file types.

    All concrete file handlers should inherit from this class and implement
    the required abstract methods.

    Attributes:
        filepath: Path to the file being handled
    """

    def __init__(self, filepath: Path) -> None:
        """Initialize the file handler with a file path.

        Args:
            filepath: Path object pointing to the target file

        Note:
            The file is not accessed during initialization,
            only when operations are performed.
        """
        logger.debug("Initializing FileHandler for path: %s", filepath)
        self.filepath = filepath
        logger.debug("FileHandler initialized for: %s", filepath)

    def _file_exists(self) -> bool:
        """
        Check if the file exists.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        exists = self.filepath.exists()
        logger.debug("File existence check for %s: %s", self.filepath, exists)
        return exists

    @abstractmethod
    def read(self) -> dict[str, Any]:
        """
        Read the file and return its contents as a dictionary.
        This method should be overridden by subclasses.

        Returns:
            dict[str, Any]: The contents of the file as a dictionary.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.
        """


class FileYamlHandler(FileHandler):
    """Handles YAML files."""

    def read(self) -> dict[str, Any]:
        """
        Read the YAML file and return its contents as a dictionary.

        Returns:
            dict[str, Any]: The contents of the YAML file as a dictionary.

        Raises:
            FileNotFoundError: If the file does not exist.
            PermissionError: If permission is denied for accessing the file.
            yaml.YAMLError: If there is an error reading the YAML file.
        """
        logger.info("Reading YAML file: %s", self.filepath)

        if not self._file_exists():
            error_msg = f"File '{self.filepath}' not found."
            logger.error("YAML file not found: %s", self.filepath)
            raise FileNotFoundError(error_msg)

        try:
            logger.debug("Opening YAML file for reading: %s", self.filepath)
            with open(file=self.filepath, mode="r", encoding="utf-8") as file:
                data = yaml.safe_load(file)
                logger.info("Successfully read YAML file: %s", self.filepath)
                return data
        except FileNotFoundError as e:
            raise FileNotFoundError(f"File '{self.filepath}' not found.") from e
        except PermissionError as e:
            raise PermissionError(f"Permission denied for file '{self.filepath}'.") from e
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error in YAML file '{self.filepath}': {e}") from e


class FileJsonHandler(FileHandler):
    """Handles JSON files."""

    def read(self) -> dict[str, Any]:
        """
        Read the JSON file and return its contents as a dictionary.

        Returns:
            dict[str, Any]: The contents of the JSON file as a dictionary.

        Raises:
            FileNotFoundError: If the file does not exist.
            PermissionError: If permission is denied for accessing the file.
            json.JSONDecodeError: If there is an error decoding the JSON file.
            ValueError: If JSON cannot be decoded.
        """
        logger.info("Reading JSON file: %s", self.filepath)

        if not self._file_exists():
            raise FileNotFoundError(f"File '{self.filepath}' not found.")

        try:
            logger.debug("Opening JSON file for reading: %s", self.filepath)
            with open(file=self.filepath, mode="r", encoding="utf-8") as file:
                data = json.load(file)
                logger.info("Successfully read JSON file: %s", self.filepath)
                return data
        except FileNotFoundError as e:
            raise FileNotFoundError(f"File '{self.filepath}' not found.") from e
        except PermissionError as e:
            raise PermissionError(f"Permission denied for file '{self.filepath}'.") from e
        except json.JSONDecodeError as e:
            # Using ValueError instead of JSONDecodeError due to complexity in supplying additional arguments.
            raise ValueError(f"Error decoding JSON file '{self.filepath}': {e}") from e


class FileHandlerContext:
    """Factory for creating appropriate file handlers."""

    SUPPORTED_EXTENSIONS: dict[str, type[FileHandler]] = {
        ".yml": FileYamlHandler,
        ".yaml": FileYamlHandler,
        ".json": FileJsonHandler,
    }

    @classmethod
    def from_filepath(cls, filepath: Path) -> FileHandler:
        """
        Create and return the appropriate file handler based on file extension.

        Args:
            filepath (Path): The path to the file.

        Returns:
            FileHandler: An instance of the appropriate file handler.

        Raises:
            NotImplementedError: If the file extension is not supported.
        """
        logger.debug("Creating file handler for path: %s", filepath)
        _, file_extension = os.path.splitext(filepath)
        logger.debug("Detected file extension: %s", file_extension)

        handler_class = cls.SUPPORTED_EXTENSIONS.get(file_extension)

        if handler_class is None:
            supported_extensions = ", ".join(cls.SUPPORTED_EXTENSIONS.keys())
            raise NotImplementedError(
                f"File extension '{file_extension}' is not supported. Supported extensions are: {supported_extensions}"
            )

        logger.debug("Selected handler class: %s", handler_class.__name__)
        handler = handler_class(filepath=filepath)
        logger.info("Created file handler: %s for %s", handler_class.__name__, filepath)
        return handler
