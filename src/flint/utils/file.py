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
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import yaml


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
        self.filepath = filepath

    def _file_exists(self) -> bool:
        """
        Check if the file exists.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        return self.filepath.exists()

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
        if not self._file_exists():
            raise FileNotFoundError("File '%s' not found." % self.filepath)

        try:
            with open(file=self.filepath, mode="r", encoding="utf-8") as file:
                return yaml.safe_load(file)
        except FileNotFoundError as e:
            raise FileNotFoundError("File '%s' not found." % self.filepath) from e
        except PermissionError as e:
            raise PermissionError("Permission denied for file '%s'." % self.filepath) from e
        except yaml.YAMLError as e:
            raise yaml.YAMLError("Error in YAML file '%s': %s" % (self.filepath, e)) from e


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
        if not self._file_exists():
            raise FileNotFoundError("File '%s' not found." % self.filepath)

        try:
            with open(file=self.filepath, mode="r", encoding="utf-8") as file:
                return json.load(file)
        except FileNotFoundError as e:
            raise FileNotFoundError("File '%s' not found." % self.filepath) from e
        except PermissionError as e:
            raise PermissionError("Permission denied for file '%s'." % self.filepath) from e
        except json.JSONDecodeError as e:
            # Using ValueError instead of JSONDecodeError due to complexity in supplying additional arguments.
            raise ValueError("Error decoding JSON file '%s': %s" % (self.filepath, e)) from e


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
        _, file_extension = os.path.splitext(filepath)
        handler_class = cls.SUPPORTED_EXTENSIONS.get(file_extension)

        if handler_class is None:
            supported_extensions = ", ".join(cls.SUPPORTED_EXTENSIONS.keys())
            raise NotImplementedError(
                f"File extension '{file_extension}' is not supported. "
                f"Supported extensions are: {supported_extensions}"
            )

        return handler_class(filepath=filepath)
