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

    # Class constants for validation limits
    DEFAULT_MAX_SIZE: int = 10 * 1024 * 1024  # 10 MB
    MIN_SIZE: int = 1  # 1 byte minimum
    ENCODING: str = "utf-8"

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

    def _file_exists(self) -> None:
        """Validate that the file exists.

        Raises:
            FileNotFoundError: If the file does not exist.
            OSError: If there's a system-level error checking file existence.
        """
        if not self.filepath.exists():
            raise FileNotFoundError(f"File not found: {self.filepath}")
        logger.debug("File existence validated: %s", self.filepath)

    def _is_file(self) -> None:
        """Validate that the path is a regular file.

        Raises:
            IsADirectoryError: If the path is a directory.
            OSError: If the path is not a regular file or there's a system-level error.
        """
        if not self.filepath.is_file():
            raise OSError(f"Path is not a file: {self.filepath}")
        logger.debug("File type validated: %s", self.filepath)

    def _read_permission(self) -> None:
        """Validate that the file has read permissions.

        Raises:
            PermissionError: If the file is not readable.
            OSError: If there's a system-level error checking permissions.
        """
        if not os.access(self.filepath, os.R_OK):
            raise PermissionError(f"Read permission denied for file: {self.filepath}")
        logger.debug("Read permission validated: %s", self.filepath)

    def _file_not_empty(self) -> None:
        """Validate that the file is not empty.

        Raises:
            OSError: If the file is empty or there's a system-level error accessing file metadata.
        """
        file_size = self.filepath.stat().st_size
        if file_size == 0:
            raise OSError(f"File is empty: {self.filepath}")
        logger.debug("File not empty validated: %s (size: %d bytes)", self.filepath, file_size)

    def _file_size_limits(self, max_size: int = DEFAULT_MAX_SIZE, min_size: int = MIN_SIZE) -> None:
        """Validate that the file size is within specified limits.

        Args:
            max_size: Maximum allowed file size in bytes.
            min_size: Minimum allowed file size in bytes.

        Raises:
            OSError: If the file size is outside the specified limits or there's a system-level error.
        """
        file_size = self.filepath.stat().st_size

        if file_size < min_size:
            raise OSError(f"File is too small: {self.filepath} (size: {file_size} bytes, minimum: {min_size} bytes)")

        if file_size > max_size:
            raise OSError(f"File is too large: {self.filepath} (size: {file_size} bytes, maximum: {max_size} bytes)")

        logger.debug(
            "File size validated: %s (size: %d, min: %d, max: %d)", self.filepath, file_size, min_size, max_size
        )

    def _text_file(self) -> None:
        """Validate that the file is a readable text file.

        Raises:
            OSError: If the file contains binary content or has encoding/access issues.
            PermissionError: If permission is denied reading the file.
        """
        try:
            with self.filepath.open("r", encoding=self.ENCODING) as file:
                # Read first 512 bytes to check for binary content
                sample = file.read(512)
                if "\x00" in sample:
                    raise OSError(f"File appears to contain binary content: {self.filepath}")
                logger.debug("Text file validation passed: %s", self.filepath)
        except UnicodeDecodeError as e:
            raise OSError(f"File encoding error (not valid UTF-8): {self.filepath}") from e

    def read(self) -> dict[str, Any]:
        """Read the file and return its contents as a dictionary.

        This method should be implemented by subclasses to handle specific file formats.

        Returns:
            dict[str, Any]: The contents of the file as a dictionary.

        Raises:
            FileNotFoundError: If the file does not exist.
            PermissionError: If permission is denied for accessing the file.
            OSError: If the file has invalid properties (empty, too large, wrong type, etc.).
            NotImplementedError: If the file extension is not supported.
        """
        logger.debug("Validating file: %s", self.filepath)
        self._file_exists()
        self._is_file()
        self._read_permission()
        self._file_not_empty()
        logger.info("Validation successful for file: %s", self.filepath)

        logger.debug("Reading file: %s", self.filepath)
        data = self._read()
        logger.info("File read successfully: %s", self.filepath)
        return data

    @abstractmethod
    def _read(self) -> dict[str, Any]:
        """Read the file and return its contents as a dictionary.

        This method should be overridden by subclasses to implement
        format-specific reading logic.

        Returns:
            dict[str, Any]: The contents of the file as a dictionary.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.
            FileNotFoundError: If the file does not exist.
            PermissionError: If permission is denied for accessing the file.
            ValueError: If the file content cannot be parsed.
            OSError: If there's a system-level error accessing the file.
        """


class FileYamlHandler(FileHandler):
    """Handles YAML files."""

    def _read(self) -> dict[str, Any]:
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

        try:
            logger.debug("Opening YAML file for reading: %s", self.filepath)
            with open(file=self.filepath, mode="r", encoding="utf-8") as file:
                data = yaml.safe_load(file)
                logger.info("Successfully read YAML file: %s", self.filepath)
                return data
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error in YAML file '{self.filepath}': {e}") from e


class FileJsonHandler(FileHandler):
    """Handles JSON files."""

    def _read(self) -> dict[str, Any]:
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

        try:
            logger.debug("Opening JSON file for reading: %s", self.filepath)
            with open(file=self.filepath, mode="r", encoding="utf-8") as file:
                data = json.load(file)
                logger.info("Successfully read JSON file: %s", self.filepath)
                return data
        except json.JSONDecodeError as e:
            raise e


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
