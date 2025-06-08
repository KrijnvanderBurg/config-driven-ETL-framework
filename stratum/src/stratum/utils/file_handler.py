"""
FileHandler strategy.


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
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import yaml


class FileHandlerStrategy(ABC):
    """Defines the interface for file handling strategies."""

    def __init__(self, filepath: str) -> None:
        """
        Initialize the file handling strategy.

        Args:
            filepath (str): The path to the file.
        """
        self.filepath: Path = Path(filepath)

    @abstractmethod
    def read(self) -> dict[str, Any]:
        """
        Abstract method to read the file and return its contents as a dictionary.

        Returns:
            dict[str, Any]: The contents of the file as a dictionary.

        Raises:
            FileNotFoundError: If the file does not exist.
            PermissionError: If permission is denied for accessing the file.
            Exception: Any other error that may occur during file reading.
        """
        raise NotImplementedError

    def _file_exists(self) -> bool:
        """
        Check if the file exists.

            bool: True if the file exists, False otherwise.
        """
        return self.filepath.exists()


class FileHandler:
    """
    Coordinates file handling strategies through implementations.

    Args:
        strategy (FileHandlerStrategy): The strategy to be used for file handling.
    """

    def __init__(self, strategy: FileHandlerStrategy) -> None:
        """
        Initialize FileHandler with a strategy.

        Args:
            strategy (FileHandlerStrategy): The strategy to be used for file handling.
        """
        self.strategy: FileHandlerStrategy = strategy

    def set_strategy(self, strategy: FileHandlerStrategy) -> None:
        """
        Set the file handling strategy.

        Args:
            strategy (FileHandlerStrategy): The strategy to be used for file handling.
        """
        self.strategy = strategy

    def read(self) -> dict[str, Any]:
        """
        Read the file using the set strategy and return its contents as a dictionary.

        Returns:
            dict[str, Any]: The contents of the file as a dictionary.
        """
        return self.strategy.read()

    @staticmethod
    def is_json(s: str) -> bool:
        """
        Checks if string is json.

        Args:
            s (str): The string.

        Returns:
            bool: True if string is json, False otherwise.
        """
        try:
            json.loads(s=s)
            return True
        except json.JSONDecodeError:
            return False

    # @staticmethod
    # def is_yaml(schema: str) -> bool:
    #     """
    #     Checks if string is YAML.

    #     Args:
    #         schema (str): The string.

    #     Returns:
    #         bool: True if string is YAML, False otherwise.
    #     """
    #     try:
    #         yaml.safe_load(stream=schema)
    #         return True
    #     except yaml.YAMLError:
    #         return False


class FileYamlHandler(FileHandlerStrategy):
    """Handles YAML files."""

    def __init__(self, filepath: str) -> None:
        """
        Initialize the YAML file handler.

        Args:
            filepath (str): The path to the YAML file.
        """
        super().__init__(filepath=filepath)

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
            raise FileNotFoundError(f"File '{self.filepath}' not found.")

        try:
            with open(file=self.filepath, mode="r", encoding="utf-8") as file:
                return yaml.safe_load(file)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"File '{self.filepath}' not found.") from e
        except PermissionError as e:
            raise PermissionError(f"Permission denied for file '{self.filepath}'.") from e
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error reading YAML file '{self.filepath}': {e}") from e


class FileJsonHandler(FileHandlerStrategy):
    """Handles JSON files."""

    def __init__(self, filepath: str) -> None:
        """
        Initialize the JSON file handler.

        Args:
            filepath (str): The path to the JSON file.
        """
        super().__init__(filepath=filepath)

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
            raise FileNotFoundError(f"File '{self.filepath}' not found.")

        try:
            with open(file=self.filepath, mode="r", encoding="utf-8") as file:
                return json.load(file)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"File '{self.filepath}' not found.") from e
        except PermissionError as e:
            raise PermissionError(f"Permission denied for file '{self.filepath}'.") from e
        except json.JSONDecodeError as e:
            # Using ValueError instead of JSONDecodeError due to complexity in supplying additional arguments.
            raise ValueError(f"Error decoding JSON file '{self.filepath}': {e}") from e


class FileHandlerContext:
    """Manages file handling contexts."""

    SUPPORTED_EXTENSIONS: dict[str, Any] = {
        ".yml": FileYamlHandler,
        ".yaml": FileYamlHandler,
        ".json": FileJsonHandler,
    }

    @staticmethod
    def get_strategy(filepath: str) -> FileHandlerStrategy:
        """
        Get the appropriate file handling strategy based on the file extension.

        Args:
            filepath (str): The path to the file.

        Returns:
            FileHandlerStrategy: An instance of the appropriate file handling strategy.

        Raises:
            NotImplementedError: If the file extension is not supported.
        """
        _, file_extension = os.path.splitext(filepath)
        strategy_class = FileHandlerContext.SUPPORTED_EXTENSIONS.get(file_extension)
        if strategy_class is None:
            raise NotImplementedError(f"File extension '{file_extension}' is not supported.")

        return strategy_class(filepath=filepath)

    @classmethod
    def factory(cls, filepath: str) -> FileHandler:
        """
        Get a file handler with the appropriate strategy based on the file extension.

        Args:
            filepath (str): The path to the file.

        Returns:
            FileHandler: An instance of FileHandler with the appropriate strategy set.
        """
        strategy_class = cls.get_strategy(filepath=filepath)
        strategy_instance = strategy_class  # Instantiate the strategy class
        file_handler = FileHandler(strategy=strategy_instance)

        return file_handler
