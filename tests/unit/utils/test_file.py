"""Unit tests for the file handling utility module.

This module contains tests for the file utilities that are responsible
for reading and parsing configuration files in various formats like JSON and YAML.

The tests verify that:
- Files in different formats can be correctly read and parsed
- Error conditions like missing files are properly handled
- The factory pattern correctly selects the appropriate handler for each file type
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from flint.utils.file import FileHandlerContext, FileJsonHandler, FileYamlHandler


class TestYamlHandler:
    """Unit tests for FileYamlHandler.

    Tests cover:
        - Reading YAML data from files
        - Handling missing files
        - Error handling for invalid YAML content
    """

    @pytest.fixture
    def temp_yaml_file(self) -> Path:
        """Create a temporary YAML file with valid content."""
        yaml_content = "key: value"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as temp_file:
            temp_file.write(yaml_content)
            temp_file.flush()
            return Path(temp_file.name)

    def test_read(self, temp_yaml_file: Path) -> None:
        """Test reading YAML data from a file."""
        # Act
        handler = FileYamlHandler(filepath=temp_yaml_file)
        data = handler.read()

        # Assert
        assert data == {"key": "value"}

    def test_read__file_not_found_error(self) -> None:
        """
        Test reading YAML file raises `FileNotFoundError`
            when file does not exist while `self._file_exists() returns true.
        """
        # Arrange
        non_existent_path = Path("non_existent_file.yaml")

        # Act & Assert
        handler = FileYamlHandler(filepath=non_existent_path)
        with pytest.raises(FileNotFoundError):
            handler.read()

    def test_read__file_permission_error(self, temp_yaml_file: Path) -> None:
        """Test reading YAML file raises `PermissionError` when `builtins.open()` raises `PermissionError`."""
        with patch("builtins.open", side_effect=PermissionError):
            # Act & Assert
            handler = FileYamlHandler(filepath=temp_yaml_file)
            with pytest.raises(PermissionError):
                handler.read()

    def test_read__invalid_yaml(self) -> None:
        """Test reading invalid YAML content raises yaml.YAMLError."""
        # Arrange
        invalid_yaml = "key: value:"  # colon `:` after value: is invalid yaml.
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as temp_file:
            temp_file.write(invalid_yaml)
            temp_file.flush()
            temp_path = Path(temp_file.name)

        # Act & Assert
        handler = FileYamlHandler(filepath=temp_path)
        with pytest.raises(yaml.YAMLError):
            handler.read()


class TestJsonHandler:
    """Tests for FileJsonHandler class."""

    @pytest.fixture
    def temp_json_file(self) -> Path:
        """Create a temporary JSON file with valid content."""
        json_content = {"key": "value"}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as temp_file:
            json.dump(json_content, temp_file)
            temp_file.flush()
            return Path(temp_file.name)

    def test_read(self, temp_json_file: Path) -> None:
        """Test reading JSON data from a file."""
        # Act
        handler = FileJsonHandler(filepath=temp_json_file)
        data = handler.read()

        # Assert
        assert data == {"key": "value"}

    def test_read__file_not_exists(self) -> None:
        """Test reading JSON file raises `FileNotFoundError` when `self._file_exists()` returns false."""
        with pytest.raises(FileNotFoundError):  # Assert
            # Act
            handler = FileJsonHandler(filepath=Path("non_existent.json"))
            handler.read()

    def test_read__file_not_found_error(self) -> None:
        """
        Test reading JSON file raises `FileNotFoundError`
            when file does not exist while `self._file_exists() returns true.
        """
        with pytest.raises(FileNotFoundError):  # Assert
            # Act
            handler = FileJsonHandler(filepath=Path("test.json"))
            handler.read()

    def test_read__file_permission_error(self, temp_json_file: Path) -> None:
        """Test reading JSON file raises `PermissionError` when `builtins.open()` raises `PermissionError`."""
        with patch("builtins.open", side_effect=PermissionError):
            with pytest.raises(PermissionError):  # Assert
                # Act
                handler = FileJsonHandler(filepath=temp_json_file)
                handler.read()

    def test_read__json_decode_error(self) -> None:
        """Test reading invalid JSON content raises json.JSONDecodeError."""
        # Arrange
        invalid_json = '{"name": "John" "age": 30}'  # Missing comma makes it invalid JSON.
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as temp_file:
            temp_file.write(invalid_json)
            temp_file.flush()
            temp_path = Path(temp_file.name)

        # Act & Assert
        handler = FileJsonHandler(filepath=temp_path)
        with pytest.raises(json.JSONDecodeError):
            handler.read()


class TestFileHandlerFactory:
    """Tests for FileHandlerFactory class."""

    @pytest.mark.parametrize(
        "filepath, expected_handler_class",
        [
            (Path("test.yml"), FileYamlHandler),
            (Path("test.yaml"), FileYamlHandler),
            (Path("test.json"), FileJsonHandler),
        ],
    )
    def test_create_handler(self, filepath: Path, expected_handler_class: type) -> None:
        """Test `create_handler` returns the correct handler for given file extension."""
        # Act
        handler = FileHandlerContext.from_filepath(filepath=filepath)

        # Assert
        assert isinstance(handler, expected_handler_class)

    def test_create_handler__unsupported_extension__raises_not_implemented_error(self) -> None:
        """Test `create_handler` raises `NotImplementedError` for unsupported file extension."""
        with pytest.raises(NotImplementedError):  # Assert
            FileHandlerContext.from_filepath(Path("path/fail.testfile"))  # Act
