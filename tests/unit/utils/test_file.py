from pathlib import Path
from unittest.mock import mock_open, patch

import pytest
import yaml

from ingestion_framework.utils.file import FileHandlerContext, FileJsonHandler, FileYamlHandler


class TestYamlHandler:
    """Tests for FileYamlHandler class."""

    def test_read(self) -> None:
        """Test reading YAML data from a file."""
        # Arrange
        with (
            patch("builtins.open", mock_open(read_data="key: value")),
            patch.object(FileYamlHandler, "_file_exists", return_value=True),
        ):
            # Act
            handler = FileYamlHandler(filepath=Path("test.yaml"))
            data = handler.read()

            # Assert
            assert data == {"key": "value"}

    def test_read__file_not_exists(self) -> None:
        """Test reading YAML file raises `FileNotFoundError` when `self._file_exists()` returns false."""
        # Arrange
        with (
            patch("builtins.open", side_effect=FileNotFoundError),
            # patch.object(FileYamlHandler, "_file_exists", return_value=False),
        ):
            with pytest.raises(FileNotFoundError):  # Assert
                # Act
                handler = FileYamlHandler(filepath=Path("test.yaml"))
                handler.read()

    def test_read__file_not_found_error(self) -> None:
        """
        Test reading YAML file raises `FileNotFoundError`
            when file does not exist while `self._file_exists() returns true.
        """
        # Arrange
        with (
            patch("builtins.open", side_effect=FileNotFoundError),
            # patch.object(FileYamlHandler, "_file_exists", return_value=True),
        ):
            with pytest.raises(FileNotFoundError):  # Assert
                # Act
                handler = FileYamlHandler(filepath=Path("test.yaml"))
                handler.read()

    def test_read__file_permission_error(self) -> None:
        """Test reading YAML file raises `PermissionError` when `builtins.open()` raises `PermissionError`."""
        # Arrange
        with (
            patch("builtins.open", side_effect=PermissionError),
            patch.object(FileYamlHandler, "_file_exists", return_value=True),
        ):
            with pytest.raises(PermissionError):  # Assert
                # Act
                handler = FileYamlHandler(filepath=Path("test.yaml"))
                handler.read()

    def test_read__yaml_error(self) -> None:
        """Test reading YAML file raises `YAMLError` when file contains invalid yaml."""
        # Arrange
        invalid_yaml = "key: value:"  # colon `:` after value: is invalid yaml.
        with (
            patch("builtins.open", mock_open(read_data=invalid_yaml)),
            patch.object(FileYamlHandler, "_file_exists", return_value=True),
        ):
            with pytest.raises(yaml.YAMLError):  # Assert
                # Act
                handler = FileYamlHandler(filepath=Path("test.yaml"))
                handler.read()


class TestJsonHandler:
    """Tests for FileJsonHandler class."""

    def test_read(self) -> None:
        """Test reading JSON data from a file."""
        # Arrange
        with (
            patch("builtins.open", mock_open(read_data='{"key": "value"}')),
            patch.object(FileJsonHandler, "_file_exists", return_value=True),
        ):
            # Act
            handler = FileJsonHandler(filepath=Path("test.json"))
            data = handler.read()

            # Assert
            assert data == {"key": "value"}

    def test_read__file_not_exists(self) -> None:
        """Test reading JSON file raises `FileNotFoundError` when `self._file_exists()` returns false."""
        # Arrange
        with (
            patch("builtins.open", side_effect=FileNotFoundError),
            patch.object(FileJsonHandler, "_file_exists", return_value=False),
        ):
            with pytest.raises(FileNotFoundError):  # Assert
                # Act
                handler = FileJsonHandler(filepath=Path("test.json"))
                handler.read()

    def test_read__file_not_found_error(self) -> None:
        """
        Test reading JSON file raises `FileNotFoundError`
            when file does not exist while `self._file_exists() returns true.
        """
        # Arrange
        with (
            patch("builtins.open", side_effect=FileNotFoundError),
            patch.object(FileJsonHandler, "_file_exists", return_value=True),
        ):
            with pytest.raises(FileNotFoundError):  # Assert
                # Act
                handler = FileJsonHandler(filepath=Path("test.json"))
                handler.read()

    def test_read__file_permission_error(self) -> None:
        """Test reading JSON file raises `PermissionError` when `builtins.open()` raises `PermissionError`."""
        # Arrange
        with (
            patch("builtins.open", side_effect=PermissionError),
            patch.object(FileJsonHandler, "_file_exists", return_value=True),
        ):
            with pytest.raises(PermissionError):  # Assert
                # Act
                handler = FileJsonHandler(filepath=Path("test.json"))
                handler.read()

    def test_read__json_decode_error(self) -> None:
        """Test reading JSON file raises `JSONDecodeError` when file contains invalid JSON."""
        # Arrange
        invalid_json = '{"name": "John" "age": 30}'  # Missing comma makes it invalid JSON.
        with (
            patch("builtins.open", mock_open(read_data=invalid_json)),
            patch.object(FileJsonHandler, "_file_exists", return_value=True),
        ):
            with pytest.raises(ValueError):  # Assert
                # Act
                handler = FileJsonHandler(filepath=Path("test.json"))
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
