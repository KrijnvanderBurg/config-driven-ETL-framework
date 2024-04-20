"""
FileHandler strategy tests.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from unittest.mock import Mock, mock_open, patch

import pytest
import yaml

from datastore.utils.file_handler import FileHandler, FileHandlerContext, FileJsonHandler, FileYamlHandler


class TestFileHandlerStrategy:
    """FileHandlerStrategy is tested through its implementation classes tests."""


class TestFileHandler:
    """Tests for FileHandler class."""

    def test__init(self) -> None:
        """Test setting strategy for FileHandler."""
        # Arrange
        strategy_mock = Mock(spec=FileJsonHandler)

        # Act
        handler = FileHandler(strategy=strategy_mock)

        # Assert
        assert handler.strategy == strategy_mock

    def test__set_strategy(self) -> None:
        """Test that FileHandler can change strategy when already initialised."""
        # Arrange
        json_handler_mock = Mock(spec=FileJsonHandler)
        yaml_handler_mock = Mock(spec=FileYamlHandler)

        # Act
        handler = FileHandler(strategy=json_handler_mock)
        handler.set_strategy(strategy=yaml_handler_mock)

        # Assert
        assert handler.strategy == yaml_handler_mock

    def test__read__calls_strategy_read(self) -> None:
        """Test that FileHandler's read method calls the correct strategy's read method."""
        # Arrange
        strategy_mock = Mock(spec=FileJsonHandler)
        handler = FileHandler(strategy=strategy_mock)
        with (patch.object(FileJsonHandler, "read", return_value={"key": "value"}),):
            # Act
            handler.read()

            # Assert
            strategy_mock.read.assert_called_once()


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
            handler = FileYamlHandler(filepath="test.yaml")
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
                handler = FileYamlHandler(filepath="test.yaml")
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
                handler = FileYamlHandler(filepath="test.yaml")
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
                handler = FileYamlHandler(filepath="test.yaml")
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
                handler = FileYamlHandler(filepath="test.yaml")
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
            handler = FileJsonHandler(filepath="test.json")
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
                handler = FileJsonHandler(filepath="test.json")
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
                handler = FileJsonHandler(filepath="test.json")
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
                handler = FileJsonHandler(filepath="test.json")
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
                handler = FileJsonHandler(filepath="test.json")
                handler.read()


class TestFileHandlerContext:
    """Tests for FileHandlerContext class."""

    @pytest.mark.parametrize(
        "filepath, expected_strategy",
        [
            ("test.yml", FileYamlHandler),
            ("test.yaml", FileYamlHandler),
            ("test.json", FileJsonHandler),
        ],
    )
    def test_get_strategy(self, filepath: str, expected_strategy: type) -> None:
        """Test `get_strategy` returns the correct strategy for given file extension."""
        # Act
        strategy = FileHandlerContext.get_strategy(filepath=filepath)

        # Assert
        assert isinstance(strategy, expected_strategy)

    def test_get_strategy__unsupported_extension__raises_not_implemented_error(self):
        """Test `get_strategy` raises `NotImplementedError` for unsupported file extension."""
        with pytest.raises(NotImplementedError):  # Assert
            FileHandlerContext.get_strategy("path/fail.testfile")  # Act

    @pytest.mark.parametrize(
        "filepath, expected_strategy",
        [
            ("test.yml", FileYamlHandler),
            ("test.yaml", FileYamlHandler),
            ("test.json", FileJsonHandler),
        ],
    )
    def test_get(self, filepath: str, expected_strategy: type) -> None:
        """Test `get` returns the correct strategy for given file extension."""
        # Act
        file_handler = FileHandlerContext.factory(filepath=filepath)

        # Assert
        assert isinstance(file_handler, FileHandler)
        assert isinstance(file_handler.strategy, expected_strategy)

    def test_get__unsupported_extension__raises_not_implemented_error(self) -> None:
        """Test `get` raises `NotImplementedError` for unsupported file extension."""
        with pytest.raises(NotImplementedError):  # Assert
            # Act
            FileHandlerContext.factory(filepath="path/fail.testfile")
