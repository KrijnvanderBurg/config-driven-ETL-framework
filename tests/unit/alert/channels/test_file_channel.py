"""Tests for FileChannel alert functionality.

These tests verify FileChannel creation, validation, alert sending,
and error handling scenarios.
"""

from pathlib import Path
from typing import Any
from unittest.mock import mock_open, patch

import pytest
from pydantic import ValidationError

from flint.alert.channels.file import FileChannel

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="valid_file_config")
def fixture_valid_file_config(tmp_path: Path) -> dict[str, Any]:
    """Provide a valid file channel configuration."""
    return {
        "name": "error-log-alerts",
        "description": "Error logging to file",
        "file_path": tmp_path / "alerts.log",
    }


# =========================================================================== #
# ========================== VALIDATION TESTS ============================= #
# =========================================================================== #


class TestFileChannelValidation:
    """Test FileChannel model validation and instantiation."""

    def test_create_file_channel__with_valid_config__succeeds(self, valid_file_config: dict[str, Any]) -> None:
        """Test FileChannel creation with valid configuration."""
        # Act
        channel = FileChannel(**valid_file_config)

        # Assert
        assert channel.name == "error-log-alerts"
        assert channel.description == "Error logging to file"
        assert channel.channel_id == "file"
        assert isinstance(channel.file_path, Path)
        assert channel.file_path.name == "alerts.log"

    def test_create_file_channel__with_missing_name__raises_validation_error(
        self, valid_file_config: dict[str, Any]
    ) -> None:
        """Test FileChannel creation fails when name is missing."""
        # Arrange
        del valid_file_config["name"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            FileChannel(**valid_file_config)

    def test_create_file_channel__with_empty_name__raises_validation_error(
        self, valid_file_config: dict[str, Any]
    ) -> None:
        """Test FileChannel creation fails when name is empty string."""
        # Arrange
        valid_file_config["name"] = ""

        # Assert
        with pytest.raises(ValidationError):
            # Act
            FileChannel(**valid_file_config)

    def test_create_file_channel__with_missing_file_path__raises_validation_error(
        self, valid_file_config: dict[str, Any]
    ) -> None:
        """Test FileChannel creation fails when file_path is missing."""
        # Arrange
        del valid_file_config["file_path"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            FileChannel(**valid_file_config)

    def test_create_file_channel__with_string_file_path__converts_to_path(
        self, valid_file_config: dict[str, Any]
    ) -> None:
        """Test FileChannel creation converts string file path to Path object."""
        # Arrange
        valid_file_config["file_path"] = "/tmp/alerts.log"

        # Act
        channel = FileChannel(**valid_file_config)

        # Assert
        assert isinstance(channel.file_path, Path)
        assert str(channel.file_path) == "/tmp/alerts.log"

    def test_create_file_channel__with_default_description__uses_empty_string(
        self, valid_file_config: dict[str, Any]
    ) -> None:
        """Test FileChannel creation uses empty description when not provided."""
        # Arrange
        del valid_file_config["description"]

        # Act
        channel = FileChannel(**valid_file_config)

        # Assert
        assert channel.description == ""


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="file_channel")
def fixture_file_channel(valid_file_config: dict[str, Any]) -> FileChannel:
    """Create FileChannel instance from valid configuration."""
    return FileChannel(**valid_file_config)


# =========================================================================== #
# ============================ ALERT TESTS ================================ #
# =========================================================================== #


class TestFileChannelAlert:
    """Test FileChannel alert functionality."""

    def test_alert__with_valid_inputs__writes_to_file_successfully(self, file_channel: FileChannel) -> None:
        """Test successful alert writing to file."""
        # Act
        file_channel.alert("Critical System Alert", "Database connection failed")

        # Assert
        assert file_channel.file_path.exists()
        content = file_channel.file_path.read_text(encoding="utf-8")
        assert content == "Critical System Alert: Database connection failed\n"

    def test_alert__with_multiple_alerts__appends_to_file(self, file_channel: FileChannel) -> None:
        """Test that multiple alerts are appended to the same file."""
        # Act
        file_channel.alert("First Alert", "First message")
        file_channel.alert("Second Alert", "Second message")

        # Assert
        content = file_channel.file_path.read_text(encoding="utf-8")
        expected = "First Alert: First message\nSecond Alert: Second message\n"
        assert content == expected

    def test_alert__with_special_characters__handles_encoding_correctly(self, file_channel: FileChannel) -> None:
        """Test alert handling with special characters in title and body."""
        # Act
        file_channel.alert("Ålørt: Üñíçødé Tëst", "Body with émojis 🔥 and spëcial chars: áéíóú")

        # Assert
        content = file_channel.file_path.read_text(encoding="utf-8")
        expected = "Ålørt: Üñíçødé Tëst: Body with émojis 🔥 and spëcial chars: áéíóú\n"
        assert content == expected

    def test_alert__with_empty_title__writes_with_empty_prefix(self, file_channel: FileChannel) -> None:
        """Test alert writing with empty title results in colon-prefixed body."""
        # Act
        file_channel.alert("", "Important alert body")

        # Assert
        content = file_channel.file_path.read_text(encoding="utf-8")
        assert content == ": Important alert body\n"

    def test_alert__with_empty_body__writes_with_title_only(self, file_channel: FileChannel) -> None:
        """Test alert writing with empty body results in title with colon."""
        # Act
        file_channel.alert("Alert Title", "")

        # Assert
        content = file_channel.file_path.read_text(encoding="utf-8")
        assert content == "Alert Title: \n"

    def test_alert__with_newlines_in_content__preserves_formatting(self, file_channel: FileChannel) -> None:
        """Test alert writing preserves newlines in title and body."""
        # Act
        file_channel.alert("Multi\nLine\nTitle", "Multi\nLine\nBody")

        # Assert
        content = file_channel.file_path.read_text(encoding="utf-8")
        expected = "Multi\nLine\nTitle: Multi\nLine\nBody\n"
        assert content == expected

    def test_alert__with_existing_file__appends_to_existing_content(self, file_channel: FileChannel) -> None:
        """Test alert appends to existing file content."""
        # Arrange
        existing_content = "Previous log entry\n"
        file_channel.file_path.write_text(existing_content, encoding="utf-8")

        # Act
        file_channel.alert("New Alert", "New message")

        # Assert
        content = file_channel.file_path.read_text(encoding="utf-8")
        expected = "Previous log entry\nNew Alert: New message\n"
        assert content == expected

    def test_alert__with_nonexistent_directory__raises_file_not_found_error(self, tmp_path: Path) -> None:
        """Test alert raises FileNotFoundError when parent directories don't exist."""
        # Arrange
        nested_path = tmp_path / "logs" / "alerts" / "system.log"
        config = {
            "name": "nested-alerts",
            "description": "Nested directory alerts",
            "file_path": nested_path,
        }
        channel = FileChannel(**config)

        # Assert
        with pytest.raises(FileNotFoundError):
            # Act
            channel.alert("Test Alert", "Test message")

    def test_alert__with_permission_error__raises_os_error(self, file_channel: FileChannel) -> None:
        """Test alert handling when file writing fails due to permissions."""
        # Mock open to simulate permission error
        with patch("builtins.open", mock_open()) as mock_file:
            mock_file.side_effect = PermissionError("Permission denied")

            # Assert
            with pytest.raises(PermissionError):
                # Act
                file_channel.alert("Test Alert", "Test Body")

    def test_alert__with_disk_full_error__raises_os_error(self, file_channel: FileChannel) -> None:
        """Test alert handling when file writing fails due to disk space."""
        # Mock open to simulate disk full error
        with patch("builtins.open", mock_open()) as mock_file:
            mock_file.side_effect = OSError(28, "No space left on device")

            # Assert
            with pytest.raises(OSError):
                # Act
                file_channel.alert("Test Alert", "Test Body")

    def test_alert__with_readonly_file__raises_os_error(self, file_channel: FileChannel) -> None:
        """Test alert handling when file is read-only."""
        # Arrange - create and make file read-only
        file_channel.file_path.touch()
        file_channel.file_path.chmod(0o444)

        try:
            # Assert
            with pytest.raises(PermissionError):
                # Act
                file_channel.alert("Test Alert", "Test Body")
        finally:
            # Cleanup - restore write permissions for cleanup
            file_channel.file_path.chmod(0o644)
