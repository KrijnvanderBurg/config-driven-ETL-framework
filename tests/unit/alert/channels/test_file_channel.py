"""Unit tests for the FileAlertChannel class.

This module contains comprehensive tests for the FileAlertChannel functionality,
including file writing, error handling, and configuration parsing.
"""

import tempfile
from pathlib import Path

import pytest

from flint.alert.channels.file import FileAlertChannel
from flint.exceptions import FlintConfigurationKeyError


class TestFileAlertChannel:
    """Test cases for FileAlertChannel class."""

    @pytest.fixture
    def file_channel_config(self) -> dict:
        """Provide a file channel configuration for testing."""
        return {"file_path": "/tmp/test-alerts.log"}

    @pytest.fixture
    def file_channel(self, file_channel_config) -> FileAlertChannel:
        """Create a FileAlertChannel instance for testing."""
        return FileAlertChannel.from_dict(file_channel_config)

    def test_from_dict_creates_channel_correctly(self, file_channel_config) -> None:
        """Test that from_dict creates a FileAlertChannel correctly."""
        channel = FileAlertChannel.from_dict(file_channel_config)

        assert channel.file_path == "/tmp/test-alerts.log"

    def test_from_dict_raises_error_for_missing_file_path(self) -> None:
        """Test that from_dict raises error when file_path is missing."""
        config = {}

        with pytest.raises(FlintConfigurationKeyError):
            FileAlertChannel.from_dict(config)

    def test_alert_writes_to_file_successfully(self) -> None:
        """Test that _alert writes alert to file successfully."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".log") as temp_file:
            temp_file_path = temp_file.name

        try:
            # Create channel with temporary file
            channel = FileAlertChannel(file_path=temp_file_path)

            # Send an alert
            channel._alert("Test Alert", "This is a test alert message")

            # Verify the content was written
            with open(temp_file_path, "r", encoding="utf-8") as file:
                content = file.read()

            expected_content = "Test Alert: This is a test alert message\n"
            assert content == expected_content

        finally:
            Path(temp_file_path).unlink(missing_ok=True)

    def test_alert_appends_to_existing_file(self) -> None:
        """Test that _alert appends to existing file content."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".log") as temp_file:
            temp_file.write("Existing content\n")
            temp_file_path = temp_file.name

        try:
            # Create channel with existing file
            channel = FileAlertChannel(file_path=temp_file_path)

            # Send an alert
            channel._alert("New Alert", "New alert message")

            # Verify both old and new content exist
            with open(temp_file_path, "r", encoding="utf-8") as file:
                content = file.read()

            expected_content = "Existing content\nNew Alert: New alert message\n"
            assert content == expected_content

        finally:
            Path(temp_file_path).unlink(missing_ok=True)

    def test_alert_creates_file_if_not_exists(self) -> None:
        """Test that _alert creates file if it doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file_path = Path(temp_dir) / "new_alerts.log"

            # Ensure file doesn't exist
            assert not temp_file_path.exists()

            # Create channel with non-existent file
            channel = FileAlertChannel(file_path=str(temp_file_path))

            # Send an alert
            channel._alert("First Alert", "First alert message")

            # Verify file was created and content written
            assert temp_file_path.exists()

            with open(temp_file_path, "r", encoding="utf-8") as file:
                content = file.read()

            expected_content = "First Alert: First alert message\n"
            assert content == expected_content

    def test_alert_handles_empty_title_and_body(self) -> None:
        """Test that _alert handles empty title and body gracefully."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".log") as temp_file:
            temp_file_path = temp_file.name

        try:
            channel = FileAlertChannel(file_path=temp_file_path)

            # Send alert with empty title and body
            channel._alert("", "")

            # Verify the content
            with open(temp_file_path, "r", encoding="utf-8") as file:
                content = file.read()

            expected_content = ": \n"
            assert content == expected_content

        finally:
            Path(temp_file_path).unlink(missing_ok=True)

    def test_alert_handles_special_characters(self) -> None:
        """Test that _alert handles special characters correctly."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".log") as temp_file:
            temp_file_path = temp_file.name

        try:
            channel = FileAlertChannel(file_path=temp_file_path)

            # Send alert with special characters
            title = "Émergence Crítica!"
            body = "Message with special chars: àáâãäåæçèé & symbols: @#$%^&*()"
            channel._alert(title, body)

            # Verify the content
            with open(temp_file_path, "r", encoding="utf-8") as file:
                content = file.read()

            expected_content = f"{title}: {body}\n"
            assert content == expected_content

        finally:
            Path(temp_file_path).unlink(missing_ok=True)

    def test_alert_handles_multiline_messages(self) -> None:
        """Test that _alert handles multiline messages correctly."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".log") as temp_file:
            temp_file_path = temp_file.name

        try:
            channel = FileAlertChannel(file_path=temp_file_path)

            # Send alert with multiline body
            title = "Multi-line Alert"
            body = "Line 1\nLine 2\nLine 3"
            channel._alert(title, body)

            # Verify the content
            with open(temp_file_path, "r", encoding="utf-8") as file:
                content = file.read()

            expected_content = f"{title}: {body}\n"
            assert content == expected_content

        finally:
            Path(temp_file_path).unlink(missing_ok=True)

    def test_alert_raises_error_for_invalid_path(self) -> None:
        """Test that _alert raises OSError for invalid file paths."""
        # Use an invalid path (directory that doesn't exist)
        channel = FileAlertChannel(file_path="/non/existent/directory/alerts.log")

        with pytest.raises(OSError):
            channel._alert("Test Alert", "Test message")

    def test_alert_raises_error_for_permission_denied(self):
        """Test that _alert raises OSError when permission is denied."""
        # Try to write to a read-only directory (if available on system)
        channel = FileAlertChannel(file_path="/proc/alerts.log")

        with pytest.raises(OSError):
            channel._alert("Test Alert", "Test message")

    def test_multiple_alerts_to_same_file(self) -> None:
        """Test multiple sequential alerts to the same file."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".log") as temp_file:
            temp_file_path = temp_file.name

        try:
            channel = FileAlertChannel(file_path=temp_file_path)

            # Send multiple alerts
            channel._alert("Alert 1", "Message 1")
            channel._alert("Alert 2", "Message 2")
            channel._alert("Alert 3", "Message 3")

            # Verify all content
            with open(temp_file_path, "r", encoding="utf-8") as file:
                content = file.read()

            expected_lines = [
                "Alert 1: Message 1",
                "Alert 2: Message 2",
                "Alert 3: Message 3",
                "",  # Final newline creates empty string when split
            ]
            actual_lines = content.split("\n")
            assert actual_lines == expected_lines

        finally:
            Path(temp_file_path).unlink(missing_ok=True)

    def test_alert_method_delegates_to_internal_alert(self) -> None:
        """Test that the public alert method delegates to _alert correctly."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".log") as temp_file:
            temp_file_path = temp_file.name

        try:
            channel = FileAlertChannel(file_path=temp_file_path)

            # Use the public alert method
            channel.alert("Public Alert", "Public message")

            # Verify the content was written
            with open(temp_file_path, "r", encoding="utf-8") as file:
                content = file.read()

            expected_content = "Public Alert: Public message\n"
            assert content == expected_content

        finally:
            Path(temp_file_path).unlink(missing_ok=True)
