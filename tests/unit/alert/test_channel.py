"""Unit tests for the AlertChannel wrapper class.

This module contains comprehensive tests for the AlertChannel functionality,
including channel type resolution, configuration parsing, and alert delegation.
"""

from unittest.mock import MagicMock

import pytest

from flint.alert.channel import AlertChannel
from flint.alert.channels.email import EmailAlertChannel
from flint.alert.channels.file import FileAlertChannel
from flint.alert.channels.http import HttpAlertChannel
from flint.exceptions import FlintConfigurationKeyError


class TestAlertChannel:
    """Test cases for AlertChannel class."""

    @pytest.fixture
    def file_channel_config(self) -> dict:
        """Provide a file channel configuration for testing."""
        return {"type": "file", "name": "test-file-channel", "config": {"file_path": "/tmp/test-alerts.log"}}

    @pytest.fixture
    def http_channel_config(self) -> dict:
        """Provide an HTTP channel configuration for testing."""
        return {
            "type": "http",
            "name": "test-http-channel",
            "config": {
                "url": "https://example.com/webhook",
                "method": "POST",
                "headers": {"Content-Type": "application/json"},
                "timeout": 30,
                "retry": {"error_on_alert_failure": False, "attempts": 3, "delay_in_seconds": 5},
            },
        }

    @pytest.fixture
    def email_channel_config(self) -> dict:
        """Provide an email channel configuration for testing."""
        return {
            "type": "email",
            "name": "test-email-channel",
            "config": {
                "smtp_server": "smtp.example.com",
                "smtp_port": 587,
                "username": "test@example.com",
                "password": "password",
                "from_email": "alerts@example.com",
                "to_emails": ["admin@example.com", "ops@example.com"],
            },
        }

    def test_from_dict_creates_file_channel(self, file_channel_config) -> None:
        """Test that from_dict creates a file channel correctly."""
        channel = AlertChannel.from_dict(file_channel_config)

        assert channel.type == "file"
        assert channel.name == "test-file-channel"
        assert isinstance(channel.config, FileAlertChannel)
        assert channel.config.file_path == "/tmp/test-alerts.log"

    def test_from_dict_creates_http_channel(self, http_channel_config) -> None:
        """Test that from_dict creates an HTTP channel correctly."""
        channel = AlertChannel.from_dict(http_channel_config)

        assert channel.type == "http"
        assert channel.name == "test-http-channel"
        assert isinstance(channel.config, HttpAlertChannel)
        assert channel.config.url == "https://example.com/webhook"
        assert channel.config.method == "POST"
        assert channel.config.timeout == 30

    def test_from_dict_creates_email_channel(self, email_channel_config) -> None:
        """Test that from_dict creates an email channel correctly."""
        channel = AlertChannel.from_dict(email_channel_config)

        assert channel.type == "email"
        assert channel.name == "test-email-channel"
        assert isinstance(channel.config, EmailAlertChannel)
        assert channel.config.smtp_server == "smtp.example.com"
        assert channel.config.smtp_port == 587
        assert channel.config.from_email == "alerts@example.com"
        assert channel.config.to_emails == ["admin@example.com", "ops@example.com"]

    def test_from_dict_raises_error_for_unknown_channel_type(self) -> None:
        """Test that from_dict raises ValueError for unknown channel types."""
        config = {"type": "unknown", "name": "test-channel", "config": {}}

        with pytest.raises(ValueError, match="Unknown channel type: unknown"):
            AlertChannel.from_dict(config)

    def test_from_dict_raises_error_for_missing_type(self) -> None:
        """Test that from_dict raises error when 'type' is missing."""
        config = {"name": "test-channel", "config": {}}

        with pytest.raises(FlintConfigurationKeyError):
            AlertChannel.from_dict(config)

    def test_from_dict_raises_error_for_missing_name(self) -> None:
        """Test that from_dict raises error when 'name' is missing."""
        config = {"type": "file", "config": {"file_path": "/tmp/test.log"}}

        with pytest.raises(FlintConfigurationKeyError):
            AlertChannel.from_dict(config)

    def test_from_dict_raises_error_for_missing_config(self):
        """Test that from_dict raises error when 'config' is missing."""
        config = {"type": "file", "name": "test-channel"}

        with pytest.raises(FlintConfigurationKeyError):
            AlertChannel.from_dict(config)

    def test_send_alert_delegates_to_config(self, file_channel_config) -> None:
        """Test that send_alert delegates to the underlying channel config."""
        channel = AlertChannel.from_dict(file_channel_config)

        # Mock the config's alert method
        channel.config.alert = MagicMock()

        # Send an alert
        channel.send_alert("Test Title", "Test Body")

        # Verify delegation
        channel.config.alert.assert_called_once_with(title="Test Title", body="Test Body")

    def test_send_alert_with_empty_title_and_body(self, file_channel_config) -> None:
        """Test that send_alert works with empty title and body."""
        channel = AlertChannel.from_dict(file_channel_config)

        # Mock the config's alert method
        channel.config.alert = MagicMock()

        # Send an alert with empty strings
        channel.send_alert("", "")

        # Verify delegation
        channel.config.alert.assert_called_once_with(title="", body="")

    def test_channel_preserves_configuration_data(self, http_channel_config) -> None:
        """Test that channel preserves all configuration data correctly."""
        channel = AlertChannel.from_dict(http_channel_config)

        # Verify all configuration is preserved (cast to specific type for testing)
        http_config = channel.config
        assert isinstance(http_config, HttpAlertChannel)
        assert http_config.headers == {"Content-Type": "application/json"}
        assert http_config.retry.attempts == 3
        assert http_config.retry.delay_in_seconds == 5
        assert http_config.retry.error_on_alert_failure is False

    def test_multiple_channels_are_independent(self, file_channel_config, http_channel_config) -> None:
        """Test that multiple channel instances are independent."""
        file_channel = AlertChannel.from_dict(file_channel_config)
        http_channel = AlertChannel.from_dict(http_channel_config)

        # Mock both configs
        file_channel.config.alert = MagicMock()
        http_channel.config.alert = MagicMock()

        # Send alerts to both
        file_channel.send_alert("File Alert", "File message")
        http_channel.send_alert("HTTP Alert", "HTTP message")

        # Verify each was called correctly
        file_channel.config.alert.assert_called_once_with(title="File Alert", body="File message")
        http_channel.config.alert.assert_called_once_with(title="HTTP Alert", body="HTTP message")
