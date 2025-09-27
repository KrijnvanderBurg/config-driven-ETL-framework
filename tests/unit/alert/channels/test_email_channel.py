"""Tests for EmailChannel alert functionality.

These tests verify EmailChannel creation, validation, alert sending,
and error handling scenarios.
"""

import smtplib
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from flint.alert.channels.email import EmailChannel

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="valid_email_config")
def fixture_valid_email_config() -> dict[str, Any]:
    """Provide a valid email channel configuration."""
    return {
        "name": "production-alerts",
        "description": "Production error notifications",
        "smtp_server": "smtp.company.com",
        "smtp_port": 587,
        "username": "alerts@company.com",
        "password": "secure_password",
        "from_email": "alerts@company.com",
        "to_emails": ["admin@company.com", "team@company.com"],
    }


# =========================================================================== #
# ========================== VALIDATION TESTS ============================= #
# =========================================================================== #


class TestEmailChannelValidation:
    """Test EmailChannel model validation and instantiation."""

    def test_create_email_channel__with_valid_config__succeeds(self, valid_email_config: dict[str, Any]) -> None:
        """Test EmailChannel creation with valid configuration."""
        # Act
        channel = EmailChannel(**valid_email_config)

        # Assert
        assert channel.name == "production-alerts"
        assert channel.description == "Production error notifications"
        assert channel.channel_id == "email"
        assert channel.smtp_server == "smtp.company.com"
        assert channel.smtp_port == 587
        assert channel.username == "alerts@company.com"
        assert channel.password.get_secret_value() == "secure_password"
        assert channel.from_email == "alerts@company.com"
        assert channel.to_emails == ["admin@company.com", "team@company.com"]

    def test_create_email_channel__with_missing_name__raises_validation_error(
        self, valid_email_config: dict[str, Any]
    ) -> None:
        """Test EmailChannel creation fails when name is missing."""
        # Arrange
        del valid_email_config["name"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            EmailChannel(**valid_email_config)

    def test_create_email_channel__with_empty_name__raises_validation_error(
        self, valid_email_config: dict[str, Any]
    ) -> None:
        """Test EmailChannel creation fails when name is empty string."""
        # Arrange
        valid_email_config["name"] = ""

        # Assert
        with pytest.raises(ValidationError):
            # Act
            EmailChannel(**valid_email_config)

    def test_create_email_channel__with_invalid_port__raises_validation_error(
        self, valid_email_config: dict[str, Any]
    ) -> None:
        """Test EmailChannel creation fails with invalid port numbers."""
        # Arrange
        valid_email_config["smtp_port"] = 0

        # Assert
        with pytest.raises(ValidationError):
            # Act
            EmailChannel(**valid_email_config)

    def test_create_email_channel__with_port_too_high__raises_validation_error(
        self, valid_email_config: dict[str, Any]
    ) -> None:
        """Test EmailChannel creation fails with port number too high."""
        # Arrange
        valid_email_config["smtp_port"] = 65536

        # Assert
        with pytest.raises(ValidationError):
            # Act
            EmailChannel(**valid_email_config)

    def test_create_email_channel__with_invalid_email__raises_validation_error(
        self, valid_email_config: dict[str, Any]
    ) -> None:
        """Test EmailChannel creation fails with invalid email format."""
        # Arrange
        valid_email_config["from_email"] = "not-an-email"

        # Assert
        with pytest.raises(ValidationError):
            # Act
            EmailChannel(**valid_email_config)

    def test_create_email_channel__with_empty_to_emails__raises_validation_error(
        self, valid_email_config: dict[str, Any]
    ) -> None:
        """Test EmailChannel creation fails with empty recipient list."""
        # Arrange
        valid_email_config["to_emails"] = []

        # Assert
        with pytest.raises(ValidationError):
            # Act
            EmailChannel(**valid_email_config)

    def test_create_email_channel__with_empty_smtp_server__raises_validation_error(
        self, valid_email_config: dict[str, Any]
    ) -> None:
        """Test EmailChannel creation fails with empty SMTP server."""
        # Arrange
        valid_email_config["smtp_server"] = ""

        # Assert
        with pytest.raises(ValidationError):
            # Act
            EmailChannel(**valid_email_config)

    def test_create_email_channel__with_empty_username__raises_validation_error(
        self, valid_email_config: dict[str, Any]
    ) -> None:
        """Test EmailChannel creation fails with empty username."""
        # Arrange
        valid_email_config["username"] = ""

        # Assert
        with pytest.raises(ValidationError):
            # Act
            EmailChannel(**valid_email_config)


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="email_channel")
def fixture_email_channel(valid_email_config: dict[str, Any]) -> EmailChannel:
    """Create EmailChannel instance from valid configuration."""
    return EmailChannel(**valid_email_config)


# =========================================================================== #
# ============================ ALERT TESTS ================================ #
# =========================================================================== #


class TestEmailChannelAlert:
    """Test EmailChannel alert functionality."""

    def test_alert__with_valid_inputs__sends_email_successfully(self, email_channel: EmailChannel) -> None:
        """Test successful email alert sending with proper SMTP calls."""
        # Mock the SMTP client because we don't want to make real network calls
        with patch("flint.alert.channels.email.smtplib.SMTP") as mock_smtp:
            mock_server = MagicMock()
            mock_smtp.return_value.__enter__.return_value = mock_server

            # Act
            email_channel.alert("Critical System Alert", "Database connection failed")

            # Assert SMTP connection established with correct parameters
            mock_smtp.assert_called_once_with(email_channel.smtp_server, email_channel.smtp_port)

            # Assert authentication performed
            mock_server.login.assert_called_once_with(email_channel.username, email_channel.password.get_secret_value())

            # Assert email sent with correct parameters
            mock_server.sendmail.assert_called_once()
            from_addr, to_addrs, message = mock_server.sendmail.call_args[0]
            assert from_addr == email_channel.from_email
            assert to_addrs == list(email_channel.to_emails)
            assert "Subject: Critical System Alert" in message
            assert "Database connection failed" in message

    def test_alert__with_multiple_recipients__includes_all_recipients(self, email_channel: EmailChannel) -> None:
        """Test that alert sends to all configured recipients."""
        # Mock the SMTP client because we don't want to make real network calls
        with patch("flint.alert.channels.email.smtplib.SMTP") as mock_smtp:
            mock_server = MagicMock()
            mock_smtp.return_value.__enter__.return_value = mock_server

            # Act
            email_channel.alert("Alert Title", "Alert Body")

            # Assert
            mock_server.sendmail.assert_called_once()
            _, to_addrs, message = mock_server.sendmail.call_args[0]
            assert to_addrs == ["admin@company.com", "team@company.com"]
            assert "To: admin@company.com, team@company.com" in message

    def test_alert__with_special_characters__handles_encoding_correctly(self, email_channel: EmailChannel) -> None:
        """Test alert handling with special characters in title and body."""
        # Mock the SMTP client because we don't want to make real network calls
        with patch("flint.alert.channels.email.smtplib.SMTP") as mock_smtp:
            mock_server = MagicMock()
            mock_smtp.return_value.__enter__.return_value = mock_server

            # Act
            email_channel.alert("Ålørt: Üñíçødé Tëst", "Body with émojis 🔥 and spëcial chars: áéíóú")

            # Assert
            mock_server.sendmail.assert_called_once()
            _, _, message = mock_server.sendmail.call_args[0]
            assert "Subject: =?utf-8?b?" in message or "Ålørt: Üñíçødé Tëst" in message
            assert "émojis" in message or "=?utf-8?" in message

    def test_alert__with_empty_title__sends_with_empty_subject(self, email_channel: EmailChannel) -> None:
        """Test alert sending with empty title results in empty subject."""
        # Mock the SMTP client because we don't want to make real network calls
        with patch("flint.alert.channels.email.smtplib.SMTP") as mock_smtp:
            mock_server = MagicMock()
            mock_smtp.return_value.__enter__.return_value = mock_server

            # Act
            email_channel.alert("", "Important alert body")

            # Assert
            mock_server.sendmail.assert_called_once()
            _, _, message = mock_server.sendmail.call_args[0]
            assert "Subject: " in message
            assert "Important alert body" in message

    def test_alert__with_empty_body__sends_with_empty_content(self, email_channel: EmailChannel) -> None:
        """Test alert sending with empty body results in empty message content."""
        # Mock the SMTP client because we don't want to make real network calls
        with patch("flint.alert.channels.email.smtplib.SMTP") as mock_smtp:
            mock_server = MagicMock()
            mock_smtp.return_value.__enter__.return_value = mock_server

            # Act
            email_channel.alert("Alert Title", "")

            # Assert
            mock_server.sendmail.assert_called_once()
            _, _, message = mock_server.sendmail.call_args[0]
            assert "Subject: Alert Title" in message
            # Body should be empty but email structure should be valid
            assert "Content-Type: text/plain" in message

    def test_alert__with_smtp_authentication_error__raises_smtp_exception(self, email_channel: EmailChannel) -> None:
        """Test alert handling when SMTP authentication fails."""
        # Mock the SMTP client to simulate authentication failure
        with patch("flint.alert.channels.email.smtplib.SMTP") as mock_smtp:
            mock_server = MagicMock()
            mock_server.login.side_effect = smtplib.SMTPAuthenticationError(535, "Authentication failed")
            mock_smtp.return_value.__enter__.return_value = mock_server

            # Assert
            with pytest.raises(smtplib.SMTPAuthenticationError):
                # Act
                email_channel.alert("Test Alert", "Test Body")

    def test_alert__with_smtp_server_error__raises_smtp_exception(self, email_channel: EmailChannel) -> None:
        """Test alert handling when SMTP server returns error."""
        # Mock the SMTP client to simulate server error during send
        with patch("flint.alert.channels.email.smtplib.SMTP") as mock_smtp:
            mock_server = MagicMock()
            mock_server.sendmail.side_effect = smtplib.SMTPServerDisconnected("Connection lost")
            mock_smtp.return_value.__enter__.return_value = mock_server

            # Assert
            with pytest.raises(smtplib.SMTPServerDisconnected):
                # Act
                email_channel.alert("Test Alert", "Test Body")

            # Assert authentication was attempted before failure
            mock_server.login.assert_called_once()

    def test_alert__with_smtp_recipient_refused__raises_smtp_exception(self, email_channel: EmailChannel) -> None:
        """Test alert handling when SMTP server refuses recipients."""
        # Mock the SMTP client to simulate recipient refusal
        with patch("flint.alert.channels.email.smtplib.SMTP") as mock_smtp:
            mock_server = MagicMock()
            refused_recipients = {"admin@company.com": (550, b"User unknown")}
            mock_server.sendmail.side_effect = smtplib.SMTPRecipientsRefused(refused_recipients)
            mock_smtp.return_value.__enter__.return_value = mock_server

            # Assert
            with pytest.raises(smtplib.SMTPRecipientsRefused):
                # Act
                email_channel.alert("Test Alert", "Test Body")

    def test_alert__with_smtp_connection_error__raises_smtp_exception(self, email_channel: EmailChannel) -> None:
        """Test alert handling when SMTP connection fails."""
        # Mock the SMTP client to simulate connection failure
        with patch("flint.alert.channels.email.smtplib.SMTP") as mock_smtp:
            mock_smtp.side_effect = smtplib.SMTPConnectError(421, "Service not available")

            # Assert
            with pytest.raises(smtplib.SMTPConnectError):
                # Act
                email_channel.alert("Test Alert", "Test Body")
