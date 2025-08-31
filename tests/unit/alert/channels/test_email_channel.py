"""Unit tests for the EmailAlertChannel class.

This module contains comprehensive tests for the EmailAlertChannel functionality,
including SMTP operations, email formatting, error handling, and configuration parsing.
"""

import smtplib
from unittest.mock import Mock, patch

import pytest

from flint.alert.channels.email import EmailAlertChannel
from flint.exceptions import FlintConfigurationKeyError


class TestEmailAlertChannel:
    """Test cases for EmailAlertChannel class."""

    @pytest.fixture
    def email_channel_config(self) -> dict:
        """Provide an email channel configuration for testing."""
        return {
            "smtp_server": "smtp.company.com",
            "smtp_port": 587,
            "username": "alerts@company.com",
            "password": "secure_password",
            "from_email": "etl-alerts@company.com",
            "to_emails": ["admin@company.com", "ops@company.com", "data-team@company.com"],
        }

    @pytest.fixture
    def email_channel(self, email_channel_config) -> EmailAlertChannel:
        """Create an EmailAlertChannel instance for testing."""
        return EmailAlertChannel.from_dict(email_channel_config)

    def test_from_dict_creates_channel_correctly(self, email_channel_config) -> None:
        """Test that from_dict creates an EmailAlertChannel correctly."""
        channel = EmailAlertChannel.from_dict(email_channel_config)

        assert channel.smtp_server == "smtp.company.com"
        assert channel.smtp_port == 587
        assert channel.username == "alerts@company.com"
        assert channel.password == "secure_password"
        assert channel.from_email == "etl-alerts@company.com"
        assert channel.to_emails == ["admin@company.com", "ops@company.com", "data-team@company.com"]

    def test_from_dict_raises_error_for_missing_keys(self):
        """Test that from_dict raises error when required keys are missing."""
        incomplete_config = {
            "smtp_server": "smtp.company.com",
            "smtp_port": 587,
            # Missing username, password, from_email, to_emails
        }

        with pytest.raises(FlintConfigurationKeyError):
            EmailAlertChannel.from_dict(incomplete_config)

    @patch("flint.alert.channels.email.smtplib.SMTP")
    def test_alert_sends_email_successfully(self, mock_smtp_class, email_channel: EmailAlertChannel) -> None:
        """Test that _alert sends email successfully."""
        # Configure mock SMTP server
        mock_smtp = Mock()
        mock_smtp_class.return_value.__enter__.return_value = mock_smtp

        # Send alert
        email_channel._alert("Test Alert", "This is a test alert message")

        # Verify SMTP operations
        mock_smtp_class.assert_called_once_with("smtp.company.com", 587)
        mock_smtp.starttls.assert_called_once()
        mock_smtp.login.assert_called_once_with("alerts@company.com", "secure_password")

        # Verify email was sent
        mock_smtp.sendmail.assert_called_once()
        args = mock_smtp.sendmail.call_args[0]

        # Check sender and recipients
        assert args[0] == "etl-alerts@company.com"  # from_email
        assert args[1] == ["admin@company.com", "ops@company.com", "data-team@company.com"]  # to_emails

        # Verify email content structure
        email_content = args[2]
        assert "Test Alert" in email_content  # Subject should be in headers
        assert "This is a test alert message" in email_content  # Body should be in content

    @patch("flint.alert.channels.email.smtplib.SMTP")
    def test_alert_formats_email_message_correctly(self, mock_smtp_class, email_channel: EmailAlertChannel) -> None:
        """Test that _alert formats the email message correctly."""
        mock_smtp = Mock()
        mock_smtp_class.return_value.__enter__.return_value = mock_smtp

        title = "Critical Database Error"
        body = "Database connection failed at 10:30 AM"

        email_channel._alert(title, body)

        # Get the email content that was sent
        email_content = mock_smtp.sendmail.call_args[0][2]

        # Parse the email to verify structure
        lines = email_content.split("\n")
        headers = {}
        content_start = 0

        for i, line in enumerate(lines):
            if line.strip() == "":
                content_start = i + 1
                break
            if ":" in line:
                key, value = line.split(":", 1)
                headers[key.strip()] = value.strip()

        # Verify headers
        assert headers.get("Subject") == title
        assert headers.get("From") == "etl-alerts@company.com"
        assert headers.get("To") == "admin@company.com, ops@company.com, data-team@company.com"

        # Verify body content
        email_body = "\n".join(lines[content_start:]).strip()
        assert body in email_body

    @patch("flint.alert.channels.email.smtplib.SMTP")
    def test_alert_handles_single_recipient(self, mock_smtp_class) -> None:
        """Test that _alert works with a single recipient."""
        config = {
            "smtp_server": "smtp.example.com",
            "smtp_port": 587,
            "username": "user@example.com",
            "password": "password",
            "from_email": "alerts@example.com",
            "to_emails": ["single@example.com"],
        }
        channel = EmailAlertChannel.from_dict(config)

        mock_smtp = Mock()
        mock_smtp_class.return_value.__enter__.return_value = mock_smtp

        channel._alert("Single Recipient", "Test message")

        # Verify recipient
        args = mock_smtp.sendmail.call_args[0]
        assert args[1] == ["single@example.com"]

    @patch("flint.alert.channels.email.smtplib.SMTP")
    def test_alert_handles_empty_title_and_body(self, mock_smtp_class, email_channel: EmailAlertChannel) -> None:
        """Test that _alert handles empty title and body gracefully."""
        mock_smtp = Mock()
        mock_smtp_class.return_value.__enter__.return_value = mock_smtp

        email_channel._alert("", "")

        # Verify email was still sent
        mock_smtp.sendmail.assert_called_once()

        # Check that empty values were handled
        email_content = mock_smtp.sendmail.call_args[0][2]
        assert "Subject: " in email_content  # Empty subject header

    @patch("flint.alert.channels.email.smtplib.SMTP")
    def test_alert_handles_special_characters(self, mock_smtp_class, email_channel: EmailAlertChannel) -> None:
        """Test that _alert handles special characters in email content."""
        mock_smtp = Mock()
        mock_smtp_class.return_value.__enter__.return_value = mock_smtp

        title = "Émergence Crítica!"
        body = "Message with special chars: àáâãäåæçèé & symbols: @#$%^&*()"

        email_channel._alert(title, body)

        # Verify email was sent successfully
        mock_smtp.sendmail.assert_called_once()

        # Verify content includes special characters (may be encoded)
        email_content = mock_smtp.sendmail.call_args[0][2]
        # The content should include UTF-8 encoding and the body content (possibly base64 encoded)
        assert "utf-8" in email_content
        assert "Content-Type: text/plain" in email_content

    @patch("flint.alert.channels.email.smtplib.SMTP")
    def test_alert_handles_multiline_body(self, mock_smtp_class, email_channel: EmailAlertChannel) -> None:
        """Test that _alert handles multiline message body correctly."""
        mock_smtp = Mock()
        mock_smtp_class.return_value.__enter__.return_value = mock_smtp

        title = "Multi-line Alert"
        body = "Line 1: Error occurred\nLine 2: Stack trace\nLine 3: Resolution steps"

        email_channel._alert(title, body)

        # Verify email was sent
        mock_smtp.sendmail.assert_called_once()

        # Verify multiline content is preserved
        email_content = mock_smtp.sendmail.call_args[0][2]
        assert "Line 1: Error occurred" in email_content
        assert "Line 2: Stack trace" in email_content
        assert "Line 3: Resolution steps" in email_content

    @patch("flint.alert.channels.email.smtplib.SMTP")
    def test_alert_handles_smtp_authentication_error(self, mock_smtp_class, email_channel: EmailAlertChannel) -> None:
        """Test that _alert raises SMTPException for authentication errors."""
        mock_smtp = Mock()
        mock_smtp_class.return_value.__enter__.return_value = mock_smtp
        mock_smtp.login.side_effect = smtplib.SMTPAuthenticationError(535, "Authentication failed")

        with pytest.raises(smtplib.SMTPException):
            email_channel._alert("Auth Error Test", "Test message")

    @patch("flint.alert.channels.email.smtplib.SMTP")
    def test_alert_handles_smtp_connection_error(self, mock_smtp_class, email_channel: EmailAlertChannel) -> None:
        """Test that _alert raises SMTPException for connection errors."""
        mock_smtp_class.side_effect = smtplib.SMTPConnectError(421, "Cannot connect to server")

        with pytest.raises(smtplib.SMTPException):
            email_channel._alert("Connection Error Test", "Test message")

    @patch("flint.alert.channels.email.smtplib.SMTP")
    def test_alert_handles_smtp_send_error(self, mock_smtp_class, email_channel: EmailAlertChannel) -> None:
        """Test that _alert raises SMTPException for send errors."""
        mock_smtp = Mock()
        mock_smtp_class.return_value.__enter__.return_value = mock_smtp
        mock_smtp.sendmail.side_effect = smtplib.SMTPRecipientsRefused(
            {"admin@company.com": (550, b"Mailbox unavailable")}
        )

        with pytest.raises(smtplib.SMTPException):
            email_channel._alert("Send Error Test", "Test message")  # noqa: SLF001

    @patch("flint.alert.channels.email.smtplib.SMTP")
    def test_alert_uses_correct_smtp_settings(self, mock_smtp_class) -> None:
        """Test that _alert uses the correct SMTP server settings."""
        config = {
            "smtp_server": "mail.custom.com",
            "smtp_port": 465,
            "username": "custom@custom.com",
            "password": "custom_pass",
            "from_email": "noreply@custom.com",
            "to_emails": ["test@custom.com"],
        }
        channel = EmailAlertChannel.from_dict(config)

        mock_smtp = Mock()
        mock_smtp_class.return_value.__enter__.return_value = mock_smtp

        channel._alert("SMTP Settings Test", "Test message")  # noqa: SLF001

        # Verify correct SMTP server and port were used
        mock_smtp_class.assert_called_once_with("mail.custom.com", 465)

        # Verify correct credentials were used
        mock_smtp.login.assert_called_once_with("custom@custom.com", "custom_pass")

    @patch("flint.alert.channels.email.smtplib.SMTP")
    def test_alert_context_manager_properly_closes_connection(
        self, mock_smtp_class, email_channel: EmailAlertChannel
    ) -> None:
        """Test that _alert properly closes SMTP connection using context manager."""
        mock_smtp = Mock()
        mock_smtp_class.return_value.__enter__.return_value = mock_smtp

        email_channel._alert("Context Manager Test", "Test message")

        # Verify context manager was used (enter and exit called)
        mock_smtp_class.return_value.__enter__.assert_called_once()
        mock_smtp_class.return_value.__exit__.assert_called_once()

    def test_alert_method_delegates_to_internal_alert(self, email_channel: EmailAlertChannel) -> None:
        """Test that the public alert method delegates to _alert correctly."""
        with patch.object(email_channel, "_alert") as mock_internal_alert:
            # Use the public alert method
            email_channel.alert("Public Alert", "Public message")

            # Verify delegation
            mock_internal_alert.assert_called_once_with(title="Public Alert", body="Public message")
