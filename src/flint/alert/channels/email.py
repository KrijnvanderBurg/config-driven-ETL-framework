"""Email channel for sending alert messages via SMTP.

This module implements the email alert channel that sends alerts
through SMTP servers. It supports authentication, multiple recipients,
and configurable failure handling.

The EmailChannel follows the Flint framework patterns for configuration-driven
initialization and implements the ChannelModel interface.
"""

import logging
import smtplib
from email.mime.text import MIMEText
from typing import Literal

from pydantic import EmailStr, Field, SecretStr, StrictInt, StrictStr
from typing_extensions import override

from flint.alert.channels.base import ChannelModel
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class EmailChannel(ChannelModel):
    """Email alert channel for SMTP-based alerts.

    This class implements email alerting functionality using SMTP servers.
    It supports authentication, multiple recipients, and configurable
    failure handling with retry logic.

    Attributes:
        channel_type: Always "email" for email channels
        id: Human-readable identifier for the channel
        description: Description of the channel purpose
        smtp_server: SMTP server hostname or IP address
        smtp_port: SMTP server port number
        username: SMTP authentication username
        password: SMTP authentication password
        from_email: Email address to send alerts from
        to_emails: List of recipient email addresses
    """

    channel_type: Literal["email"] = Field("email", description="Type identifier for the email channel")
    smtp_server: StrictStr = Field(..., description="SMTP server hostname or IP address", min_length=1)
    smtp_port: StrictInt = Field(..., description="SMTP server port number", gt=0, le=65535)
    username: StrictStr = Field(..., description="SMTP authentication username", min_length=1)
    password: SecretStr = Field(..., description="SMTP authentication password")
    from_email: EmailStr = Field(..., description="Sender email address")
    to_emails: list[EmailStr] = Field(..., description="List of recipient email addresses", min_length=1)

    @override
    def _alert(self, title: str, body: str) -> None:
        """Send an alert message via email.

        Args:
            title: The alert title (used as email subject).
            body: The alert message content (used as email body).

        Raises:
            smtplib.SMTPException: If email sending fails.
        """
        # Create simple text message
        msg = MIMEText(body, "plain")
        msg["From"] = self.from_email
        msg["To"] = ", ".join(self.to_emails)
        msg["Subject"] = title

        try:
            # Create SMTP session
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.login(self.username, self.password.get_secret_value())
                server.sendmail(self.from_email, list(self.to_emails), msg.as_string())

            logger.info("Email alert sent successfully to %s", ", ".join(self.to_emails))
        except smtplib.SMTPException as exc:
            logger.error("Failed to send email alert: %s", exc)
            raise
