"""Email channel for sending alert messages via SMTP.

This module implements the email alert channel that sends alerts
through SMTP servers. It supports authentication, multiple recipients,
and configurable failure handling.

The EmailAlertChannel follows the Flint framework patterns for configuration-driven
initialization and implements the BaseAlertChannel interface.
"""

import logging
import smtplib
from dataclasses import dataclass
from email.mime.text import MIMEText
from typing import Any, Final, Self

from flint.alert.channels.base import BaseAlertChannel
from flint.exceptions import FlintConfigurationKeyError
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

SMTP_SERVER: Final[str] = "smtp_server"
SMTP_PORT: Final[str] = "smtp_port"
USERNAME: Final[str] = "username"
PASSWORD: Final[str] = "password"
FROM_EMAIL: Final[str] = "from_email"
TO_EMAILS: Final[str] = "to_emails"


@dataclass
class EmailAlertChannel(BaseAlertChannel):
    """Email alert channel for SMTP-based alerts.

    This class implements email alerting functionality using SMTP servers.
    It supports authentication, multiple recipients, and configurable
    failure handling with retry logic.

    Attributes:
        smtp_server: SMTP server hostname or IP address
        smtp_port: SMTP server port number
        username: SMTP authentication username
        password: SMTP authentication password
        from_email: Email address to send alerts from
        to_emails: List of recipient email addresses
        failure_handling: Configuration for handling channel failures and retries
    """

    smtp_server: str
    smtp_port: int
    username: str
    password: str
    from_email: str
    to_emails: list[str]

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create an EmailChannel instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing email channel configuration with keys:
                  - smtp_server: SMTP server hostname
                  - smtp_port: SMTP server port
                  - username: SMTP authentication username
                  - password: SMTP authentication password
                  - from_email: Sender email address
                  - to_emails: List of recipient email addresses
                  - failure_handling: Failure handling configuration

        Returns:
            An EmailChannel instance configured from the dictionary

        Examples:
            >>> config = {
            ...     "smtp_server": "smtp.company.com",
            ...     "smtp_port": 587,
            ...     "username": "${SMTP_USER}",
            ...     "password": "${SMTP_PASSWORD}",
            ...     "from_email": "etl-alerts@company.com",
            ...     "to_emails": ["ops@company.com", "data-team@company.com"],
            ...     "failure_handling": {...}
            ... }
            >>> email_channel = EmailChannel.from_dict(config)
        """
        logger.debug("Creating EmailChannel from configuration dictionary")
        try:
            return cls(
                smtp_server=dict_[SMTP_SERVER],
                smtp_port=dict_[SMTP_PORT],
                username=dict_[USERNAME],
                password=dict_[PASSWORD],
                from_email=dict_[FROM_EMAIL],
                to_emails=dict_[TO_EMAILS],
            )
        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

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
                server.starttls()  # Enable security
                server.login(self.username, self.password)
                # Send email
                server.sendmail(self.from_email, self.to_emails, msg.as_string())

            logger.info("Email alert sent successfully to %s", ", ".join(self.to_emails))

        except smtplib.SMTPException as exc:
            logger.error("Failed to send email alert: %s", exc)
            raise
