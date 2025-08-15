"""Email channel for sending alert notifications via SMTP.

This module implements the email alert channel that sends notifications
through SMTP servers. It supports authentication, multiple recipients,
and configurable failure handling.

The EmailChannel follows the Flint framework patterns for configuration-driven
initialization and implements the BaseChannel interface.
"""

import logging
from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import ConfigurationKeyError
from flint.utils.alert.channels.base import BaseConfig
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

SMTP_SERVER: Final[str] = "smtp_server"
SMTP_PORT: Final[str] = "smtp_port"
USERNAME: Final[str] = "username"
PASSWORD: Final[str] = "password"
FROM_EMAIL: Final[str] = "from_email"
TO_EMAILS: Final[str] = "to_emails"


@dataclass
class EmailConfig(BaseConfig):
    """Email config for SMTP-based notifications.

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
            smtp_server = dict_[SMTP_SERVER]
            smtp_port = dict_[SMTP_PORT]
            username = dict_[USERNAME]
            password = dict_[PASSWORD]
            from_email = dict_[FROM_EMAIL]
            to_emails = dict_[TO_EMAILS]
        except KeyError as e:
            raise ConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            smtp_server=smtp_server,
            smtp_port=smtp_port,
            username=username,
            password=password,
            from_email=from_email,
            to_emails=to_emails,
        )

    def _alert(self, title: str, body: str) -> None:
        """Send an alert message via email (not implemented yet)."""
        raise NotImplementedError("send_alert not implemented for EmailChannel.")
