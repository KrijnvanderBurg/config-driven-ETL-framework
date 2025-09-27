"""Tests for EmailChannel model.

These tests verify that EmailChannel instances can be correctly created from
configuration and function as expected.
"""

from typing import Any

import pytest

from flint.alert.channels.email import EmailChannel

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="email_config")
def fixture_email_config() -> dict[str, Any]:
    """Provide a representative email channel configuration."""
    return {
        "name": "email1",
        "description": "an email channel",
        "smtp_server": "smtp.example.com",
        "smtp_port": 587,
        "username": "user",
        "password": "secret",
        "from_email": "from@example.com",
        "to_emails": ["to@example.com"],
    }


def test_email_channel_creation__from_config__creates_valid_model(email_config: dict[str, Any]) -> None:
    """Test specifically for the creation process itself."""
    # Act
    ch = EmailChannel(**email_config)

    # Assert
    assert ch.name == "email1"
    assert ch.smtp_server == "smtp.example.com"
    assert ch.smtp_port == 587
    # Additional attributes (match order in examples/job.jsonc)
    assert ch.channel_id == "email"
    assert ch.description == "an email channel"
    assert ch.username == "user"
    # password is SecretStr
    assert ch.password.get_secret_value() == "secret"
    assert ch.from_email == "from@example.com"
    assert ch.to_emails == ["to@example.com"]


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="email_channel")
def fixture_email_channel(email_config: dict[str, Any]) -> EmailChannel:
    """Create EmailChannel instance from configuration."""
    return EmailChannel(**email_config)


def test_email_channel_properties__after_creation__match_expected_values(email_channel: EmailChannel) -> None:
    """Test model properties using the instantiated object fixture.

    Use hard-coded expected values to avoid comparing fixtures directly.
    """
    assert email_channel.name == "email1"
    assert email_channel.smtp_server == "smtp.example.com"
    assert email_channel.smtp_port == 587
    # Additional attributes asserted on the fixture
    assert email_channel.channel_id == "email"
    assert email_channel.description == "an email channel"
    assert email_channel.username == "user"
    assert email_channel.password.get_secret_value() == "secret"
    assert email_channel.from_email == "from@example.com"
    assert email_channel.to_emails == ["to@example.com"]


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #
