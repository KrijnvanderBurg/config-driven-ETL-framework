"""Tests for AlertController model.

These tests verify that AlertController instances can be correctly created from
configuration and function as expected.
"""

from typing import Any

import pytest

from flint.alert.controller import AlertController

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="controller_config")
def fixture_controller_config() -> dict[str, Any]:
    """Provide a representative AlertController configuration with multiple items.

    Returns:
        A dictionary representing the alert configuration with multiple channels
        and triggers keyed by 'channels' and 'triggers'. This mirrors how the
        AlertController is constructed from a configuration dict in production.
    """
    channels = [
        {
            "channel_id": "email",
            "name": "email1",
            "description": "an email channel",
            "smtp_server": "smtp.example.com",
            "smtp_port": 587,
            "username": "user",
            "password": "secret",
            "from_email": "from@example.com",
            "to_emails": ["to@example.com"],
        },
        {
            "channel_id": "file",
            "name": "file1",
            "description": "a file channel",
            "file_path": "/tmp/alerts.log",
        },
        {
            "channel_id": "http",
            "name": "http1",
            "description": "an http channel",
            "url": "https://example.com/webhook",
            "method": "POST",
            "headers": {"Content-Type": "application/json"},
            "timeout": 5,
            "retry": {"raise_on_error": False, "max_attempts": 0, "delay_in_seconds": 0},
        },
    ]

    triggers = [
        {
            "name": "test_trigger",
            "enabled": True,
            "description": "A test trigger",
            "channel_names": ["file1"],
            "template": {"prepend_title": "", "append_title": "", "prepend_body": "", "append_body": ""},
            "rules": [],
        },
        {
            "name": "disabled_trigger",
            "enabled": False,
            "description": "A disabled trigger",
            "channel_names": ["http1"],
            "template": {"prepend_title": "", "append_title": "", "prepend_body": "", "append_body": ""},
            "rules": [],
        },
    ]

    return {"channels": channels, "triggers": triggers}


def test_controller_creation__from_config__creates_valid_model(controller_config: dict[str, Any]) -> None:
    """Test specifically for the creation process itself."""
    # Act
    controller = AlertController(**controller_config)

    # Assert
    assert isinstance(controller.channels, list)
    assert len(controller.channels) == 3
    assert isinstance(controller.triggers, list)
    assert len(controller.triggers) == 2


# Two blank lines separate top-level test sections
# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="controller_instance")
def fixture_controller_instance(controller_config: dict[str, Any]) -> AlertController:
    """Create AlertController instance from configuration."""
    return AlertController(**controller_config)


def test_controller_properties__after_creation__match_expected_values(controller_instance: AlertController) -> None:
    """Test model properties using the instantiated object fixture.

    Use hard-coded expected values to avoid comparing fixtures directly.
    """
    assert controller_instance.channels[0].name == "email1"
    assert controller_instance.channels[1].name == "file1"
    assert controller_instance.channels[2].name == "http1"

    # Assert additional channel attributes in config order
    # Email channel
    email_ch = controller_instance.channels[0]
    assert email_ch.channel_id == "email"
    assert email_ch.description == "an email channel"
    assert email_ch.smtp_server == "smtp.example.com"
    assert email_ch.smtp_port == 587

    # File channel
    file_ch = controller_instance.channels[1]
    assert file_ch.channel_id == "file"
    assert file_ch.description == "a file channel"

    # HTTP channel
    http_ch = controller_instance.channels[2]
    assert http_ch.channel_id == "http"
    assert http_ch.description == "an http channel"
    assert str(http_ch.url) == "https://example.com/webhook"
    assert http_ch.method == "POST"
    assert isinstance(http_ch.retry, type(http_ch.retry))

    assert controller_instance.triggers[0].name == "test_trigger"
    assert controller_instance.triggers[1].name == "disabled_trigger"
    # Trigger enabled flags and template/channel_names
    assert controller_instance.triggers[0].enabled is True
    assert controller_instance.triggers[1].enabled is False
    assert controller_instance.triggers[0].channel_names == ["file1"]
    assert controller_instance.triggers[0].template.prepend_title == ""
    assert controller_instance.triggers[0].template.append_title == ""


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #
