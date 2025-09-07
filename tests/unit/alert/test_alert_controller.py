"""Unit tests for the AlertController class.

This module contains comprehensive tests for the AlertController functionality,
including configuration loading, channel management, and alert processing.
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from flint.alert.controller import AlertController


class TestAlertController:
    """Test cases for AlertController class."""

    @pytest.fixture
    def sample_alert_config(self) -> dict:
        """Provide a sample alert configuration for testing."""
        return {
            "alert": {
                "channels": [
                    {"type": "file", "name": "test-file", "config": {"file_path": "/tmp/test-alerts.log"}},
                    {
                        "type": "http",
                        "name": "test-webhook",
                        "config": {
                            "url": "https://example.com/webhook",
                            "method": "POST",
                            "headers": {"Content-Type": "application/json"},
                            "timeout": 30,
                            "retry": {"error_on_alert_failure": False, "attempts": 3, "delay_in_seconds": 5},
                        },
                    },
                ],
                "triggers": [
                    {
                        "name": "test-trigger",
                        "enabled": True,
                        "channel_names": ["test-file", "test-webhook"],
                        "Template": {
                            "prepend_title": "[ALERT] ",
                            "append_title": " - ETL Pipeline",
                            "prepend_body": "Alert Details:\n",
                            "append_body": "\n\nPlease investigate.",
                        },
                        "conditions": {
                            "exception_regex": ".*critical.*",
                            "env_vars_matches": {"ENV": ["production", "staging"]},
                        },
                    }
                ],
            }
        }

    @pytest.fixture
    def alert_manager(self, sample_alert_config) -> AlertController:
        """Create an AlertController instance for testing."""
        return AlertController.from_dict(sample_alert_config)

    def test_from_dict_creates_manager_with_channels_and_triggers(self, sample_alert_config) -> None:
        """Test that from_dict creates an AlertController with proper channels and triggers."""
        manager = AlertController.from_dict(sample_alert_config)

        assert len(manager.channels) == 2
        assert len(manager.triggers) == 1

        # Verify channel types and names
        channel_names = [channel.name for channel in manager.channels]
        assert "test-file" in channel_names
        assert "test-webhook" in channel_names

        # Verify trigger configuration
        trigger = manager.triggers[0]
        assert trigger.name == "test-trigger"
        assert trigger.enabled is True
        assert "test-file" in trigger.channel_names
        assert "test-webhook" in trigger.channel_names

    def test_from_dict_raises_error_for_missing_alert_key(self) -> None:
        """Test that from_dict raises error when 'alert' key is missing."""
        config = {"other_key": "value"}

        with pytest.raises(KeyError):
            AlertController.from_dict(config)

    def test_from_dict_raises_error_for_missing_channels_key(self) -> None:
        """Test that from_dict raises error when 'channels' key is missing."""
        config = {"alert": {"triggers": []}}

        with pytest.raises(KeyError):
            AlertController.from_dict(config)

    def test_from_dict_raises_error_for_missing_triggers_key(self) -> None:
        """Test that from_dict raises error when 'triggers' key is missing."""
        config = {"alert": {"channels": []}}

        with pytest.raises(KeyError):
            AlertController.from_dict(config)

    def test_from_file_loads_configuration_successfully(self, sample_alert_config) -> None:
        """Test that from_file loads and creates AlertController from file."""
        import json

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as temp_file:
            json.dump(sample_alert_config, temp_file)
            temp_file_path = Path(temp_file.name)

        try:
            manager = AlertController.from_file(temp_file_path)

            assert len(manager.channels) == 2
            assert len(manager.triggers) == 1
            assert manager.triggers[0].name == "test-trigger"

        finally:
            temp_file_path.unlink()

    def test_evaluate_trigger_and_alert_sends_to_matching_channels(self, alert_manager: AlertController) -> None:
        """Test that evaluate_trigger_and_alert sends alerts to channels when trigger conditions are met."""
        # Mock the channel send_alert methods
        for channel in alert_manager.channels:
            channel.trigger = MagicMock()

        # Create an exception that should trigger the alert
        test_exception = ValueError("critical error occurred")

        # Process the alert with environment that matches
        os.environ["ENV"] = "production"
        alert_manager.evaluate_trigger_and_alert("Test Alert", "Test message", test_exception)

        # Clean up
        if "ENV" in os.environ:
            del os.environ["ENV"]

        # Verify alerts were sent to both channels
        file_channel = next(ch for ch in alert_manager.channels if ch.name == "test-file")
        webhook_channel = next(ch for ch in alert_manager.channels if ch.name == "test-webhook")

        file_channel.trigger.assert_called_once()
        webhook_channel.trigger.assert_called_once()

        # Verify the formatted messages
        _, kwargs = file_channel.trigger.call_args
        expected_title = "[ALERT] Test Alert - ETL Pipeline"
        expected_body = "Alert Details:\nTest message\n\nPlease investigate."

        assert kwargs["title"] == expected_title
        assert kwargs["body"] == expected_body

    def test_evaluate_trigger_and_alert_skips_disabled_triggers(self, sample_alert_config) -> None:
        """Test that evaluate_trigger_and_alert skips disabled triggers."""
        # Disable the trigger
        sample_alert_config["alert"]["triggers"][0]["enabled"] = False
        manager = AlertController.from_dict(sample_alert_config)

        # Mock the channel send_alert methods
        for channel in manager.channels:
            channel.trigger = MagicMock()

        # Process an alert
        test_exception = ValueError("critical error occurred")
        manager.evaluate_trigger_and_alert("Test Alert", "Test message", test_exception)

        # Verify no alerts were sent
        for channel in manager.channels:
            channel.trigger.assert_not_called()

    def test_evaluate_trigger_and_alert_skips_non_matching_conditions(self, sample_alert_config: dict) -> None:
        """Test that evaluate_trigger_and_alert skips triggers when conditions don't match."""
        # Change conditions so they won't match
        non_matching_conditions = {
            "exception_regex": ".*timeout.*",
            "env_vars_matches": {"ENV": ["production"]},
        }
        sample_alert_config["alert"]["triggers"][0]["conditions"] = non_matching_conditions
        manager = AlertController.from_dict(sample_alert_config)

        # Mock the channel send_alert methods
        for channel in manager.channels:
            channel.trigger = MagicMock()

        # Process an alert with non-matching exception
        test_exception = ValueError("network error")
        manager.evaluate_trigger_and_alert("Test Alert", "Test message", test_exception)

        # Verify no alerts were sent
        for channel in manager.channels:
            channel.trigger.assert_not_called()

    def test_evaluate_trigger_and_alert_handles_missing_channel(self, sample_alert_config) -> None:
        """Test that evaluate_trigger_and_alert handles gracefully when referenced channel doesn't exist."""
        # Add a trigger that references a non-existent channel
        sample_alert_config["alert"]["triggers"][0]["channel_names"] = ["non-existent-channel"]
        manager = AlertController.from_dict(sample_alert_config)

        # Process an alert
        test_exception = ValueError("critical error occurred")

        # Should not raise an exception
        manager.evaluate_trigger_and_alert("Test Alert", "Test message", test_exception)

    def test_multiple_triggers_can_fire(self, sample_alert_config: dict) -> None:
        """Test that multiple triggers can fire for the same alert."""
        # Add a secondary trigger with different template
        secondary_trigger = {
            "name": "secondary-trigger",
            "enabled": True,
            "channel_names": ["test-file"],
            "Template": {
                "prepend_title": "[SECONDARY] ",
                "append_title": "",
                "prepend_body": "Secondary alert: ",
                "append_body": "",
            },
            "conditions": {"exception_regex": "", "env_vars_matches": {}},
        }

        sample_alert_config["alert"]["triggers"].append(secondary_trigger)

        manager = AlertController.from_dict(sample_alert_config)

        # Mock the channel send_alert methods
        for channel in manager.channels:
            channel.trigger = MagicMock()

        # Process an alert that should trigger both triggers (set ENV to match first trigger)
        test_exception = ValueError("critical error occurred")
        os.environ["ENV"] = "production"
        manager.evaluate_trigger_and_alert("Test Alert", "Test message", test_exception)

        # Clean up
        if "ENV" in os.environ:
            del os.environ["ENV"]

        # File channel should receive alerts from both triggers
        file_channel = next(ch for ch in manager.channels if ch.name == "test-file")
        assert file_channel.trigger.call_count == 2
