"""Unit tests for action base classes.

This module contains comprehensive tests for action base classes and wrapper functionality,
including configuration loading and validation.
"""

import pytest

from flint.exceptions import FlintConfigurationKeyError
from flint.sensor.actions.base import SensorAction


class TestSensorAction:
    """Test cases for SensorAction wrapper class."""

    @pytest.fixture
    def sample_http_post_action_config(self) -> dict:
        """Provide a complete HTTP POST action configuration for testing."""
        return {
            "name": "notify-files-ready",
            "type": "http_post",
            "config": {
                "url": "https://webhook.example.com/files-ready",
                "method": "POST",
                "headers": {"Authorization": "Bearer webhook-token", "Content-Type": "application/json"},
                "timeout": 30,
                "retry": {"error_on_alert_failure": False, "attempts": 3, "delay_in_seconds": 5},
            },
        }

    @pytest.fixture
    def sample_etl_action_config(self) -> dict:
        """Provide a complete ETL action configuration for testing."""
        return {
            "name": "trigger-etl-pipeline",
            "type": "trigger_job",
            "config": {"job_names": ["extract-data", "transform-data", "load-data"]},
        }

    @pytest.fixture
    def minimal_action_config(self) -> dict:
        """Provide a minimal action configuration for testing."""
        return {
            "name": "simple-action",
            "type": "http_post",
            "config": {
                "url": "https://webhook.example.com/simple",
                "method": "GET",
                "headers": {},
                "timeout": 10,
                "retry": {"error_on_alert_failure": True, "attempts": 1, "delay_in_seconds": 1},
            },
        }

    def test_from_dict_http_post_action_success(self, sample_http_post_action_config: dict) -> None:
        """Test successful SensorAction creation with HTTP POST action from dictionary."""
        action = SensorAction.from_dict(sample_http_post_action_config)

        assert action.name == "notify-files-ready"
        assert action.type == "http_post"

        # Test that config is an HttpPostAction instance
        assert hasattr(action.config, "url")
        assert hasattr(action.config, "method")
        assert hasattr(action.config, "headers")
        assert hasattr(action.config, "retry")

    def test_from_dict_etl_action_success(self, sample_etl_action_config: dict) -> None:
        """Test successful SensorAction creation with ETL action from dictionary."""
        action = SensorAction.from_dict(sample_etl_action_config)

        assert action.name == "trigger-etl-pipeline"
        assert action.type == "trigger_job"

        # Test that config is an EtlInSensor instance
        assert hasattr(action.config, "job_names")

    def test_from_dict_minimal_config(self, minimal_action_config: dict) -> None:
        """Test SensorAction creation with minimal configuration."""
        action = SensorAction.from_dict(minimal_action_config)

        assert action.name == "simple-action"
        assert action.type == "http_post"

    def test_from_dict_missing_name_raises_error(self) -> None:
        """Test that missing name raises FlintConfigurationKeyError."""
        config = {"type": "http_post", "config": {}}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            SensorAction.from_dict(config)

        assert exc_info.value.key == "name"

    def test_from_dict_missing_type_raises_error(self) -> None:
        """Test that missing type raises FlintConfigurationKeyError."""
        config = {"name": "test-action", "config": {}}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            SensorAction.from_dict(config)

        assert exc_info.value.key == "type"

    def test_from_dict_missing_config_raises_error(self) -> None:
        """Test that missing config raises FlintConfigurationKeyError."""
        config = {"name": "test-action", "type": "http_post"}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            SensorAction.from_dict(config)

        assert exc_info.value.key == "config"

    def test_from_dict_unknown_action_type_raises_error(self) -> None:
        """Test that unknown action type raises ValueError."""
        config = {"name": "test-action", "type": "unknown_type", "config": {}}
        with pytest.raises(ValueError) as exc_info:
            SensorAction.from_dict(config)

        assert "Unknown action type: unknown_type" in str(exc_info.value)

    def test_from_dict_various_action_names(self) -> None:
        """Test SensorAction creation with various action names."""
        action_names = ["simple-action", "action_with_underscores", "action.with.dots", "action-123", "CamelCaseAction"]

        for name in action_names:
            config = {
                "name": name,
                "type": "http_post",
                "config": {
                    "url": "https://webhook.example.com/test",
                    "method": "POST",
                    "headers": {},
                    "timeout": 15,
                    "retry": {"error_on_alert_failure": False, "attempts": 1, "delay_in_seconds": 1},
                },
            }
            action = SensorAction.from_dict(config)
            assert action.name == name

    def test_from_dict_both_action_types(self) -> None:
        """Test that both supported action types work correctly."""
        # Test HTTP POST action
        http_config = {
            "name": "http-action",
            "type": "http_post",
            "config": {
                "url": "https://webhook.example.com/test",
                "method": "POST",
                "headers": {},
                "timeout": 20,
                "retry": {"error_on_alert_failure": False, "attempts": 1, "delay_in_seconds": 1},
            },
        }
        http_action = SensorAction.from_dict(http_config)
        assert http_action.type == "http_post"

        # Test ETL trigger action
        etl_config = {"name": "etl-action", "type": "trigger_job", "config": {"job_names": ["test-job"]}}
        etl_action = SensorAction.from_dict(etl_config)
        assert etl_action.type == "trigger_job"

    def test_sensor_action_dataclass_properties(self, sample_http_post_action_config: dict) -> None:
        """Test that SensorAction dataclass behaves as expected."""
        action1 = SensorAction.from_dict(sample_http_post_action_config)
        action2 = SensorAction.from_dict(sample_http_post_action_config)

        # Test equality
        assert action1.name == action2.name
        assert action1.type == action2.type

        # Test that it has the expected attributes
        assert hasattr(action1, "name")
        assert hasattr(action1, "type")
        assert hasattr(action1, "config")
