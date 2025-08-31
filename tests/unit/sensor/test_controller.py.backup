"""Unit tests for sensor controller.

This module contains comprehensive tests for sensor controller functionality,
including configuration loading and validation.
"""

import pytest

from flint.exceptions import FlintConfigurationKeyError
from flint.sensor.controller import SensorController


class TestSensorController:
    """Test cases for SensorController class."""

    @pytest.fixture
    def sample_sensor_controller_config(self) -> dict:
        """Provide a complete sensor controller configuration for testing."""
        return {
            "schedule": {
                "expression": "*/5 * * * *",
                "timezone": "UTC"
            },
            "watchers": [
                {
                    "name": "file-watcher",
                    "type": "file_system",
                    "enabled": True,
                    "config": {
                        "location": {
                            "path": "incoming/daily/",
                            "recursive": True
                        },
                        "file_patterns": {
                            "include_fnmatch_patterns": ["*.csv", "*.json", "*.parquet"],
                            "exclude_fnmatch_patterns": [".*", "*.tmp", "*_backup_*"]
                        },
                        "trigger_conditions": {
                            "minimum_file_count": 5,
                            "minimum_total_size_mb": 100,
                            "maximum_file_age_hours": 1
                        }
                    },
                    "trigger_actions": ["notify-files-ready"]
                }
            ],
            "actions": [
                {
                    "name": "notify-files-ready",
                    "type": "http_post",
                    "config": {
                        "url": "https://webhook.example.com/files-ready",
                        "method": "POST",
                        "headers": {
                            "Authorization": "Bearer webhook-token",
                            "Content-Type": "application/json"
                        },
                        "retry": {
                            "error_on_alert_failure": False,
                            "attempts": 3,
                            "delay_in_seconds": 5
                        }
                    }
                }
            ]
        }

    @pytest.fixture
    def minimal_sensor_controller_config(self) -> dict:
        """Provide a minimal sensor manager configuration for testing."""
        return {
            "schedule": {
                "expression": "0 * * * *",
                "timezone": "UTC"
            },
            "watchers": [],
            "actions": []
        }

    @pytest.fixture
    def multiple_watchers_config(self) -> dict:
        """Provide a sensor manager configuration with multiple watchers."""
        return {
            "schedule": {
                "expression": "*/10 * * * *",
                "timezone": "America/New_York"
            },
            "watchers": [
                {
                    "name": "file-watcher-1",
                    "type": "file_system",
                    "enabled": True,
                    "config": {
                        "location": {"path": "incoming/csv/", "recursive": False},
                        "file_patterns": {"include_fnmatch_patterns": ["*.csv"], "exclude_fnmatch_patterns": []},
                        "trigger_conditions": {"minimum_file_count": 1, "minimum_total_size_mb": 0, "maximum_file_age_hours": 24}
                    },
                    "trigger_actions": ["process-csv"]
                },
                {
                    "name": "api-monitor",
                    "type": "http_polling",
                    "enabled": True,
                    "config": {
                        "polling": {"interval_seconds": 30, "failure_threshold": 2},
                        "request": {
                            "url": "https://api.example.com/status",
                            "method": "GET",
                            "headers": {},
                            "timeout": 10,
                            "retry": {"error_on_alert_failure": True, "attempts": 1, "delay_in_seconds": 1}
                        },
                        "trigger_conditions": {"status_codes": [200], "response_regex": ["ok"]}
                    },
                    "trigger_actions": ["api-health-check"]
                }
            ],
            "actions": [
                {
                    "name": "process-csv",
                    "type": "trigger_job",
                    "config": {"job_names": ["csv-processor"]}
                },
                {
                    "name": "api-health-check",
                    "type": "http_post",
                    "config": {
                        "url": "https://monitoring.example.com/alert",
                        "method": "POST",
                        "headers": {"Content-Type": "application/json"},
                        "retry": {"error_on_alert_failure": False, "attempts": 2, "delay_in_seconds": 3}
                    }
                }
            ]
        }

    def test_from_dict_success(self, sample_sensor_controller_config: dict) -> None:
        """Test successful SensorController creation from dictionary."""
        manager = SensorController.from_dict(sample_sensor_controller_config)

        # Test schedule
        assert manager.schedule.expression == "*/5 * * * *"
        assert manager.schedule.timezone == "UTC"

        # Test watchers
        assert len(manager.watchers) == 1
        assert manager.watchers[0].name == "file-watcher"
        assert manager.watchers[0].type == "file_system"
        assert manager.watchers[0].enabled is True

        # Test actions
        assert len(manager.actions) == 1
        assert manager.actions[0].name == "notify-files-ready"
        assert manager.actions[0].type == "http_post"

    def test_from_dict_minimal_config(self, minimal_sensor_controller_config: dict) -> None:
        """Test SensorController creation with minimal configuration."""
        manager = SensorController.from_dict(minimal_sensor_controller_config)

        assert manager.schedule.expression == "0 * * * *"
        assert manager.schedule.timezone == "UTC"
        assert len(manager.watchers) == 0
        assert len(manager.actions) == 0

    def test_from_dict_multiple_watchers(self, multiple_watchers_config: dict) -> None:
        """Test SensorController creation with multiple watchers and actions."""
        manager = SensorController.from_dict(multiple_watchers_config)

        # Test schedule
        assert manager.schedule.expression == "*/10 * * * *"
        assert manager.schedule.timezone == "America/New_York"

        # Test watchers
        assert len(manager.watchers) == 2
        assert manager.watchers[0].name == "file-watcher-1"
        assert manager.watchers[0].type == "file_system"
        assert manager.watchers[1].name == "api-monitor"
        assert manager.watchers[1].type == "http_polling"

        # Test actions
        assert len(manager.actions) == 2
        assert manager.actions[0].name == "process-csv"
        assert manager.actions[0].type == "trigger_job"
        assert manager.actions[1].name == "api-health-check"
        assert manager.actions[1].type == "http_post"

    def test_from_dict_missing_schedule_raises_error(self) -> None:
        """Test that missing schedule raises FlintConfigurationKeyError."""
        config = {
            "watchers": [],
            "actions": []
        }
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            SensorController.from_dict(config)

        assert exc_info.value.key == "schedule"

    def test_from_dict_missing_watchers_raises_error(self) -> None:
        """Test that missing watchers raises FlintConfigurationKeyError."""
        config = {
            "schedule": {"expression": "0 * * * *", "timezone": "UTC"},
            "actions": []
        }
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            SensorController.from_dict(config)

        assert exc_info.value.key == "watchers"

    def test_from_dict_missing_actions_raises_error(self) -> None:
        """Test that missing actions raises FlintConfigurationKeyError."""
        config = {
            "schedule": {"expression": "0 * * * *", "timezone": "UTC"},
            "watchers": []
        }
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            SensorController.from_dict(config)

        assert exc_info.value.key == "actions"

    def test_from_dict_empty_watchers_and_actions(self) -> None:
        """Test SensorController creation with empty watchers and actions lists."""
        config = {
            "schedule": {"expression": "0 * * * *", "timezone": "UTC"},
            "watchers": [],
            "actions": []
        }
        manager = SensorController.from_dict(config)

        assert len(manager.watchers) == 0
        assert len(manager.actions) == 0

    def test_from_dict_disabled_watchers(self) -> None:
        """Test SensorController creation with disabled watchers."""
        config = {
            "schedule": {"expression": "0 * * * *", "timezone": "UTC"},
            "watchers": [
                {
                    "name": "disabled-watcher",
                    "type": "file_system",
                    "enabled": False,
                    "config": {
                        "location": {"path": "test/", "recursive": True},
                        "file_patterns": {"include_fnmatch_patterns": ["*.txt"], "exclude_fnmatch_patterns": []},
                        "trigger_conditions": {"minimum_file_count": 1, "minimum_total_size_mb": 0, "maximum_file_age_hours": 24}
                    },
                    "trigger_actions": []
                }
            ],
            "actions": []
        }
        manager = SensorController.from_dict(config)

        assert len(manager.watchers) == 1
        assert manager.watchers[0].enabled is False

    def test_sensor_manager_dataclass_properties(self, sample_sensor_controller_config: dict) -> None:
        """Test that SensorController dataclass behaves as expected."""
        manager1 = SensorController.from_dict(sample_sensor_controller_config)
        manager2 = SensorController.from_dict(sample_sensor_controller_config)

        # Test that it has the expected attributes
        assert hasattr(manager1, 'schedule')
        assert hasattr(manager1, 'watchers')
        assert hasattr(manager1, 'actions')

        # Test equality of basic attributes
        assert manager1.schedule.expression == manager2.schedule.expression
        assert len(manager1.watchers) == len(manager2.watchers)
        assert len(manager1.actions) == len(manager2.actions)
