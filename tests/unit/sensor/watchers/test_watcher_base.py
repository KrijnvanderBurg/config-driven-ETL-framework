"""Unit tests for watcher base classes.

This module contains comprehensive tests for watcher base classes and wrapper functionality,
including configuration loading and validation.
"""

import pytest

from flint.exceptions import FlintConfigurationKeyError
from flint.sensor.watchers.base import Watcher


class TestWatcher:
    """Test cases for Watcher wrapper class."""

    @pytest.fixture
    def sample_file_system_watcher_config(self) -> dict:
        """Provide a complete file system watcher configuration for testing."""
        return {
            "name": "file-watcher",
            "type": "file_system",
            "enabled": True,
            "config": {
                "location": {"path": "incoming/daily/", "recursive": True},
                "file_patterns": {
                    "include_fnmatch_patterns": ["*.csv", "*.json", "*.parquet"],
                    "exclude_fnmatch_patterns": [".*", "*.tmp", "*_backup_*"],
                },
                "trigger_conditions": {
                    "minimum_file_count": 5,
                    "minimum_total_size_mb": 100,
                    "maximum_file_age_hours": 1,
                },
            },
            "trigger_actions": ["notify-files-ready", "etl-job-x"],
        }

    @pytest.fixture
    def sample_http_polling_watcher_config(self) -> dict:
        """Provide a complete HTTP polling watcher configuration for testing."""
        return {
            "name": "api-health-monitor",
            "type": "http_polling",
            "enabled": True,
            "config": {
                "polling": {"interval_seconds": 60, "failure_threshold": 3},
                "request": {
                    "url": "https://api.example.com/health",
                    "method": "GET",
                    "headers": {"Authorization": "Bearer token123", "Content-Type": "application/json"},
                    "timeout": 30,
                    "retry": {"error_on_alert_failure": False, "attempts": 3, "delay_in_seconds": 5},
                },
                "trigger_conditions": {"status_codes": [200, 201], "response_regex": ["healthy", "ok"]},
            },
            "trigger_actions": ["notify-api-healthy"],
        }

    @pytest.fixture
    def disabled_watcher_config(self) -> dict:
        """Provide a disabled watcher configuration for testing."""
        return {
            "name": "disabled-watcher",
            "type": "file_system",
            "enabled": False,
            "config": {
                "location": {"path": "incoming/", "recursive": True},
                "file_patterns": {"include_fnmatch_patterns": ["*.txt"], "exclude_fnmatch_patterns": []},
                "trigger_conditions": {
                    "minimum_file_count": 1,
                    "minimum_total_size_mb": 0,
                    "maximum_file_age_hours": 24,
                },
            },
            "trigger_actions": [],
        }

    def test_from_dict_file_system_watcher_success(self, sample_file_system_watcher_config: dict) -> None:
        """Test successful Watcher creation with file system watcher from dictionary."""
        watcher = Watcher.from_dict(sample_file_system_watcher_config)

        assert watcher.name == "file-watcher"
        assert watcher.type == "file_system"
        assert watcher.enabled is True
        assert watcher.trigger_actions == ["notify-files-ready", "etl-job-x"]

        # Test that config is a FileSystemWatcher instance
        assert hasattr(watcher.config, "location")
        assert hasattr(watcher.config, "file_patterns")
        assert hasattr(watcher.config, "trigger_conditions")

    def test_from_dict_http_polling_watcher_success(self, sample_http_polling_watcher_config: dict) -> None:
        """Test successful Watcher creation with HTTP polling watcher from dictionary."""
        watcher = Watcher.from_dict(sample_http_polling_watcher_config)

        assert watcher.name == "api-health-monitor"
        assert watcher.type == "http_polling"
        assert watcher.enabled is True
        assert watcher.trigger_actions == ["notify-api-healthy"]

        # Test that config is an HttpPollingWatcher instance
        assert hasattr(watcher.config, "polling")
        assert hasattr(watcher.config, "request")
        assert hasattr(watcher.config, "trigger_conditions")

    def test_from_dict_disabled_watcher(self, disabled_watcher_config: dict) -> None:
        """Test Watcher creation with disabled watcher."""
        watcher = Watcher.from_dict(disabled_watcher_config)

        assert watcher.name == "disabled-watcher"
        assert watcher.type == "file_system"
        assert watcher.enabled is False
        assert watcher.trigger_actions == []

    def test_from_dict_missing_name_raises_error(self) -> None:
        """Test that missing name raises FlintConfigurationKeyError."""
        config = {"type": "file_system", "enabled": True, "config": {}, "trigger_actions": []}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            Watcher.from_dict(config)

        assert exc_info.value.key == "name"

    def test_from_dict_missing_type_raises_error(self) -> None:
        """Test that missing type raises FlintConfigurationKeyError."""
        config = {"name": "test-watcher", "enabled": True, "config": {}, "trigger_actions": []}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            Watcher.from_dict(config)

        assert exc_info.value.key == "type"

    def test_from_dict_missing_enabled_raises_error(self) -> None:
        """Test that missing enabled raises FlintConfigurationKeyError."""
        config = {"name": "test-watcher", "type": "file_system", "config": {}, "trigger_actions": []}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            Watcher.from_dict(config)

        assert exc_info.value.key == "enabled"

    def test_from_dict_missing_config_raises_error(self) -> None:
        """Test that missing config raises FlintConfigurationKeyError."""
        config = {"name": "test-watcher", "type": "file_system", "enabled": True, "trigger_actions": []}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            Watcher.from_dict(config)

        assert exc_info.value.key == "config"

    def test_from_dict_missing_trigger_actions_raises_error(self) -> None:
        """Test that missing trigger_actions raises FlintConfigurationKeyError."""
        config = {"name": "test-watcher", "type": "file_system", "enabled": True, "config": {}}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            Watcher.from_dict(config)

        assert exc_info.value.key == "trigger_actions"

    def test_from_dict_unknown_watcher_type_raises_error(self) -> None:
        """Test that unknown watcher type raises ValueError."""
        config = {"name": "test-watcher", "type": "unknown_type", "enabled": True, "config": {}, "trigger_actions": []}
        with pytest.raises(ValueError) as exc_info:
            Watcher.from_dict(config)

        assert "Unknown watcher type: unknown_type" in str(exc_info.value)

    def test_from_dict_empty_trigger_actions_list(self) -> None:
        """Test Watcher creation with empty trigger_actions list."""
        config = {
            "name": "test-watcher",
            "type": "file_system",
            "enabled": True,
            "config": {
                "location": {"path": "incoming/", "recursive": True},
                "file_patterns": {"include_fnmatch_patterns": ["*.txt"], "exclude_fnmatch_patterns": []},
                "trigger_conditions": {
                    "minimum_file_count": 1,
                    "minimum_total_size_mb": 0,
                    "maximum_file_age_hours": 24,
                },
            },
            "trigger_actions": [],
        }
        watcher = Watcher.from_dict(config)

        assert watcher.trigger_actions == []

    def test_from_dict_multiple_trigger_actions(self) -> None:
        """Test Watcher creation with multiple trigger actions."""
        config = {
            "name": "test-watcher",
            "type": "file_system",
            "enabled": True,
            "config": {
                "location": {"path": "incoming/", "recursive": True},
                "file_patterns": {"include_fnmatch_patterns": ["*.txt"], "exclude_fnmatch_patterns": []},
                "trigger_conditions": {
                    "minimum_file_count": 1,
                    "minimum_total_size_mb": 0,
                    "maximum_file_age_hours": 24,
                },
            },
            "trigger_actions": ["action1", "action2", "action3"],
        }
        watcher = Watcher.from_dict(config)

        assert watcher.trigger_actions == ["action1", "action2", "action3"]
