"""Unit tests for file system watcher models.

This module contains comprehensive tests for file system watcher model functionality,
including configuration loading and validation.
"""

import pytest

from flint.exceptions import FlintConfigurationKeyError
from flint.sensor.watchers.file_system import (
    FilePatterns,
    FileSystemTriggerConditions,
    FileSystemWatcher,
    WatcherLocation,
)


class TestWatcherLocation:
    """Test cases for WatcherLocation class."""

    @pytest.fixture
    def sample_location_config(self) -> dict:
        """Provide a sample location configuration for testing."""
        return {"path": "incoming/daily/", "recursive": True}

    @pytest.fixture
    def non_recursive_location_config(self) -> dict:
        """Provide a non-recursive location configuration for testing."""
        return {"path": "/data/files/", "recursive": False}

    def test_from_dict_success(self, sample_location_config: dict) -> None:
        """Test successful WatcherLocation creation from dictionary."""
        location = WatcherLocation.from_dict(sample_location_config)

        assert location.path == "incoming/daily/"
        assert location.recursive is True

    def test_from_dict_non_recursive(self, non_recursive_location_config: dict) -> None:
        """Test WatcherLocation creation with recursive=False."""
        location = WatcherLocation.from_dict(non_recursive_location_config)

        assert location.path == "/data/files/"
        assert location.recursive is False

    def test_from_dict_missing_path_raises_error(self) -> None:
        """Test that missing path raises FlintConfigurationKeyError."""
        config = {"recursive": True}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            WatcherLocation.from_dict(config)

        assert exc_info.value.key == "path"

    def test_from_dict_missing_recursive_raises_error(self) -> None:
        """Test that missing recursive raises FlintConfigurationKeyError."""
        config = {"path": "incoming/daily/"}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            WatcherLocation.from_dict(config)

        assert exc_info.value.key == "recursive"


class TestFilePatterns:
    """Test cases for FilePatterns class."""

    @pytest.fixture
    def sample_patterns_config(self) -> dict:
        """Provide a sample file patterns configuration for testing."""
        return {
            "include_fnmatch_patterns": ["*.csv", "*.json", "*.parquet"],
            "exclude_fnmatch_patterns": [".*", "*.tmp", "*_backup_*"],
        }

    @pytest.fixture
    def minimal_patterns_config(self) -> dict:
        """Provide a minimal file patterns configuration for testing."""
        return {"include_fnmatch_patterns": ["*.txt"], "exclude_fnmatch_patterns": []}

    def test_from_dict_success(self, sample_patterns_config: dict) -> None:
        """Test successful FilePatterns creation from dictionary."""
        patterns = FilePatterns.from_dict(sample_patterns_config)

        assert patterns.include_fnmatch_patterns == ["*.csv", "*.json", "*.parquet"]
        assert patterns.exclude_fnmatch_patterns == [".*", "*.tmp", "*_backup_*"]

    def test_from_dict_minimal_config(self, minimal_patterns_config: dict) -> None:
        """Test FilePatterns creation with minimal configuration."""
        patterns = FilePatterns.from_dict(minimal_patterns_config)

        assert patterns.include_fnmatch_patterns == ["*.txt"]
        assert patterns.exclude_fnmatch_patterns == []

    def test_from_dict_missing_include_patterns_raises_error(self) -> None:
        """Test that missing include patterns raises FlintConfigurationKeyError."""
        config = {"exclude_fnmatch_patterns": ["*.tmp"]}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            FilePatterns.from_dict(config)

        assert exc_info.value.key == "include_fnmatch_patterns"

    def test_from_dict_missing_exclude_patterns_raises_error(self) -> None:
        """Test that missing exclude patterns raises FlintConfigurationKeyError."""
        config = {"include_fnmatch_patterns": ["*.csv"]}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            FilePatterns.from_dict(config)

        assert exc_info.value.key == "exclude_fnmatch_patterns"


class TestFileSystemTriggerConditions:
    """Test cases for FileSystemTriggerConditions class."""

    @pytest.fixture
    def sample_conditions_config(self) -> dict:
        """Provide a sample trigger conditions configuration for testing."""
        return {"minimum_file_count": 5, "minimum_total_size_mb": 100, "maximum_file_age_hours": 1}

    @pytest.fixture
    def zero_values_conditions_config(self) -> dict:
        """Provide a trigger conditions configuration with zero values."""
        return {"minimum_file_count": 0, "minimum_total_size_mb": 0, "maximum_file_age_hours": 24}

    def test_from_dict_success(self, sample_conditions_config: dict) -> None:
        """Test successful FileSystemTriggerConditions creation from dictionary."""
        conditions = FileSystemTriggerConditions.from_dict(sample_conditions_config)

        assert conditions.minimum_file_count == 5
        assert conditions.minimum_total_size_mb == 100
        assert conditions.maximum_file_age_hours == 1

    def test_from_dict_zero_values(self, zero_values_conditions_config: dict) -> None:
        """Test FileSystemTriggerConditions creation with zero values."""
        conditions = FileSystemTriggerConditions.from_dict(zero_values_conditions_config)

        assert conditions.minimum_file_count == 0
        assert conditions.minimum_total_size_mb == 0
        assert conditions.maximum_file_age_hours == 24

    def test_from_dict_missing_minimum_file_count_raises_error(self) -> None:
        """Test that missing minimum_file_count raises FlintConfigurationKeyError."""
        config = {"minimum_total_size_mb": 100, "maximum_file_age_hours": 1}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            FileSystemTriggerConditions.from_dict(config)

        assert exc_info.value.key == "minimum_file_count"

    def test_from_dict_missing_minimum_total_size_mb_raises_error(self) -> None:
        """Test that missing minimum_total_size_mb raises FlintConfigurationKeyError."""
        config = {"minimum_file_count": 5, "maximum_file_age_hours": 1}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            FileSystemTriggerConditions.from_dict(config)

        assert exc_info.value.key == "minimum_total_size_mb"

    def test_from_dict_missing_maximum_file_age_hours_raises_error(self) -> None:
        """Test that missing maximum_file_age_hours raises FlintConfigurationKeyError."""
        config = {"minimum_file_count": 5, "minimum_total_size_mb": 100}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            FileSystemTriggerConditions.from_dict(config)

        assert exc_info.value.key == "maximum_file_age_hours"


class TestFileSystemWatcher:
    """Test cases for FileSystemWatcher class."""

    @pytest.fixture
    def sample_file_system_watcher_config(self) -> dict:
        """Provide a complete file system watcher configuration for testing."""
        return {
            "location": {"path": "incoming/daily/", "recursive": True},
            "file_patterns": {
                "include_fnmatch_patterns": ["*.csv", "*.json", "*.parquet"],
                "exclude_fnmatch_patterns": [".*", "*.tmp", "*_backup_*"],
            },
            "trigger_conditions": {"minimum_file_count": 5, "minimum_total_size_mb": 100, "maximum_file_age_hours": 1},
        }

    def test_from_dict_success(self, sample_file_system_watcher_config: dict) -> None:
        """Test successful FileSystemWatcher creation from dictionary."""
        watcher = FileSystemWatcher.from_dict(sample_file_system_watcher_config)

        # Test location
        assert watcher.location.path == "incoming/daily/"
        assert watcher.location.recursive is True

        # Test file patterns
        assert watcher.file_patterns.include_fnmatch_patterns == ["*.csv", "*.json", "*.parquet"]
        assert watcher.file_patterns.exclude_fnmatch_patterns == [".*", "*.tmp", "*_backup_*"]

        # Test trigger conditions
        assert watcher.trigger_conditions.minimum_file_count == 5
        assert watcher.trigger_conditions.minimum_total_size_mb == 100
        assert watcher.trigger_conditions.maximum_file_age_hours == 1

    def test_from_dict_missing_location_raises_error(self) -> None:
        """Test that missing location raises FlintConfigurationKeyError."""
        config = {
            "file_patterns": {"include_fnmatch_patterns": ["*.csv"], "exclude_fnmatch_patterns": []},
            "trigger_conditions": {"minimum_file_count": 1, "minimum_total_size_mb": 0, "maximum_file_age_hours": 24},
        }
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            FileSystemWatcher.from_dict(config)

        assert exc_info.value.key == "location"

    def test_from_dict_missing_file_patterns_raises_error(self) -> None:
        """Test that missing file_patterns raises FlintConfigurationKeyError."""
        config = {
            "location": {"path": "incoming/", "recursive": True},
            "trigger_conditions": {"minimum_file_count": 1, "minimum_total_size_mb": 0, "maximum_file_age_hours": 24},
        }
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            FileSystemWatcher.from_dict(config)

        assert exc_info.value.key == "file_patterns"

    def test_from_dict_missing_trigger_conditions_raises_error(self) -> None:
        """Test that missing trigger_conditions raises FlintConfigurationKeyError."""
        config = {
            "location": {"path": "incoming/", "recursive": True},
            "file_patterns": {"include_fnmatch_patterns": ["*.csv"], "exclude_fnmatch_patterns": []},
        }
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            FileSystemWatcher.from_dict(config)

        assert exc_info.value.key == "trigger_conditions"

    def test_check_conditions_method(self, sample_file_system_watcher_config: dict) -> None:
        """Test that check_conditions method can be called without errors."""
        watcher = FileSystemWatcher.from_dict(sample_file_system_watcher_config)
        # This should not raise an exception and return True (placeholder implementation)
        result = watcher.check_conditions()
        assert result is True
