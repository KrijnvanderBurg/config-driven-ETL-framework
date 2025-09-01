"""Unit tests for sensor watcher model classes.

This module contains comprehensive tests for watcher model functionality,
including configuration loading and validation for all watcher types.
"""

import pytest

from flint.exceptions import FlintConfigurationKeyError
from flint.sensor.watchers.file_system import (
    FilePatterns,
    FileSystemTriggerConditions,
    FileSystemWatcher,
    WatcherLocation,
)
from flint.sensor.watchers.http_polling import (
    HttpPollingWatcher,
    HttpTriggerConditions,
    PollingConfig,
    RequestConfig,
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


class TestPollingConfig:
    """Test cases for PollingConfig class."""

    @pytest.fixture
    def sample_polling_config(self) -> dict:
        """Provide a sample polling configuration for testing."""
        return {"interval_seconds": 60, "failure_threshold": 3}

    @pytest.fixture
    def high_frequency_polling_config(self) -> dict:
        """Provide a high frequency polling configuration for testing."""
        return {"interval_seconds": 5, "failure_threshold": 1}

    def test_from_dict_success(self, sample_polling_config: dict) -> None:
        """Test successful PollingConfig creation from dictionary."""
        polling = PollingConfig.from_dict(sample_polling_config)

        assert polling.interval_seconds == 60
        assert polling.failure_threshold == 3

    def test_from_dict_high_frequency(self, high_frequency_polling_config: dict) -> None:
        """Test PollingConfig creation with high frequency settings."""
        polling = PollingConfig.from_dict(high_frequency_polling_config)

        assert polling.interval_seconds == 5
        assert polling.failure_threshold == 1

    def test_from_dict_missing_interval_seconds_raises_error(self) -> None:
        """Test that missing interval_seconds raises FlintConfigurationKeyError."""
        config = {"failure_threshold": 3}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            PollingConfig.from_dict(config)

        assert exc_info.value.key == "interval_seconds"

    def test_from_dict_missing_failure_threshold_raises_error(self) -> None:
        """Test that missing failure_threshold raises FlintConfigurationKeyError."""
        config = {"interval_seconds": 60}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            PollingConfig.from_dict(config)

        assert exc_info.value.key == "failure_threshold"


class TestRequestConfig:
    """Test cases for RequestConfig class."""

    @pytest.fixture
    def sample_request_config(self) -> dict:
        """Provide a sample request configuration for testing."""
        return {
            "url": "https://api.example.com/health",
            "method": "GET",
            "headers": {"Authorization": "Bearer token123", "Content-Type": "application/json"},
            "timeout": 30,
            "retry": {"error_on_alert_failure": False, "attempts": 3, "delay_in_seconds": 5},
        }

    @pytest.fixture
    def minimal_request_config(self) -> dict:
        """Provide a minimal request configuration for testing."""
        return {
            "url": "https://api.example.com/ping",
            "method": "HEAD",
            "headers": {},
            "timeout": 10,
            "retry": {"error_on_alert_failure": True, "attempts": 1, "delay_in_seconds": 1},
        }

    def test_from_dict_success(self, sample_request_config: dict) -> None:
        """Test successful RequestConfig creation from dictionary."""
        request = RequestConfig.from_dict(sample_request_config)

        assert request.url == "https://api.example.com/health"
        assert request.method == "GET"
        assert request.headers == {"Authorization": "Bearer token123", "Content-Type": "application/json"}
        assert request.timeout == 30
        assert request.retry.error_on_alert_failure is False
        assert request.retry.attempts == 3
        assert request.retry.delay_in_seconds == 5

    def test_from_dict_minimal_config(self, minimal_request_config: dict) -> None:
        """Test RequestConfig creation with minimal configuration."""
        request = RequestConfig.from_dict(minimal_request_config)

        assert request.url == "https://api.example.com/ping"
        assert request.method == "HEAD"
        assert request.headers == {}
        assert request.timeout == 10
        assert request.retry.error_on_alert_failure is True
        assert request.retry.attempts == 1
        assert request.retry.delay_in_seconds == 1

    def test_from_dict_missing_url_raises_error(self) -> None:
        """Test that missing url raises FlintConfigurationKeyError."""
        config = {
            "method": "GET",
            "headers": {},
            "timeout": 30,
            "retry": {"error_on_alert_failure": False, "attempts": 3, "delay_in_seconds": 5},
        }
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            RequestConfig.from_dict(config)

        assert exc_info.value.key == "url"

    def test_from_dict_missing_method_raises_error(self) -> None:
        """Test that missing method raises FlintConfigurationKeyError."""
        config = {
            "url": "https://api.example.com/health",
            "headers": {},
            "timeout": 30,
            "retry": {"error_on_alert_failure": False, "attempts": 3, "delay_in_seconds": 5},
        }
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            RequestConfig.from_dict(config)

        assert exc_info.value.key == "method"

    def test_from_dict_missing_headers_raises_error(self) -> None:
        """Test that missing headers raises FlintConfigurationKeyError."""
        config = {
            "url": "https://api.example.com/health",
            "method": "GET",
            "timeout": 30,
            "retry": {"error_on_alert_failure": False, "attempts": 3, "delay_in_seconds": 5},
        }
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            RequestConfig.from_dict(config)

        assert exc_info.value.key == "headers"

    def test_from_dict_missing_timeout_raises_error(self) -> None:
        """Test that missing timeout raises FlintConfigurationKeyError."""
        config = {
            "url": "https://api.example.com/health",
            "method": "GET",
            "headers": {},
            "retry": {"error_on_alert_failure": False, "attempts": 3, "delay_in_seconds": 5},
        }
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            RequestConfig.from_dict(config)

        assert exc_info.value.key == "timeout"

    def test_from_dict_missing_retry_raises_error(self) -> None:
        """Test that missing retry raises FlintConfigurationKeyError."""
        config = {"url": "https://api.example.com/health", "method": "GET", "headers": {}, "timeout": 30}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            RequestConfig.from_dict(config)

        assert exc_info.value.key == "retry"


class TestHttpTriggerConditions:
    """Test cases for HttpTriggerConditions class."""

    @pytest.fixture
    def sample_http_conditions_config(self) -> dict:
        """Provide a sample HTTP trigger conditions configuration for testing."""
        return {"status_codes": [200, 201], "response_regex": ["healthy", "ok"]}

    @pytest.fixture
    def strict_http_conditions_config(self) -> dict:
        """Provide a strict HTTP trigger conditions configuration for testing."""
        return {"status_codes": [201], "response_regex": ["success"]}

    def test_from_dict_success(self, sample_http_conditions_config: dict) -> None:
        """Test successful HttpTriggerConditions creation from dictionary."""
        conditions = HttpTriggerConditions.from_dict(sample_http_conditions_config)

        assert conditions.status_codes == [200, 201]
        assert conditions.response_regex == ["healthy", "ok"]

    def test_from_dict_strict_conditions(self, strict_http_conditions_config: dict) -> None:
        """Test HttpTriggerConditions creation with strict conditions."""
        conditions = HttpTriggerConditions.from_dict(strict_http_conditions_config)

        assert conditions.status_codes == [201]
        assert conditions.response_regex == ["success"]

    def test_from_dict_missing_status_codes_raises_error(self) -> None:
        """Test that missing status_codes raises FlintConfigurationKeyError."""
        config = {"response_regex": ["healthy"]}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            HttpTriggerConditions.from_dict(config)

        assert exc_info.value.key == "status_codes"

    def test_from_dict_missing_response_regex_raises_error(self) -> None:
        """Test that missing response_regex raises FlintConfigurationKeyError."""
        config = {"status_codes": [200]}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            HttpTriggerConditions.from_dict(config)

        assert exc_info.value.key == "response_regex"


class TestHttpPollingWatcher:
    """Test cases for HttpPollingWatcher class."""

    @pytest.fixture
    def sample_http_polling_watcher_config(self) -> dict:
        """Provide a complete HTTP polling watcher configuration for testing."""
        return {
            "polling": {"interval_seconds": 60, "failure_threshold": 3},
            "request": {
                "url": "https://api.example.com/health",
                "method": "GET",
                "headers": {"Authorization": "Bearer token123", "Content-Type": "application/json"},
                "timeout": 30,
                "retry": {"error_on_alert_failure": False, "attempts": 3, "delay_in_seconds": 5},
            },
            "trigger_conditions": {"status_codes": [200, 201], "response_regex": ["healthy", "ok"]},
        }

    def test_from_dict_success(self, sample_http_polling_watcher_config: dict) -> None:
        """Test successful HttpPollingWatcher creation from dictionary."""
        watcher = HttpPollingWatcher.from_dict(sample_http_polling_watcher_config)

        # Test polling config
        assert watcher.polling.interval_seconds == 60
        assert watcher.polling.failure_threshold == 3

        # Test request config
        assert watcher.request.url == "https://api.example.com/health"
        assert watcher.request.method == "GET"
        assert watcher.request.headers == {"Authorization": "Bearer token123", "Content-Type": "application/json"}
        assert watcher.request.timeout == 30

        # Test trigger conditions
        assert watcher.trigger_conditions.status_codes == [200, 201]
        assert watcher.trigger_conditions.response_regex == ["healthy", "ok"]

    def test_from_dict_missing_polling_raises_error(self) -> None:
        """Test that missing polling raises FlintConfigurationKeyError."""
        config = {
            "request": {
                "url": "https://api.example.com/health",
                "method": "GET",
                "headers": {},
                "timeout": 30,
                "retry": {"error_on_alert_failure": False, "attempts": 3, "delay_in_seconds": 5},
            },
            "trigger_conditions": {"status_codes": [200], "response_regex": ["healthy"]},
        }
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            HttpPollingWatcher.from_dict(config)

        assert exc_info.value.key == "polling"

    def test_from_dict_missing_request_raises_error(self) -> None:
        """Test that missing request raises FlintConfigurationKeyError."""
        config = {
            "polling": {"interval_seconds": 60, "failure_threshold": 3},
            "trigger_conditions": {"status_codes": [200], "response_regex": ["healthy"]},
        }
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            HttpPollingWatcher.from_dict(config)

        assert exc_info.value.key == "request"

    def test_from_dict_missing_trigger_conditions_raises_error(self) -> None:
        """Test that missing trigger_conditions raises FlintConfigurationKeyError."""
        config = {
            "polling": {"interval_seconds": 60, "failure_threshold": 3},
            "request": {
                "url": "https://api.example.com/health",
                "method": "GET",
                "headers": {},
                "timeout": 30,
                "retry": {"error_on_alert_failure": False, "attempts": 3, "delay_in_seconds": 5},
            },
        }
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            HttpPollingWatcher.from_dict(config)

        assert exc_info.value.key == "trigger_conditions"
