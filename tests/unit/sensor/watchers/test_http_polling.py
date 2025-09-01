"""Unit tests for HTTP polling watcher models.

This module contains comprehensive tests for HTTP polling watcher model functionality,
including configuration loading and validation.
"""

import pytest

from flint.exceptions import FlintConfigurationKeyError
from flint.sensor.watchers.http_polling import (
    HttpPollingWatcher,
    HttpTriggerConditions,
    PollingConfig,
    RequestConfig,
    RequestRetry,
)


class TestRequestRetry:
    """Test cases for RequestRetry class."""

    @pytest.fixture
    def sample_retry_config(self) -> dict:
        """Provide a sample retry configuration for testing."""
        return {"error_on_alert_failure": False, "attempts": 3, "delay_in_seconds": 5}

    @pytest.fixture
    def strict_retry_config(self) -> dict:
        """Provide a strict retry configuration for testing."""
        return {"error_on_alert_failure": True, "attempts": 1, "delay_in_seconds": 1}

    def test_from_dict_success(self, sample_retry_config: dict) -> None:
        """Test successful RequestRetry creation from dictionary."""
        retry = RequestRetry.from_dict(sample_retry_config)

        assert retry.error_on_alert_failure is False
        assert retry.attempts == 3
        assert retry.delay_in_seconds == 5

    def test_from_dict_strict_config(self, strict_retry_config: dict) -> None:
        """Test RequestRetry creation with strict configuration."""
        retry = RequestRetry.from_dict(strict_retry_config)

        assert retry.error_on_alert_failure is True
        assert retry.attempts == 1
        assert retry.delay_in_seconds == 1

    def test_from_dict_missing_error_on_alert_failure_raises_error(self) -> None:
        """Test that missing error_on_alert_failure raises FlintConfigurationKeyError."""
        config = {"attempts": 3, "delay_in_seconds": 5}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            RequestRetry.from_dict(config)

        assert exc_info.value.key == "error_on_alert_failure"

    def test_from_dict_missing_attempts_raises_error(self) -> None:
        """Test that missing attempts raises FlintConfigurationKeyError."""
        config = {"error_on_alert_failure": False, "delay_in_seconds": 5}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            RequestRetry.from_dict(config)

        assert exc_info.value.key == "attempts"

    def test_from_dict_missing_delay_in_seconds_raises_error(self) -> None:
        """Test that missing delay_in_seconds raises FlintConfigurationKeyError."""
        config = {"error_on_alert_failure": False, "attempts": 3}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            RequestRetry.from_dict(config)

        assert exc_info.value.key == "delay_in_seconds"


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

    def test_check_conditions_method(self, sample_http_polling_watcher_config: dict) -> None:
        """Test that check_conditions method can be called without errors."""
        watcher = HttpPollingWatcher.from_dict(sample_http_polling_watcher_config)
        # This should not raise an exception and return True (placeholder implementation)
        result = watcher.check_conditions()
        assert result is True
