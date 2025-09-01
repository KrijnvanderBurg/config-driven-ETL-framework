"""Unit tests for HTTP POST action models.

This module contains comprehensive tests for HTTP POST action model functionality,
including configuration loading and validation.
"""

import pytest

from flint.exceptions import FlintConfigurationKeyError
from flint.sensor.actions.http_post import ActionRetry, HttpPostAction


class TestActionRetry:
    """Test cases for ActionRetry class."""

    @pytest.fixture
    def sample_retry_config(self) -> dict:
        """Provide a sample retry configuration for testing."""
        return {"error_on_alert_failure": False, "attempts": 3, "delay_in_seconds": 5}

    @pytest.fixture
    def minimal_retry_config(self) -> dict:
        """Provide a minimal retry configuration for testing."""
        return {"error_on_alert_failure": True, "attempts": 1, "delay_in_seconds": 1}

    def test_from_dict_success(self, sample_retry_config: dict) -> None:
        """Test successful ActionRetry creation from dictionary."""
        retry = ActionRetry.from_dict(sample_retry_config)

        assert retry.attempts == 3
        assert retry.delay_in_seconds == 5

    def test_from_dict_minimal_config(self, minimal_retry_config: dict) -> None:
        """Test ActionRetry creation with minimal configuration."""
        retry = ActionRetry.from_dict(minimal_retry_config)

        assert retry.attempts == 1
        assert retry.delay_in_seconds == 1

    def test_from_dict_missing_attempts_raises_error(self) -> None:
        """Test that missing attempts raises FlintConfigurationKeyError."""
        config = {"error_on_alert_failure": False, "delay_in_seconds": 5}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            ActionRetry.from_dict(config)

        assert exc_info.value.key == "attempts"

    def test_from_dict_missing_delay_in_seconds_raises_error(self) -> None:
        """Test that missing delay_in_seconds raises FlintConfigurationKeyError."""
        config = {"error_on_alert_failure": False, "attempts": 3}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            ActionRetry.from_dict(config)

        assert exc_info.value.key == "delay_in_seconds"

        config = {"attempts": 3, "delay_in_seconds": 5}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            ActionRetry.from_dict(config)


class TestHttpPostAction:
    """Test cases for HttpPostAction class."""

    @pytest.fixture
    def sample_http_post_action_config(self) -> dict:
        """Provide a complete HTTP POST action configuration for testing."""
        return {
            "url": "https://webhook.example.com/files-ready",
            "method": "POST",
            "headers": {"Authorization": "Bearer webhook-token", "Content-Type": "application/json"},
            "timeout": 30,
            "retry": {
                "error_on_alert_failure": False,
                "attempts": 3,
                "delay_in_seconds": 5,
            },
        }

    @pytest.fixture
    def minimal_http_post_action_config(self) -> dict:
        """Provide a minimal HTTP POST action configuration for testing."""
        return {
            "url": "https://webhook.example.com/simple",
            "method": "GET",
            "headers": {},
            "timeout": 10,
            "retry": {
                "error_on_alert_failure": True,
                "attempts": 1,
                "delay_in_seconds": 1,
            },
        }

    def test_from_dict_success(self, sample_http_post_action_config: dict) -> None:
        """Test successful HttpPostAction creation from dictionary."""
        action = HttpPostAction.from_dict(sample_http_post_action_config)

        assert action.url == "https://webhook.example.com/files-ready"
        assert action.method == "POST"
        assert action.headers == {"Authorization": "Bearer webhook-token", "Content-Type": "application/json"}
        assert action.timeout == 30
        assert action.retry.error_on_alert_failure is False
        assert action.retry.attempts == 3
        assert action.retry.delay_in_seconds == 5

    def test_from_dict_minimal_config(self, minimal_http_post_action_config: dict) -> None:
        """Test HttpPostAction creation with minimal configuration."""
        action = HttpPostAction.from_dict(minimal_http_post_action_config)

        assert action.url == "https://webhook.example.com/simple"
        assert action.method == "GET"
        assert action.headers == {}
        assert action.timeout == 10
        assert action.retry.error_on_alert_failure is True
        assert action.retry.attempts == 1
        assert action.retry.delay_in_seconds == 1

        assert action.url == "https://webhook.example.com/simple"
        assert action.method == "GET"
        assert action.headers == {}
        assert action.retry.attempts == 1
        assert action.retry.delay_in_seconds == 1

    def test_from_dict_missing_url_raises_error(self) -> None:
        """Test that missing url raises FlintConfigurationKeyError."""
        config = {
            "method": "POST",
            "headers": {},
        }
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            HttpPostAction.from_dict(config)

        assert exc_info.value.key == "url"

    def test_from_dict_missing_method_raises_error(self) -> None:
        """Test that missing method raises FlintConfigurationKeyError."""
        config = {
            "url": "https://webhook.example.com/files-ready",
            "headers": {},
        }
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            HttpPostAction.from_dict(config)

        assert exc_info.value.key == "method"

    def test_from_dict_missing_headers_raises_error(self) -> None:
        """Test that missing headers raises FlintConfigurationKeyError."""
        config = {
            "url": "https://webhook.example.com/files-ready",
            "method": "POST",
        }
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            HttpPostAction.from_dict(config)

        assert exc_info.value.key == "headers"

    def test_from_dict_missing_retry_raises_error(self) -> None:
        """Test that missing retry raises FlintConfigurationKeyError."""
        config = {
            "url": "https://webhook.example.com/files-ready",
            "method": "POST",
            "headers": {"Authorization": "Bearer webhook-token"},
            "timeout": 30,
        }
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            HttpPostAction.from_dict(config)

        assert exc_info.value.key == "retry"

    def test_from_dict_various_http_methods(self) -> None:
        """Test HttpPostAction creation with various HTTP methods."""
        methods = ["GET", "POST", "PUT", "PATCH", "DELETE"]

        for method in methods:
            config = {
                "url": f"https://api.example.com/{method.lower()}",
                "method": method,
                "headers": {},
                "timeout": 25,
                "retry": {"error_on_alert_failure": False, "attempts": 1, "delay_in_seconds": 1},
            }
            action = HttpPostAction.from_dict(config)
            assert action.method == method
            assert action.url == f"https://api.example.com/{method.lower()}"

    def test_from_dict_empty_headers(self) -> None:
        """Test HttpPostAction creation with empty headers."""
        config = {
            "url": "https://webhook.example.com/no-headers",
            "method": "POST",
            "headers": {},
            "timeout": 30,
            "retry": {"error_on_alert_failure": True, "attempts": 1, "delay_in_seconds": 1},
        }
        action = HttpPostAction.from_dict(config)
        assert action.headers == {}

    def test_from_dict_complex_headers(self) -> None:
        """Test HttpPostAction creation with complex headers."""
        config = {
            "url": "https://webhook.example.com/complex",
            "method": "POST",
            "headers": {
                "Authorization": "Bearer complex-token-12345",
                "Content-Type": "application/json",
                "X-Custom-Header": "custom-value",
                "User-Agent": "Flint-Sensor/1.0",
            },
            "timeout": 45,
            "retry": {"error_on_alert_failure": False, "attempts": 2, "delay_in_seconds": 3},
        }
        action = HttpPostAction.from_dict(config)

        expected_headers = {
            "Authorization": "Bearer complex-token-12345",
            "Content-Type": "application/json",
            "X-Custom-Header": "custom-value",
            "User-Agent": "Flint-Sensor/1.0",
        }
        assert action.headers == expected_headers
