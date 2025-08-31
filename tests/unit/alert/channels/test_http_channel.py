"""Unit tests for the HttpAlertChannel class.

This module contains comprehensive tests for the HttpAlertChannel functionality,
including HTTP requests, retry logic, error handling, and configuration parsing.
"""

import json
from unittest.mock import Mock, patch

import pytest
import requests

from flint.alert.channels.http import HttpAlertChannel, Retry
from flint.exceptions import FlintConfigurationKeyError


class TestRetry:
    """Test cases for Retry class."""

    @pytest.fixture
    def retry_config(self) -> dict:
        """Provide a retry configuration for testing."""
        return {"error_on_alert_failure": True, "attempts": 3, "delay_in_seconds": 5}

    def test_from_dict_creates_retry_correctly(self, retry_config) -> None:
        """Test that from_dict creates a Retry instance correctly."""
        retry = Retry.from_dict(retry_config)

        assert retry.error_on_alert_failure is True
        assert retry.attempts == 3
        assert retry.delay_in_seconds == 5

    def test_from_dict_raises_error_for_missing_keys(self) -> None:
        """Test that from_dict raises error when required keys are missing."""
        incomplete_config = {
            "error_on_alert_failure": True
            # Missing attempts and delay_in_seconds
        }

        with pytest.raises(FlintConfigurationKeyError):
            Retry.from_dict(incomplete_config)


class TestHttpAlertChannel:
    """Test cases for HttpAlertChannel class."""

    @pytest.fixture
    def http_channel_config(self) -> dict:
        """Provide an HTTP channel configuration for testing."""
        return {
            "url": "https://webhook.example.com/alerts",
            "method": "POST",
            "headers": {"Content-Type": "application/json", "Authorization": "Bearer token123"},
            "timeout": 30,
            "retry": {"error_on_alert_failure": False, "attempts": 2, "delay_in_seconds": 1},
        }

    @pytest.fixture
    def http_channel(self, http_channel_config) -> HttpAlertChannel:
        """Create an HttpAlertChannel instance for testing."""
        return HttpAlertChannel.from_dict(http_channel_config)

    def test_from_dict_creates_channel_correctly(self, http_channel_config) -> None:
        """Test that from_dict creates an HttpAlertChannel correctly."""
        channel = HttpAlertChannel.from_dict(http_channel_config)

        assert channel.url == "https://webhook.example.com/alerts"
        assert channel.method == "POST"
        assert channel.headers == {"Content-Type": "application/json", "Authorization": "Bearer token123"}
        assert channel.timeout == 30
        assert channel.retry.attempts == 2
        assert channel.retry.delay_in_seconds == 1
        assert channel.retry.error_on_alert_failure is False

    def test_from_dict_raises_error_for_missing_keys(self):
        """Test that from_dict raises error when required keys are missing."""
        incomplete_config = {
            "url": "https://webhook.example.com/alerts",
            "method": "POST",
            # Missing headers, timeout, and retry
        }

        with pytest.raises(FlintConfigurationKeyError):
            HttpAlertChannel.from_dict(incomplete_config)

    @patch("flint.alert.channels.http.requests.request")
    def test_alert_sends_successful_request(self, mock_request, http_channel) -> None:
        """Test that _alert sends successful HTTP request."""
        # Configure mock response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response

        # Send alert
        http_channel._alert("Test Alert", "Test message")

        # Verify request was made with correct parameters
        expected_payload = json.dumps({"title": "Test Alert", "message": "Test message"})
        mock_request.assert_called_once_with(
            method="POST",
            url="https://webhook.example.com/alerts",
            headers={"Content-Type": "application/json", "Authorization": "Bearer token123"},
            data=expected_payload,
            timeout=30,
        )
        mock_response.raise_for_status.assert_called_once()

    @patch("flint.alert.channels.http.requests.request")
    def test_alert_with_different_http_methods(self, mock_request) -> None:
        """Test that _alert works with different HTTP methods."""
        # Test with PUT method
        config = {
            "url": "https://api.example.com/alerts",
            "method": "PUT",
            "headers": {},
            "timeout": 10,
            "retry": {"error_on_alert_failure": False, "attempts": 1, "delay_in_seconds": 1},
        }
        channel = HttpAlertChannel.from_dict(config)

        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response

        channel._alert("PUT Alert", "PUT message")

        # Verify PUT method was used
        args, kwargs = mock_request.call_args
        assert kwargs["method"] == "PUT"

    @patch("flint.alert.channels.http.requests.request")
    def test_alert_handles_empty_title_and_body(self, mock_request, http_channel: HttpAlertChannel) -> None:
        """Test that _alert handles empty title and body correctly."""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response

        # Send alert with empty strings
        http_channel._alert("", "")

        # Verify correct payload
        expected_payload = json.dumps({"title": "", "message": ""})
        args, kwargs = mock_request.call_args
        assert kwargs["data"] == expected_payload

    @patch("flint.alert.channels.http.requests.request")
    @patch("flint.alert.channels.http.time.sleep")
    def test_alert_retries_on_failure_then_succeeds(
        self, mock_sleep, mock_request, http_channel: HttpAlertChannel
    ) -> None:
        """Test that _alert retries on failure and eventually succeeds."""
        # First call fails, second succeeds
        mock_response_fail = Mock()
        mock_response_fail.raise_for_status.side_effect = requests.RequestException("Connection error")

        mock_response_success = Mock()
        mock_response_success.raise_for_status.return_value = None

        mock_request.side_effect = [mock_response_fail, mock_response_success]

        # Send alert
        http_channel._alert("Retry Test", "Test message")

        # Verify two attempts were made
        assert mock_request.call_count == 2

        # Verify sleep was called between retries
        mock_sleep.assert_called_once_with(1)  # delay_in_seconds from fixture

    @patch("flint.alert.channels.http.requests.request")
    @patch("flint.alert.channels.http.time.sleep")
    def test_alert_exhausts_retries_and_suppresses_error(self, mock_sleep, mock_request) -> None:
        """Test that _alert exhausts retries and suppresses error when configured."""
        config = {
            "url": "https://webhook.example.com/alerts",
            "method": "POST",
            "headers": {},
            "timeout": 30,
            "retry": {
                "error_on_alert_failure": False,  # Suppress errors
                "attempts": 2,
                "delay_in_seconds": 1,
            },
        }
        channel = HttpAlertChannel.from_dict(config)

        # All requests fail
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.RequestException("Connection error")
        mock_request.return_value = mock_response

        # Should not raise exception
        channel._alert("Fail Test", "Test message")

        # Verify all attempts were made (initial + retries)
        assert mock_request.call_count == 3  # 1 initial + 2 retries

        # Verify sleep was called between retries
        assert mock_sleep.call_count == 2

    @patch("flint.alert.channels.http.requests.request")
    @patch("flint.alert.channels.http.time.sleep")
    def test_alert_exhausts_retries_and_raises_error(self, mock_sleep, mock_request) -> None:
        """Test that _alert exhausts retries and raises error when configured."""
        config = {
            "url": "https://webhook.example.com/alerts",
            "method": "POST",
            "headers": {},
            "timeout": 30,
            "retry": {
                "error_on_alert_failure": True,  # Raise errors
                "attempts": 1,
                "delay_in_seconds": 1,
            },
        }
        channel = HttpAlertChannel.from_dict(config)

        # All requests fail
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.RequestException("Connection error")
        mock_request.return_value = mock_response

        # Should raise exception after retries
        with pytest.raises(requests.RequestException):
            channel._alert("Fail Test", "Test message")

        # Verify all attempts were made
        assert mock_request.call_count == 2  # 1 initial + 1 retry

    @patch("flint.alert.channels.http.requests.request")
    def test_alert_handles_http_status_errors(self, mock_request, http_channel: HttpAlertChannel) -> None:
        """Test that _alert handles HTTP status errors correctly."""
        # Mock response with HTTP error
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        mock_request.return_value = mock_response

        # Should not raise exception (error_on_alert_failure is False in fixture)
        http_channel._alert("Error Test", "Test message")

        # Verify all retries were attempted
        assert mock_request.call_count == 3  # 1 initial + 2 retries

    @patch("flint.alert.channels.http.requests.request")
    def test_alert_handles_timeout_errors(self, mock_request, http_channel: HttpAlertChannel) -> None:
        """Test that _alert handles timeout errors correctly."""
        # Mock timeout error
        mock_request.side_effect = requests.Timeout("Request timeout")

        # Should not raise exception (error_on_alert_failure is False in fixture)
        http_channel._alert("Timeout Test", "Test message")

        # Verify all retries were attempted
        assert mock_request.call_count == 3  # 1 initial + 2 retries

    @patch("flint.alert.channels.http.requests.request")
    def test_alert_with_special_characters_in_payload(self, mock_request, http_channel: HttpAlertChannel) -> None:
        """Test that _alert handles special characters in payload correctly."""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response

        # Send alert with special characters
        title = "Émergence Crítica!"
        body = "Message: àáâãäåæçèé & symbols: @#$%^&*()"
        http_channel._alert(title, body)

        # Verify payload was properly encoded
        expected_payload = json.dumps({"title": title, "message": body})
        args, kwargs = mock_request.call_args
        assert kwargs["data"] == expected_payload

    @patch("flint.alert.channels.http.requests.request")
    def test_alert_with_no_retries_configured(self, mock_request) -> None:
        """Test that _alert works with zero retry attempts."""
        config = {
            "url": "https://webhook.example.com/alerts",
            "method": "POST",
            "headers": {},
            "timeout": 30,
            "retry": {
                "error_on_alert_failure": False,
                "attempts": 0,  # No retries
                "delay_in_seconds": 1,
            },
        }
        channel = HttpAlertChannel.from_dict(config)

        # Request fails
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.RequestException("Connection error")
        mock_request.return_value = mock_response

        # Should only make one attempt
        channel._alert("No Retry Test", "Test message")

        # Verify only one attempt was made
        assert mock_request.call_count == 1

    @patch("flint.alert.channels.http.requests.request")
    def test_alert_with_negative_attempts_does_nothing(self, mock_request) -> None:
        """Test that _alert handles negative retry attempts gracefully."""
        config = {
            "url": "https://webhook.example.com/alerts",
            "method": "POST",
            "headers": {},
            "timeout": 30,
            "retry": {
                "error_on_alert_failure": False,
                "attempts": -1,  # Negative attempts - edge case
                "delay_in_seconds": 1,
            },
        }
        channel = HttpAlertChannel.from_dict(config)

        # Should not make any HTTP requests
        channel._alert("Edge Case Test", "Test message")

        # Verify no requests were made
        assert mock_request.call_count == 0

    def test_alert_method_delegates_to_internal_alert(self, http_channel: HttpAlertChannel) -> None:
        """Test that the public alert method delegates to _alert correctly."""
        with patch.object(http_channel, "_alert") as mock_internal_alert:
            # Use the public alert method
            http_channel.alert("Public Alert", "Public message")

            # Verify delegation
            mock_internal_alert.assert_called_once_with(title="Public Alert", body="Public message")
