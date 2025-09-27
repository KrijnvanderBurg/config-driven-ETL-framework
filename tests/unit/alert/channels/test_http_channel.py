"""Tests for HttpChannel alert functionality.

These tests verify HttpChannel creation, validation, alert sending,
retry logic, and error handling scenarios.
"""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import requests
from pydantic import ValidationError

from flint.alert.channels.http import HttpChannel, Retry

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="valid_http_config")
def fixture_valid_http_config() -> dict[str, Any]:
    """Provide a valid HTTP channel configuration."""
    return {
        "name": "webhook-alerts",
        "description": "Production webhook notifications",
        "url": "https://hooks.slack.com/services/webhook",
        "method": "POST",
        "headers": {"Content-Type": "application/json", "Authorization": "Bearer token123"},
        "timeout": 30,
        "retry": {"raise_on_error": True, "max_attempts": 2, "delay_in_seconds": 1},
    }


@pytest.fixture(name="valid_retry_config")
def fixture_valid_retry_config() -> dict[str, Any]:
    """Provide a valid retry configuration."""
    return {"raise_on_error": False, "max_attempts": 3, "delay_in_seconds": 2}


# =========================================================================== #
# ========================== RETRY MODEL TESTS ============================= #
# =========================================================================== #


class TestRetryValidation:
    """Test Retry model validation and instantiation."""

    def test_create_retry__with_valid_config__succeeds(self, valid_retry_config: dict[str, Any]) -> None:
        """Test Retry creation with valid configuration."""
        # Act
        retry = Retry(**valid_retry_config)

        # Assert
        assert retry.raise_on_error is False
        assert retry.max_attempts == 3
        assert retry.delay_in_seconds == 2

    def test_create_retry__with_max_attempts_too_high__raises_validation_error(
        self, valid_retry_config: dict[str, Any]
    ) -> None:
        """Test Retry creation fails when max_attempts exceeds limit."""
        # Arrange
        valid_retry_config["max_attempts"] = 4

        # Assert
        with pytest.raises(ValidationError):
            # Act
            Retry(**valid_retry_config)

    def test_create_retry__with_negative_max_attempts__raises_validation_error(
        self, valid_retry_config: dict[str, Any]
    ) -> None:
        """Test Retry creation fails with negative max_attempts."""
        # Arrange
        valid_retry_config["max_attempts"] = -1

        # Assert
        with pytest.raises(ValidationError):
            # Act
            Retry(**valid_retry_config)

    def test_create_retry__with_negative_delay__raises_validation_error(
        self, valid_retry_config: dict[str, Any]
    ) -> None:
        """Test Retry creation fails with negative delay."""
        # Arrange
        valid_retry_config["delay_in_seconds"] = -1

        # Assert
        with pytest.raises(ValidationError):
            # Act
            Retry(**valid_retry_config)

    def test_create_retry__with_missing_required_field__raises_validation_error(
        self, valid_retry_config: dict[str, Any]
    ) -> None:
        """Test Retry creation fails when required field is missing."""
        # Arrange
        del valid_retry_config["raise_on_error"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            Retry(**valid_retry_config)


# =========================================================================== #
# ===================== HTTP CHANNEL VALIDATION TESTS ======================= #
# =========================================================================== #


class TestHttpChannelValidation:
    """Test HttpChannel model validation and instantiation."""

    def test_create_http_channel__with_valid_config__succeeds(self, valid_http_config: dict[str, Any]) -> None:
        """Test HttpChannel creation with valid configuration."""
        # Act
        channel = HttpChannel(**valid_http_config)

        # Assert
        assert channel.name == "webhook-alerts"
        assert channel.description == "Production webhook notifications"
        assert channel.channel_id == "http"
        assert str(channel.url) == "https://hooks.slack.com/services/webhook"
        assert channel.method == "POST"
        assert channel.headers == {
            "Content-Type": "application/json",
            "Authorization": "Bearer token123",
        }
        assert channel.timeout == 30
        assert isinstance(channel.retry, Retry)
        assert channel.retry.raise_on_error is True
        assert channel.retry.max_attempts == 2
        assert channel.retry.delay_in_seconds == 1

    def test_create_http_channel__with_missing_name__raises_validation_error(
        self, valid_http_config: dict[str, Any]
    ) -> None:
        """Test HttpChannel creation fails when name is missing."""
        # Arrange
        del valid_http_config["name"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            HttpChannel(**valid_http_config)

    def test_create_http_channel__with_empty_name__raises_validation_error(
        self, valid_http_config: dict[str, Any]
    ) -> None:
        """Test HttpChannel creation fails when name is empty string."""
        # Arrange
        valid_http_config["name"] = ""

        # Assert
        with pytest.raises(ValidationError):
            # Act
            HttpChannel(**valid_http_config)

    def test_create_http_channel__with_invalid_url__raises_validation_error(
        self, valid_http_config: dict[str, Any]
    ) -> None:
        """Test HttpChannel creation fails with invalid URL."""
        # Arrange
        valid_http_config["url"] = "not-a-valid-url"

        # Assert
        with pytest.raises(ValidationError):
            # Act
            HttpChannel(**valid_http_config)

    def test_create_http_channel__with_invalid_method__raises_validation_error(
        self, valid_http_config: dict[str, Any]
    ) -> None:
        """Test HttpChannel creation fails with unsupported HTTP method."""
        # Arrange
        valid_http_config["method"] = "DELETE"

        # Assert
        with pytest.raises(ValidationError):
            # Act
            HttpChannel(**valid_http_config)

    def test_create_http_channel__with_zero_timeout__raises_validation_error(
        self, valid_http_config: dict[str, Any]
    ) -> None:
        """Test HttpChannel creation fails with zero timeout."""
        # Arrange
        valid_http_config["timeout"] = 0

        # Assert
        with pytest.raises(ValidationError):
            # Act
            HttpChannel(**valid_http_config)

    def test_create_http_channel__with_negative_timeout__raises_validation_error(
        self, valid_http_config: dict[str, Any]
    ) -> None:
        """Test HttpChannel creation fails with negative timeout."""
        # Arrange
        valid_http_config["timeout"] = -5

        # Assert
        with pytest.raises(ValidationError):
            # Act
            HttpChannel(**valid_http_config)

    def test_create_http_channel__with_default_headers__uses_empty_dict(
        self, valid_http_config: dict[str, Any]
    ) -> None:
        """Test HttpChannel creation uses empty dict when headers not provided."""
        # Arrange
        del valid_http_config["headers"]

        # Act
        channel = HttpChannel(**valid_http_config)

        # Assert
        assert channel.headers == {}

    def test_create_http_channel__with_default_description__uses_empty_string(
        self, valid_http_config: dict[str, Any]
    ) -> None:
        """Test HttpChannel creation uses empty description when not provided."""
        # Arrange
        del valid_http_config["description"]

        # Act
        channel = HttpChannel(**valid_http_config)

        # Assert
        assert channel.description == ""


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="http_channel")
def fixture_http_channel(valid_http_config: dict[str, Any]) -> HttpChannel:
    """Create HttpChannel instance from valid configuration."""
    return HttpChannel(**valid_http_config)


# =========================================================================== #
# ============================ ALERT TESTS ================================ #
# =========================================================================== #


class TestHttpChannelAlert:
    """Test HttpChannel alert functionality."""

    def test_alert__with_valid_inputs__sends_http_request_successfully(self, http_channel: HttpChannel) -> None:
        """Test successful HTTP alert sending with proper request parameters."""
        # Mock the requests library because we don't want to make real HTTP calls
        with patch("flint.alert.channels.http.requests.request") as mock_request:
            mock_response = MagicMock()
            mock_response.raise_for_status.return_value = None
            mock_request.return_value = mock_response

            # Act
            http_channel.alert("Critical System Alert", "Database connection failed")

            # Assert HTTP request made with correct parameters
            mock_request.assert_called_once_with(
                method="POST",
                url="https://hooks.slack.com/services/webhook",
                headers={"Content-Type": "application/json", "Authorization": "Bearer token123"},
                data='{"title": "Critical System Alert", "message": "Database connection failed"}',
                timeout=30,
            )
            mock_response.raise_for_status.assert_called_once()

    def test_alert__with_get_method__uses_get_request(self, valid_http_config: dict[str, Any]) -> None:
        """Test alert uses GET method when configured."""
        # Arrange
        valid_http_config["method"] = "GET"
        channel = HttpChannel(**valid_http_config)

        # Mock the requests library because we don't want to make real HTTP calls
        with patch("flint.alert.channels.http.requests.request") as mock_request:
            mock_response = MagicMock()
            mock_response.raise_for_status.return_value = None
            mock_request.return_value = mock_response

            # Act
            channel.alert("Test Alert", "Test message")

            # Assert
            mock_request.assert_called_once()
            _, kwargs = mock_request.call_args
            assert kwargs["method"] == "GET"

    def test_alert__with_empty_headers__sends_without_headers(self, valid_http_config: dict[str, Any]) -> None:
        """Test alert works with empty headers dictionary."""
        # Arrange
        valid_http_config["headers"] = {}
        channel = HttpChannel(**valid_http_config)

        # Mock the requests library because we don't want to make real HTTP calls
        with patch("flint.alert.channels.http.requests.request") as mock_request:
            mock_response = MagicMock()
            mock_response.raise_for_status.return_value = None
            mock_request.return_value = mock_response

            # Act
            channel.alert("Test Alert", "Test message")

            # Assert
            mock_request.assert_called_once()
            _, kwargs = mock_request.call_args
            assert kwargs["headers"] == {}

    def test_alert__with_special_characters__handles_encoding_correctly(self, http_channel: HttpChannel) -> None:
        """Test alert handling with special characters in title and body."""
        # Mock the requests library because we don't want to make real HTTP calls
        with patch("flint.alert.channels.http.requests.request") as mock_request:
            mock_response = MagicMock()
            mock_response.raise_for_status.return_value = None
            mock_request.return_value = mock_response

            # Act
            http_channel.alert("Ålørt: Üñíçødé Tëst", "Body with émojis 🔥 and spëcial chars: áéíóú")

            # Assert
            mock_request.assert_called_once()
            _, kwargs = mock_request.call_args
            # JSON dumps escapes non-ASCII characters by default
            expected_json = (
                '{"title": "\\u00c5l\\u00f8rt: \\u00dc\\u00f1\\u00ed\\u00e7\\u00f8d\\u00e9 T\\u00ebst", '
                '"message": "Body with \\u00e9mojis \\ud83d\\udd25 and sp\\u00ebcial chars: '
                '\\u00e1\\u00e9\\u00ed\\u00f3\\u00fa"}'
            )
            assert kwargs["data"] == expected_json

    def test_alert__with_empty_title__sends_with_empty_title_field(self, http_channel: HttpChannel) -> None:
        """Test alert sending with empty title results in empty title field."""
        # Mock the requests library because we don't want to make real HTTP calls
        with patch("flint.alert.channels.http.requests.request") as mock_request:
            mock_response = MagicMock()
            mock_response.raise_for_status.return_value = None
            mock_request.return_value = mock_response

            # Act
            http_channel.alert("", "Important alert body")

            # Assert
            mock_request.assert_called_once()
            _, kwargs = mock_request.call_args
            expected_json = '{"title": "", "message": "Important alert body"}'
            assert kwargs["data"] == expected_json

    def test_alert__with_empty_body__sends_with_empty_message_field(self, http_channel: HttpChannel) -> None:
        """Test alert sending with empty body results in empty message field."""
        # Mock the requests library because we don't want to make real HTTP calls
        with patch("flint.alert.channels.http.requests.request") as mock_request:
            mock_response = MagicMock()
            mock_response.raise_for_status.return_value = None
            mock_request.return_value = mock_response

            # Act
            http_channel.alert("Alert Title", "")

            # Assert
            mock_request.assert_called_once()
            _, kwargs = mock_request.call_args
            expected_json = '{"title": "Alert Title", "message": ""}'
            assert kwargs["data"] == expected_json


# =========================================================================== #
# ============================ RETRY TESTS ================================= #
# =========================================================================== #


class TestHttpChannelRetry:
    """Test HttpChannel retry logic and error handling."""

    def test_alert__with_retry_success_on_second_attempt__retries_and_succeeds(
        self, valid_http_config: dict[str, Any]
    ) -> None:
        """Test alert succeeds on retry after initial failure."""
        # Arrange
        valid_http_config["retry"] = {"raise_on_error": True, "max_attempts": 2, "delay_in_seconds": 0}
        channel = HttpChannel(**valid_http_config)

        # Mock the requests library to fail first, succeed second
        with (
            patch("flint.alert.channels.http.requests.request") as mock_request,
            patch("flint.alert.channels.http.time.sleep") as mock_sleep,
        ):
            # First call fails, second succeeds
            mock_response_success = MagicMock()
            mock_response_success.raise_for_status.return_value = None
            mock_request.side_effect = [
                requests.ConnectionError("Network error"),
                mock_response_success,
            ]

            # Act
            channel.alert("Test Alert", "Test message")

            # Assert
            assert mock_request.call_count == 2
            mock_sleep.assert_called_once_with(0)

    def test_alert__with_all_retries_exhausted_and_raise_on_error__raises_exception(
        self, valid_http_config: dict[str, Any]
    ) -> None:
        """Test alert raises exception when all retries are exhausted and raise_on_error is True."""
        # Arrange
        valid_http_config["retry"] = {"raise_on_error": True, "max_attempts": 1, "delay_in_seconds": 0}
        channel = HttpChannel(**valid_http_config)

        # Mock the requests library to always fail
        with (
            patch("flint.alert.channels.http.requests.request") as mock_request,
            patch("flint.alert.channels.http.time.sleep"),
        ):
            mock_request.side_effect = requests.ConnectionError("Persistent network error")

            # Assert
            with pytest.raises(requests.ConnectionError):
                # Act
                channel.alert("Test Alert", "Test message")

            # Verify total attempts (initial + retries)
            assert mock_request.call_count == 2

    def test_alert__with_all_retries_exhausted_and_no_raise_on_error__returns_silently(
        self, valid_http_config: dict[str, Any]
    ) -> None:
        """Test alert returns silently when all retries exhausted and raise_on_error is False."""
        # Arrange
        valid_http_config["retry"] = {"raise_on_error": False, "max_attempts": 1, "delay_in_seconds": 0}
        channel = HttpChannel(**valid_http_config)

        # Mock the requests library to always fail
        with (
            patch("flint.alert.channels.http.requests.request") as mock_request,
            patch("flint.alert.channels.http.time.sleep"),
        ):
            mock_request.side_effect = requests.ConnectionError("Persistent network error")

            # Act - should not raise exception
            channel.alert("Test Alert", "Test message")

            # Assert
            assert mock_request.call_count == 2

    def test_alert__with_no_retries_and_failure__fails_immediately(self, valid_http_config: dict[str, Any]) -> None:
        """Test alert fails immediately when max_attempts is 0."""
        # Arrange
        valid_http_config["retry"] = {"raise_on_error": True, "max_attempts": 0, "delay_in_seconds": 0}
        channel = HttpChannel(**valid_http_config)

        # Mock the requests library to fail
        with patch("flint.alert.channels.http.requests.request") as mock_request:
            mock_request.side_effect = requests.ConnectionError("Network error")

            # Assert
            with pytest.raises(requests.ConnectionError):
                # Act
                channel.alert("Test Alert", "Test message")

            # Verify only one attempt made
            assert mock_request.call_count == 1

    def test_alert__with_no_retries_and_no_raise__returns_silently(self, valid_http_config: dict[str, Any]) -> None:
        """Test alert returns silently when max_attempts is 0 and raise_on_error is False."""
        # Arrange
        valid_http_config["retry"] = {"raise_on_error": False, "max_attempts": 0, "delay_in_seconds": 0}
        channel = HttpChannel(**valid_http_config)

        # Mock the requests library to fail
        with patch("flint.alert.channels.http.requests.request") as mock_request:
            mock_request.side_effect = requests.ConnectionError("Network error")

            # Act - should not raise despite failure
            channel.alert("Test Alert", "Test message")

            # Verify only one attempt was made and no exception propagated
            assert mock_request.call_count == 1

    def test_alert__with_http_status_error__retries_appropriately(self, valid_http_config: dict[str, Any]) -> None:
        """Test alert retries on HTTP status errors."""
        # Arrange
        valid_http_config["retry"] = {"raise_on_error": True, "max_attempts": 1, "delay_in_seconds": 0}
        channel = HttpChannel(**valid_http_config)

        # Mock the requests library to return 500 error
        with (
            patch("flint.alert.channels.http.requests.request") as mock_request,
            patch("flint.alert.channels.http.time.sleep"),
        ):
            mock_response = MagicMock()
            mock_response.raise_for_status.side_effect = requests.HTTPError("500 Server Error")
            mock_request.return_value = mock_response

            # Assert
            with pytest.raises(requests.HTTPError):
                # Act
                channel.alert("Test Alert", "Test message")

            # Verify total attempts (initial + retries)
            assert mock_request.call_count == 2

    def test_alert__with_timeout_error__retries_appropriately(self, valid_http_config: dict[str, Any]) -> None:
        """Test alert retries on timeout errors."""
        # Arrange
        valid_http_config["retry"] = {"raise_on_error": True, "max_attempts": 2, "delay_in_seconds": 1}
        channel = HttpChannel(**valid_http_config)

        # Mock the requests library to timeout
        with (
            patch("flint.alert.channels.http.requests.request") as mock_request,
            patch("flint.alert.channels.http.time.sleep") as mock_sleep,
        ):
            mock_request.side_effect = requests.Timeout("Request timeout")

            # Assert
            with pytest.raises(requests.Timeout):
                # Act
                channel.alert("Test Alert", "Test message")

            # Verify total attempts and sleep calls
            assert mock_request.call_count == 3  # initial + 2 retries
            assert mock_sleep.call_count == 2  # sleep between retries
            mock_sleep.assert_called_with(1)

    def test_alert__with_delay_between_retries__sleeps_correct_duration(
        self, valid_http_config: dict[str, Any]
    ) -> None:
        """Test alert waits correct duration between retry attempts."""
        # Arrange
        valid_http_config["retry"] = {"raise_on_error": False, "max_attempts": 2, "delay_in_seconds": 5}
        channel = HttpChannel(**valid_http_config)

        # Mock the requests library to always fail
        with (
            patch("flint.alert.channels.http.requests.request") as mock_request,
            patch("flint.alert.channels.http.time.sleep") as mock_sleep,
        ):
            mock_request.side_effect = requests.ConnectionError("Network error")

            # Act
            channel.alert("Test Alert", "Test message")

            # Assert correct sleep duration called between retries
            assert mock_sleep.call_count == 2
            mock_sleep.assert_called_with(5)

    def test_alert__with_no_retries_and_success__succeeds_immediately(self, valid_http_config: dict[str, Any]) -> None:
        """Test alert succeeds immediately when max_attempts is 0 and request succeeds."""
        # Arrange
        valid_http_config["retry"] = {"raise_on_error": True, "max_attempts": 0, "delay_in_seconds": 0}
        channel = HttpChannel(**valid_http_config)

        # Mock the requests library to succeed
        with patch("flint.alert.channels.http.requests.request") as mock_request:
            mock_response = MagicMock()
            mock_response.raise_for_status.return_value = None
            mock_request.return_value = mock_response

            # Act
            channel.alert("Test Alert", "Test message")

            # Assert exactly one attempt made
            assert mock_request.call_count == 1
            mock_response.raise_for_status.assert_called_once()

    def test_alert__with_maximum_retries_and_all_fail__exhausts_all_attempts(
        self, valid_http_config: dict[str, Any]
    ) -> None:
        """Test alert exhausts all attempts when max_attempts is at maximum (3) and all fail."""
        # Arrange - Use maximum allowed retries
        valid_http_config["retry"] = {"raise_on_error": False, "max_attempts": 3, "delay_in_seconds": 0}
        channel = HttpChannel(**valid_http_config)

        # Mock the requests library to always fail
        with (
            patch("flint.alert.channels.http.requests.request") as mock_request,
            patch("flint.alert.channels.http.time.sleep"),
        ):
            mock_request.side_effect = requests.ConnectionError("Network error")

            # Act - should not raise exception due to raise_on_error=False
            channel.alert("Test Alert", "Test message")

            # Assert all attempts made (initial + 3 retries = 4 total)
            assert mock_request.call_count == 4

    def test_alert__with_monkeypatched_negative_max_attempts__does_nothing(self, http_channel: HttpChannel) -> None:
        """Test that when retry.max_attempts is monkeypatched to -1 (zero iterations)
        the alert method exits without making any HTTP requests.

        This exercises the for-loop exit path where range(self.retry.max_attempts + 1)
        becomes empty and the body is never executed (coverage "->exit").
        """
        # Arrange - force max_attempts to -1 so range(-1 + 1) == range(0)
        http_channel.retry.max_attempts = -1

        # Monitor requests.request to ensure it's never called
        with patch("flint.alert.channels.http.requests.request") as mock_request:
            # Act - should quietly return without calling requests
            http_channel.alert("No-op Alert", "This should not send any HTTP requests")

            # Assert
            mock_request.assert_not_called()
