"""Unit tests for the HttpChannel class."""

from unittest.mock import patch

from flint.alert.channels.http import HttpChannel
from flint.utils.http import Retry


class TestHttpChannel:
    """Test cases for HttpChannel."""

    def test_alert__calls_alert_and_make_http_request(self) -> None:
        """Test that alert calls _alert which then calls _make_http_request."""
        # Arrange
        retry = Retry(raise_on_error=True, max_attempts=2, delay_in_seconds=1)
        channel = HttpChannel.model_construct(
            id_="test_http_channel",
            description="Test HTTP channel",
            channel_type="http",
            url="https://example.com/webhook",
            method="POST",
            timeout=30,
            retry=retry,
            enabled=True,
        )

        with patch.object(channel, "_make_http_request") as mock_make_http_request:
            # Act
            channel.alert(title="Test Alert", body="This is a test alert message")

            # Assert
            mock_make_http_request.assert_called_once_with(
                {"title": "Test Alert", "message": "This is a test alert message"}
            )

    def test_alert__with_empty_title_and_body__calls_make_http_request_with_empty_strings(self) -> None:
        """Test that alert calls _make_http_request with empty strings when title and body are empty."""
        # Arrange
        retry = Retry(raise_on_error=False, max_attempts=0, delay_in_seconds=1)
        channel = HttpChannel.model_construct(
            id_="test_http_channel",
            description="Test HTTP channel",
            channel_type="http",
            url="https://example.com/webhook",
            method="POST",
            timeout=30,
            retry=retry,
            enabled=True,
        )

        with patch.object(channel, "_make_http_request") as mock_make_http_request:
            # Act
            channel.alert(title="", body="")

            # Assert
            mock_make_http_request.assert_called_once_with({"title": "", "message": ""})
