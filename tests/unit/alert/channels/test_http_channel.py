"""Tests for HttpChannel model.

These tests verify that HttpChannel (and the nested Retry model) instances can
be correctly created from configuration and function as expected.
"""

from typing import Any

import pytest

from flint.alert.channels.http import HttpChannel, Retry

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="http_config")
def fixture_http_config() -> dict[str, Any]:
    """Provide a representative http channel configuration."""
    return {
        "name": "http1",
        "description": "an http channel",
        "url": "https://example.com/webhook",
        "method": "POST",
        "headers": {"Content-Type": "application/json"},
        "timeout": 5,
        "retry": {"raise_on_error": False, "max_attempts": 0, "delay_in_seconds": 0},
    }


def test_http_channel_creation__from_config__creates_valid_model(http_config: dict[str, Any]) -> None:
    """Test specifically for the creation process itself."""
    # Act
    ch = HttpChannel(**http_config)

    # Assert
    assert ch.channel_id == "http"
    assert ch.name == "http1"
    assert ch.description == "an http channel"
    assert str(ch.url) == "https://example.com/webhook"
    assert ch.method == "POST"
    assert ch.headers == {"Content-Type": "application/json"}
    assert ch.timeout == 5
    assert isinstance(ch.retry, Retry)
    # Inspect retry fields explicitly
    assert ch.retry.raise_on_error is False
    assert ch.retry.max_attempts == 0
    assert ch.retry.delay_in_seconds == 0


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="http_channel")
def fixture_http_channel(http_config: dict[str, Any]) -> HttpChannel:
    """Create HttpChannel instance from configuration."""
    return HttpChannel(**http_config)


def test_http_channel_properties__after_creation__match_expected_values(http_channel: HttpChannel) -> None:
    """Test model properties using the instantiated object fixture.

    Use hard-coded expected values to avoid comparing fixtures directly.
    """
    assert http_channel.name == "http1"
    assert http_channel.method == "POST"
    assert isinstance(http_channel.retry, Retry)
    # Additional attributes asserted on the fixture
    assert http_channel.channel_id == "http"
    assert http_channel.description == "an http channel"
    assert str(http_channel.url) == "https://example.com/webhook"
    assert http_channel.headers == {"Content-Type": "application/json"}
    assert http_channel.timeout == 5
    assert http_channel.retry.raise_on_error is False
    assert http_channel.retry.max_attempts == 0
    assert http_channel.retry.delay_in_seconds == 0


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #
