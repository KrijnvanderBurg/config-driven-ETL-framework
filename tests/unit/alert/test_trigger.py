"""Tests for AlertTrigger model.

These tests verify that AlertTrigger instances can be correctly created from
configuration and function as expected.
"""

from typing import Any

import pytest

from flint.alert.trigger import AlertTrigger

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="trigger_config")
def fixture_trigger_config() -> dict[str, Any]:
    """Provide a representative alert trigger configuration."""
    return {
        "name": "test_trigger",
        "enabled": True,
        "description": "A test trigger",
        "channel_names": [],
        "template": {"prepend_title": "", "append_title": "", "prepend_body": "", "append_body": ""},
        "rules": [],
    }


def test_trigger_creation__from_config__creates_valid_model(trigger_config: dict[str, Any]) -> None:
    """Test specifically for the creation process itself."""
    trigger = AlertTrigger(**trigger_config)

    assert trigger.name == "test_trigger"
    assert trigger.enabled is True
    assert trigger.description == "A test trigger"
    assert trigger.channel_names == []
    # Template fields
    assert trigger.template.prepend_title == ""
    assert trigger.template.append_title == ""
    assert trigger.template.prepend_body == ""
    assert trigger.template.append_body == ""
    assert trigger.rules == []


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="trigger_instance")
def fixture_trigger_instance(trigger_config: dict[str, Any]) -> AlertTrigger:
    """Create AlertTrigger instance from configuration."""
    return AlertTrigger(**trigger_config)


def test_trigger_properties__after_creation__match_expected_values(trigger_instance: AlertTrigger) -> None:
    """Test model properties using the instantiated object fixture.

    Use hard-coded expected values to avoid comparing fixtures directly.
    """
    # enabled and no rules -> should fire
    assert trigger_instance.should_fire(Exception("boom")) is True
    # Template remains accessible on the instance
    assert trigger_instance.template.prepend_title == ""
    assert trigger_instance.template.append_title == ""


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #
