"""Tests for AlertTemplate model.

These tests verify that AlertTemplate instances can be correctly created from
configuration and function as expected.
"""

from typing import Any

import pytest

from flint.alert.template import AlertTemplate

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="template_config")
def fixture_template_config() -> dict[str, Any]:
    """Provide a representative alert template configuration."""
    return {
        "prepend_title": "[PRE]",
        "append_title": "[POST]",
        "prepend_body": "<",
        "append_body": ">",
    }


def test_template_creation__from_config__creates_valid_model(template_config: dict[str, Any]) -> None:
    """Test specifically for the creation process itself."""
    template = AlertTemplate(**template_config)

    assert template.prepend_title == "[PRE]"
    assert template.append_title == "[POST]"
    assert template.prepend_body == "<"
    assert template.append_body == ">"


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="template_instance")
def fixture_template_instance(template_config: dict[str, Any]) -> AlertTemplate:
    """Create AlertTemplate instance from configuration."""
    return AlertTemplate(**template_config)


def test_template_properties__after_creation__match_expected_values(template_instance: AlertTemplate) -> None:
    """Test model properties using the instantiated object fixture.

    Use hard-coded expected values to avoid comparing fixtures directly.
    """
    assert template_instance.format_title("X") == "[PRE]X[POST]"
    assert template_instance.format_body("m") == "<m>"


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #
