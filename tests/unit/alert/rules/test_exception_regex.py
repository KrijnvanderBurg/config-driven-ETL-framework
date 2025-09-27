"""Tests for ExceptionRegexRule model.

These tests verify that ExceptionRegexRule instances can be correctly created
from configuration and function as expected.
"""

from typing import Any

import pytest

from flint.alert.rules.exception_regex import ExceptionRegexRule

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="exception_rule_config")
def fixture_exception_rule_config() -> dict[str, Any]:
    """Provide a representative exception regex rule configuration."""
    return {"rule": "exception_regex", "pattern": "error"}


def test_exception_regex_rule_creation__from_config__creates_valid_model(exception_rule_config: dict[str, Any]) -> None:
    """Test specifically for the creation process itself."""
    # Act
    rule = ExceptionRegexRule(**exception_rule_config)

    # Assert
    assert rule.rule == "exception_regex"
    assert rule.pattern == "error"


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="exception_rule")
def fixture_exception_rule(exception_rule_config: dict[str, Any]) -> ExceptionRegexRule:
    """Create ExceptionRegexRule instance from configuration."""
    return ExceptionRegexRule(**exception_rule_config)


def test_exception_regex_rule_properties__after_creation__match_expected_values(
    exception_rule: ExceptionRegexRule,
) -> None:
    """Test model properties using the instantiated object fixture.

    Use hard-coded expected values to avoid comparing fixtures directly.
    """
    assert exception_rule.rule == "exception_regex"
    assert exception_rule.pattern == "error"


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #
