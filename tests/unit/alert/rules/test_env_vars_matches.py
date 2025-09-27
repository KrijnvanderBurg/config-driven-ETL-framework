"""Tests for EnvVarsMatchesRule model.

These tests verify that EnvVarsMatchesRule instances can be correctly created
from configuration and function as expected.
"""

from typing import Any

import pytest

from flint.alert.rules.env_vars_matches import EnvVarsMatchesRule

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="env_rule_config")
def fixture_env_rule_config() -> dict[str, Any]:
    """Provide a representative environment variable matching rule configuration."""
    return {
        "rule": "env_vars_matches",
        "env_var_name": "TEST_ENV",
        "env_var_values": ["1", "true"],
    }


def test_env_vars_rule_creation__from_config__creates_valid_model(env_rule_config: dict[str, Any]) -> None:
    """Test specifically for the creation process itself."""
    # Act
    rule = EnvVarsMatchesRule(**env_rule_config)

    # Assert
    assert rule.rule == "env_vars_matches"
    assert rule.env_var_name == "TEST_ENV"
    assert rule.env_var_values == ["1", "true"]


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="env_rule")
def fixture_env_rule(env_rule_config: dict[str, Any]) -> EnvVarsMatchesRule:
    """Create EnvVarsMatchesRule instance from configuration."""
    return EnvVarsMatchesRule(**env_rule_config)


def test_env_vars_rule_properties__after_creation__match_expected_values(env_rule: EnvVarsMatchesRule) -> None:
    """Test model properties using the instantiated object fixture.

    Use hard-coded expected values to avoid comparing fixtures directly.
    """
    assert env_rule.rule == "env_vars_matches"
    assert env_rule.env_var_name == "TEST_ENV"
    assert env_rule.env_var_values == ["1", "true"]


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #
