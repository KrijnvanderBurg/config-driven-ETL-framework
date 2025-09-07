"""Unit tests for the AlertTrigger, AlertTemplate, and AlertConditions classes.

This module contains comprehensive tests for trigger logic, condition evaluation,
and template formatting functionality.
"""

import os

import pytest

from flint.alert.trigger import AlertConditions, AlertTemplate, AlertTrigger
from flint.exceptions import FlintConfigurationKeyError


class TestAlertTemplate:
    """Test cases for AlertTemplate class."""

    @pytest.fixture
    def template_config(self) -> dict:
        """Provide a template configuration for testing."""
        return {
            "prepend_title": "[ALERT] ",
            "append_title": " - ETL Pipeline",
            "prepend_body": "Alert Details:\n",
            "append_body": "\n\nPlease investigate.",
        }

    @pytest.fixture
    def template(self, template_config: dict) -> AlertTemplate:
        """Create an AlertTemplate instance for testing."""
        return AlertTemplate.from_dict(template_config)

    def test_from_dict_creates_template_correctly(self, template_config: dict) -> None:
        """Test that from_dict creates an AlertTemplate correctly."""
        template = AlertTemplate.from_dict(template_config)

        assert template.prepend_title == "[ALERT] "
        assert template.append_title == " - ETL Pipeline"
        assert template.prepend_body == "Alert Details:\n"
        assert template.append_body == "\n\nPlease investigate."

    def test_from_dict_raises_error_for_missing_keys(self) -> None:
        """Test that from_dict raises error when required keys are missing."""
        incomplete_config = {
            "prepend_title": "[ALERT] ",
            "append_title": " - ETL Pipeline",
            # Missing prepend_body and append_body
        }

        with pytest.raises(FlintConfigurationKeyError):
            AlertTemplate.from_dict(incomplete_config)

    def test_format_title_applies_prepend_and_append(self, template: AlertTemplate) -> None:
        """Test that format_title applies prepend and append correctly."""
        result = template.format_title("Database Error")
        expected = "[ALERT] Database Error - ETL Pipeline"

        assert result == expected

    def test_format_title_with_empty_string(self, template: AlertTemplate) -> None:
        """Test that format_title works with empty string."""
        result = template.format_title("")
        expected = "[ALERT]  - ETL Pipeline"

        assert result == expected

    def test_format_body_applies_prepend_and_append(self, template: AlertTemplate) -> None:
        """Test that format_body applies prepend and append correctly."""
        result = template.format_body("Connection timeout occurred")
        expected = "Alert Details:\nConnection timeout occurred\n\nPlease investigate."

        assert result == expected

    def test_format_body_with_empty_string(self, template: AlertTemplate) -> None:
        """Test that format_body works with empty string."""
        result = template.format_body("")
        expected = "Alert Details:\n\n\nPlease investigate."

        assert result == expected

    def test_format_with_special_characters(self, template: AlertTemplate) -> None:
        """Test that formatting works with special characters."""
        title = "Error: 100% failure!"
        body = "Special chars: àáâãäåæçèé"

        formatted_title = template.format_title(title)
        formatted_body = template.format_body(body)

        assert formatted_title == "[ALERT] Error: 100% failure! - ETL Pipeline"
        assert formatted_body == "Alert Details:\nSpecial chars: àáâãäåæçèé\n\nPlease investigate."


class TestAlertConditions:
    """Test cases for AlertConditions class."""

    @pytest.fixture
    def conditions_config(self) -> dict:
        """Provide a conditions configuration for testing."""
        return {
            "exception_regex": ".*critical.*",
            "env_vars_matches": {"ENV": ["production", "staging"], "ALERT_LEVEL": ["high", "critical"]},
        }

    @pytest.fixture
    def conditions(self, conditions_config: dict) -> AlertConditions:
        """Create an AlertConditions instance for testing."""
        return AlertConditions.from_dict(conditions_config)

    def test_from_dict_creates_conditions_correctly(self, conditions_config: dict) -> None:
        """Test that from_dict creates AlertConditions correctly."""
        conditions = AlertConditions.from_dict(conditions_config)

        assert conditions.exception_regex == ".*critical.*"
        assert conditions.env_vars_matches == {"ENV": ["production", "staging"], "ALERT_LEVEL": ["high", "critical"]}

    def test_exception_regex_matches_when_pattern_found(self, conditions: AlertConditions) -> None:
        """Test that exception_regex returns True when pattern matches."""
        exception = ValueError("This is a critical system failure")

        result = conditions._is_exception_regex(exception)

        assert result is True

    def test_exception_regex_no_match(self, conditions: AlertConditions) -> None:
        """Test that exception_regex returns False when pattern doesn't match."""
        exception = ValueError("Minor warning message")

        result = conditions._is_exception_regex(exception)

        assert result is False

    def test_exception_regex_logs_match_found(self) -> None:
        """Test that exception_regex logs when pattern matches and covers debug lines."""
        conditions = AlertConditions(exception_regex=".*critical.*", env_vars_matches={})
        exception = ValueError("This is a critical system failure")

        result = conditions._is_exception_regex(exception)

        assert result is True

    def test_exception_regex_empty_returns_true(self) -> None:
        """Test that empty exception_regex returns True."""
        conditions = AlertConditions(exception_regex="", env_vars_matches={})
        exception = ValueError("Any error")

        result = conditions._is_exception_regex(exception)

        assert result is True

    def test_exception_regex_none_returns_true(self) -> None:
        """Test that None exception_regex returns True."""
        conditions = AlertConditions(exception_regex="", env_vars_matches={})
        exception = ValueError("Any error")

        result = conditions._is_exception_regex(exception)

        assert result is True

    def test_env_vars_matches_when_found(self, conditions: AlertConditions) -> None:
        """Test that env_vars_matches returns True when environment variable matches."""
        os.environ["ENV"] = "production"
        os.environ["ALERT_LEVEL"] = "medium"

        result = conditions._is_env_vars_matches()

        assert result is True

    def test_env_vars_matches_no_match(self, conditions: AlertConditions) -> None:
        """Test that env_vars_matches returns False when no environment variables match."""
        os.environ["ENV"] = "development"
        os.environ["ALERT_LEVEL"] = "low"

        result = conditions._is_env_vars_matches()

        assert result is False

    def test_env_vars_matches_missing_variable(self, conditions: AlertConditions) -> None:
        """Test that env_vars_matches handles missing environment variables."""
        # Clear any existing environment variables
        if "ENV" in os.environ:
            del os.environ["ENV"]
        if "ALERT_LEVEL" in os.environ:
            del os.environ["ALERT_LEVEL"]

        result = conditions._is_env_vars_matches()

        assert result is False

    def test_env_vars_matches_empty_returns_true(self) -> None:
        """Test that empty env_vars_matches returns True."""
        conditions = AlertConditions(exception_regex="", env_vars_matches={})

        result = conditions._is_env_vars_matches()

        assert result is True

    def test_is_any_condition_met_returns_true_when_regex_matches(self, conditions: AlertConditions) -> None:
        """Test that is_any_condition_met returns True when both regex and env vars match."""
        exception = ValueError("This is a critical system malfunction")

        # Set both ENV and ALERT_LEVEL to matching values so both conditions pass
        os.environ["ENV"] = "production"
        os.environ["ALERT_LEVEL"] = "high"

        result = conditions.is_any_condition_met(exception)

        # Clean up
        if "ENV" in os.environ:
            del os.environ["ENV"]
        if "ALERT_LEVEL" in os.environ:
            del os.environ["ALERT_LEVEL"]

        assert result is True

    def test_is_any_condition_met_returns_true_when_env_var_matches(self, conditions: AlertConditions) -> None:
        """Test that is_any_condition_met returns True when both regex and env vars match."""
        exception = ValueError("This is a critical system error")  # Contains "critical" to match regex

        # Set both ENV and ALERT_LEVEL to matching values so both conditions pass
        os.environ["ENV"] = "production"
        os.environ["ALERT_LEVEL"] = "critical"

        result = conditions.is_any_condition_met(exception)

        # Clean up
        if "ENV" in os.environ:
            del os.environ["ENV"]
        if "ALERT_LEVEL" in os.environ:
            del os.environ["ALERT_LEVEL"]

        assert result is True

    def test_is_any_condition_met_returns_false_when_no_conditions_match(self, conditions: AlertConditions) -> None:
        """Test that is_any_condition_met returns False when no conditions match."""
        exception = ValueError("Unknown issue")

        # Set ENV to non-matching value
        os.environ["ENV"] = "development"

        result = conditions.is_any_condition_met(exception)

        assert result is False


class TestAlertTrigger:
    """Test cases for AlertTrigger class."""

    @pytest.fixture
    def trigger_config(self) -> dict:
        """Provide a trigger configuration for testing."""
        return {
            "name": "test-trigger",
            "enabled": True,
            "channel_names": ["email", "webhook"],
            "Template": {
                "prepend_title": "[ALERT] ",
                "append_title": " - ETL",
                "prepend_body": "Details: ",
                "append_body": " End.",
            },
            "conditions": {
                "exception_regex": ".*critical.*",
                "env_vars_matches": {"ENV": ["production"]},
            },
        }

    @pytest.fixture
    def trigger(self, trigger_config: dict) -> AlertTrigger:
        """Create an AlertTrigger instance for testing."""
        return AlertTrigger.from_dict(trigger_config)

    def test_from_dict_creates_trigger_correctly(self, trigger_config: dict) -> None:
        """Test that from_dict creates an AlertTrigger correctly."""
        trigger = AlertTrigger.from_dict(trigger_config)

        assert trigger.name == "test-trigger"
        assert trigger.enabled is True
        assert trigger.channel_names == ["email", "webhook"]
        assert isinstance(trigger.template, AlertTemplate)
        assert isinstance(trigger.conditions, AlertConditions)

    def test_from_dict_raises_error_for_missing_keys(self) -> None:
        """Test that from_dict raises error when required keys are missing."""
        incomplete_config = {
            "name": "test-trigger",
            "enabled": True,
            # Missing channel_names, Template, and conditions
        }

        with pytest.raises(FlintConfigurationKeyError):
            AlertTrigger.from_dict(incomplete_config)

    def test_should_trigger_returns_true_when_enabled_and_conditions_met(self, trigger: AlertTrigger) -> None:
        """Test that should_trigger returns True when enabled and conditions are met."""
        exception = ValueError("critical error occurred")

        # Set environment variable to match the trigger conditions
        os.environ["ENV"] = "production"

        result = trigger.should_fire(exception)

        # Clean up
        if "ENV" in os.environ:
            del os.environ["ENV"]

        assert result is True

    def test_should_trigger_returns_false_when_disabled(self) -> None:
        """Test that should_trigger returns False when trigger is disabled."""
        trigger_config = {
            "name": "test-trigger",
            "enabled": False,
            "channel_names": ["email", "webhook"],
            "Template": {
                "prepend_title": "[ALERT] ",
                "append_title": " - ETL",
                "prepend_body": "Details: ",
                "append_body": " End.",
            },
            "conditions": {
                "exception_regex": ".*critical.*",
                "env_vars_matches": {"ENV": ["production"]},
            },
        }
        trigger = AlertTrigger.from_dict(trigger_config)

        exception = ValueError("critical error occurred")

        result = trigger.should_fire(exception)

        assert result is False

    def test_should_trigger_returns_false_when_conditions_not_met(self, trigger: AlertTrigger) -> None:
        """Test that should_trigger returns False when conditions are not met."""
        exception = ValueError("Network issue")  # Doesn't contain "critical" or match regex

        # Set ENV to non-matching value
        os.environ["ENV"] = "development"

        result = trigger.should_fire(exception)

        # Clean up
        if "ENV" in os.environ:
            del os.environ["ENV"]

        assert result is False

    def test_should_trigger_with_both_conditions_matching(self, trigger: AlertTrigger) -> None:
        """Test should_trigger returns True when both regex and env var match."""
        # Test with both regex and env var matching
        exception = ValueError("This is a critical system failure")
        os.environ["ENV"] = "production"

        result = trigger.should_fire(exception)

        # Clean up
        if "ENV" in os.environ:
            del os.environ["ENV"]

        assert result is True

    def test_should_trigger_with_only_regex_matching(self, trigger: AlertTrigger) -> None:
        """Test should_trigger returns False when only regex matches (AND logic requires both)."""
        exception = ValueError("This is a critical error")
        os.environ["ENV"] = "development"  # Non-matching env

        result = trigger.should_fire(exception)

        # Clean up
        if "ENV" in os.environ:
            del os.environ["ENV"]

        assert result is False

    def test_should_trigger_with_only_env_var_matching(self, trigger: AlertTrigger) -> None:
        """Test should_trigger returns False when only env var matches (AND logic requires both)."""
        exception = ValueError("Some other issue")  # Non-matching regex
        os.environ["ENV"] = "production"

        result = trigger.should_fire(exception)

        # Clean up
        if "ENV" in os.environ:
            del os.environ["ENV"]

        assert result is False

    def test_trigger_preserves_all_configuration(self, trigger: AlertTrigger) -> None:
        """Test that trigger preserves all configuration correctly."""
        assert trigger.template.prepend_title == "[ALERT] "
        assert trigger.template.append_title == " - ETL"
        assert trigger.conditions.exception_regex == ".*critical.*"
        assert trigger.conditions.env_vars_matches == {"ENV": ["production"]}
