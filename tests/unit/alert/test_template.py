"""Tests for AlertTemplate model.

These tests verify AlertTemplate creation, validation, message formatting,
and error handling scenarios.
"""

from typing import Any

import pytest
from pydantic import ValidationError

from flint.alert.template import AlertTemplate

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="valid_template_config")
def fixture_valid_template_config() -> dict[str, Any]:
    """Provide a valid alert template configuration."""
    return {
        "prepend_title": "[ALERT]",
        "append_title": "[/ALERT]",
        "prepend_body": ">>> ",
        "append_body": " <<<",
    }


# =========================================================================== #
# ========================== VALIDATION TESTS ============================= #
# =========================================================================== #


class TestAlertTemplateValidation:
    """Test AlertTemplate model validation and instantiation."""

    def test_create_alert_template__with_valid_config__succeeds(self, valid_template_config: dict[str, Any]) -> None:
        """Test AlertTemplate creation with valid configuration."""
        # Act
        template = AlertTemplate(**valid_template_config)

        # Assert
        assert template.prepend_title == "[ALERT]"
        assert template.append_title == "[/ALERT]"
        assert template.prepend_body == ">>> "
        assert template.append_body == " <<<"

    def test_create_alert_template__with_missing_prepend_title__raises_validation_error(
        self, valid_template_config: dict[str, Any]
    ) -> None:
        """Test AlertTemplate creation fails when prepend_title is missing."""
        # Arrange
        del valid_template_config["prepend_title"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            AlertTemplate(**valid_template_config)

    def test_create_alert_template__with_missing_append_title__raises_validation_error(
        self, valid_template_config: dict[str, Any]
    ) -> None:
        """Test AlertTemplate creation fails when append_title is missing."""
        # Arrange
        del valid_template_config["append_title"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            AlertTemplate(**valid_template_config)

    def test_create_alert_template__with_missing_prepend_body__raises_validation_error(
        self, valid_template_config: dict[str, Any]
    ) -> None:
        """Test AlertTemplate creation fails when prepend_body is missing."""
        # Arrange
        del valid_template_config["prepend_body"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            AlertTemplate(**valid_template_config)

    def test_create_alert_template__with_missing_append_body__raises_validation_error(
        self, valid_template_config: dict[str, Any]
    ) -> None:
        """Test AlertTemplate creation fails when append_body is missing."""
        # Arrange
        del valid_template_config["append_body"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            AlertTemplate(**valid_template_config)

    def test_create_alert_template__with_empty_strings__succeeds(self, valid_template_config: dict[str, Any]) -> None:
        """Test AlertTemplate creation with empty string values."""
        # Arrange
        valid_template_config.update(
            {
                "prepend_title": "",
                "append_title": "",
                "prepend_body": "",
                "append_body": "",
            }
        )

        # Act
        template = AlertTemplate(**valid_template_config)

        # Assert
        assert template.prepend_title == ""
        assert template.append_title == ""
        assert template.prepend_body == ""
        assert template.append_body == ""

    def test_create_alert_template__with_none_values__raises_validation_error(
        self, valid_template_config: dict[str, Any]
    ) -> None:
        """Test AlertTemplate creation fails when values are None."""
        # Arrange
        valid_template_config["prepend_title"] = None

        # Assert
        with pytest.raises(ValidationError):
            # Act
            AlertTemplate(**valid_template_config)

    def test_create_alert_template__with_non_string_values__raises_validation_error(
        self, valid_template_config: dict[str, Any]
    ) -> None:
        """Test AlertTemplate creation fails when values are not strings."""
        # Arrange
        valid_template_config["prepend_title"] = 123

        # Assert
        with pytest.raises(ValidationError):
            # Act
            AlertTemplate(**valid_template_config)

    def test_create_alert_template__with_special_characters__succeeds(
        self, valid_template_config: dict[str, Any]
    ) -> None:
        """Test AlertTemplate creation with special characters and unicode."""
        # Arrange
        valid_template_config.update(
            {
                "prepend_title": "🚨 [ÅLØRT] ",
                "append_title": " [/ÅLØRT] 🚨",
                "prepend_body": "⚠️ Üñíçødé: ",
                "append_body": " ⚠️",
            }
        )

        # Act
        template = AlertTemplate(**valid_template_config)

        # Assert
        assert template.prepend_title == "🚨 [ÅLØRT] "
        assert template.append_title == " [/ÅLØRT] 🚨"
        assert template.prepend_body == "⚠️ Üñíçødé: "
        assert template.append_body == " ⚠️"

    def test_create_alert_template__with_multiline_strings__succeeds(
        self, valid_template_config: dict[str, Any]
    ) -> None:
        """Test AlertTemplate creation with multiline string values."""
        # Arrange
        valid_template_config.update(
            {
                "prepend_title": "Line 1\nLine 2\n",
                "append_title": "\nEnd Line 1\nEnd Line 2",
                "prepend_body": "Body Start\n",
                "append_body": "\nBody End",
            }
        )

        # Act
        template = AlertTemplate(**valid_template_config)

        # Assert
        assert template.prepend_title == "Line 1\nLine 2\n"
        assert template.append_title == "\nEnd Line 1\nEnd Line 2"
        assert template.prepend_body == "Body Start\n"
        assert template.append_body == "\nBody End"


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="alert_template")
def fixture_alert_template(valid_template_config: dict[str, Any]) -> AlertTemplate:
    """Create AlertTemplate instance from valid configuration."""
    return AlertTemplate(**valid_template_config)


# =========================================================================== #
# ========================== FORMATTING TESTS ============================== #
# =========================================================================== #


class TestAlertTemplateFormatting:
    """Test AlertTemplate formatting functionality."""

    def test_format_title__with_regular_text__applies_templates_correctly(self, alert_template: AlertTemplate) -> None:
        """Test title formatting with regular text."""
        # Act
        result = alert_template.format_title("System Error")

        # Assert
        assert result == "[ALERT]System Error[/ALERT]"

    def test_format_body__with_regular_text__applies_templates_correctly(self, alert_template: AlertTemplate) -> None:
        """Test body formatting with regular text."""
        # Act
        result = alert_template.format_body("Database connection failed")

        # Assert
        assert result == ">>> Database connection failed <<<"

    def test_format_title__with_empty_string__applies_templates_to_empty_string(
        self, alert_template: AlertTemplate
    ) -> None:
        """Test title formatting with empty string."""
        # Act
        result = alert_template.format_title("")

        # Assert
        assert result == "[ALERT][/ALERT]"

    def test_format_body__with_empty_string__applies_templates_to_empty_string(
        self, alert_template: AlertTemplate
    ) -> None:
        """Test body formatting with empty string."""
        # Act
        result = alert_template.format_body("")

        # Assert
        assert result == ">>>  <<<"

    def test_format_title__with_special_characters__preserves_special_characters(
        self, alert_template: AlertTemplate
    ) -> None:
        """Test title formatting with special characters and unicode."""
        # Act
        result = alert_template.format_title("Ålørt: Üñíçødé Tëst 🔥")

        # Assert
        assert result == "[ALERT]Ålørt: Üñíçødé Tëst 🔥[/ALERT]"

    def test_format_body__with_special_characters__preserves_special_characters(
        self, alert_template: AlertTemplate
    ) -> None:
        """Test body formatting with special characters and unicode."""
        # Act
        result = alert_template.format_body("Body with émojis 🔥 and spëcial chars: áéíóú")

        # Assert
        assert result == ">>> Body with émojis 🔥 and spëcial chars: áéíóú <<<"

    def test_format_title__with_multiline_text__preserves_newlines(self, alert_template: AlertTemplate) -> None:
        """Test title formatting with multiline text preserves structure."""
        # Act
        result = alert_template.format_title("Multi\nLine\nTitle")

        # Assert
        assert result == "[ALERT]Multi\nLine\nTitle[/ALERT]"

    def test_format_body__with_multiline_text__preserves_newlines(self, alert_template: AlertTemplate) -> None:
        """Test body formatting with multiline text preserves structure."""
        # Act
        result = alert_template.format_body("Multi\nLine\nBody")

        # Assert
        assert result == ">>> Multi\nLine\nBody <<<"

    def test_format_title__with_whitespace_text__preserves_whitespace(self, alert_template: AlertTemplate) -> None:
        """Test title formatting preserves leading and trailing whitespace."""
        # Act
        result = alert_template.format_title("  Spaced Title  ")

        # Assert
        assert result == "[ALERT]  Spaced Title  [/ALERT]"

    def test_format_body__with_whitespace_text__preserves_whitespace(self, alert_template: AlertTemplate) -> None:
        """Test body formatting preserves leading and trailing whitespace."""
        # Act
        result = alert_template.format_body("  Spaced Body  ")

        # Assert
        assert result == ">>>   Spaced Body   <<<"

    def test_format_title__with_template_characters_in_text__does_not_interfere(
        self, alert_template: AlertTemplate
    ) -> None:
        """Test title formatting when input text contains template characters."""
        # Act
        result = alert_template.format_title("Title with [ALERT] in it")

        # Assert
        assert result == "[ALERT]Title with [ALERT] in it[/ALERT]"

    def test_format_body__with_template_characters_in_text__does_not_interfere(
        self, alert_template: AlertTemplate
    ) -> None:
        """Test body formatting when input text contains template characters."""
        # Act
        result = alert_template.format_body("Body with >>> markers <<<")

        # Assert
        assert result == ">>> Body with >>> markers <<< <<<"


# =========================================================================== #
# ======================== EDGE CASE TESTS ================================= #
# =========================================================================== #


class TestAlertTemplateEdgeCases:
    """Test AlertTemplate edge cases and boundary conditions."""

    def test_format_title__with_very_long_text__handles_correctly(self, alert_template: AlertTemplate) -> None:
        """Test title formatting with very long text."""
        # Arrange
        long_title = "A" * 10000

        # Act
        result = alert_template.format_title(long_title)

        # Assert
        assert result.startswith("[ALERT]A")
        assert result.endswith("A[/ALERT]")
        assert len(result) == 10000 + len("[ALERT]") + len("[/ALERT]")

    def test_format_body__with_very_long_text__handles_correctly(self, alert_template: AlertTemplate) -> None:
        """Test body formatting with very long text."""
        # Arrange
        long_body = "B" * 10000

        # Act
        result = alert_template.format_body(long_body)

        # Assert
        assert result.startswith(">>> B")
        assert result.endswith("B <<<")
        assert len(result) == 10000 + len(">>> ") + len(" <<<")

    def test_format_methods__with_empty_templates__return_original_text(self) -> None:
        """Test formatting with empty templates returns original text unchanged."""
        # Arrange
        empty_template = AlertTemplate(
            prepend_title="",
            append_title="",
            prepend_body="",
            append_body="",
        )

        # Act
        title_result = empty_template.format_title("Original Title")
        body_result = empty_template.format_body("Original Body")

        # Assert
        assert title_result == "Original Title"
        assert body_result == "Original Body"

    def test_format_methods__with_complex_templates__applies_all_components(self) -> None:
        """Test formatting with complex multi-character templates."""
        # Arrange
        complex_template = AlertTemplate(
            prepend_title="🚨⚠️[CRITICAL ALERT]⚠️🚨 ",
            append_title=" 🚨⚠️[/CRITICAL ALERT]⚠️🚨",
            prepend_body="┌─ ALERT MESSAGE START ─┐\n",
            append_body="\n└─ ALERT MESSAGE END ─┘",
        )

        # Act
        title_result = complex_template.format_title("Database Down")
        body_result = complex_template.format_body("Connection timeout")

        # Assert
        expected_title = "🚨⚠️[CRITICAL ALERT]⚠️🚨 Database Down 🚨⚠️[/CRITICAL ALERT]⚠️🚨"
        expected_body = "┌─ ALERT MESSAGE START ─┐\nConnection timeout\n└─ ALERT MESSAGE END ─┘"
        assert title_result == expected_title
        assert body_result == expected_body
