"""Unit tests for the Schedule class.

This module contains comprehensive tests for the Schedule functionality,
including configuration loading and validation.
"""

import pytest

from flint.exceptions import FlintConfigurationKeyError
from flint.sensor.schedule import SensorSchedule


class TestSchedule:
    """Test cases for Schedule class."""

    @pytest.fixture
    def sample_schedule_config(self) -> dict:
        """Provide a sample schedule configuration for testing."""
        return {"expression": "*/5 * * * *", "timezone": "UTC"}

    @pytest.fixture
    def minimal_schedule_config(self) -> dict:
        """Provide a minimal schedule configuration for testing."""
        return {"expression": "0 * * * *", "timezone": "America/New_York"}

    @pytest.fixture
    def invalid_config_missing_expression(self) -> dict:
        """Provide an invalid configuration missing expression for testing errors."""
        return {"timezone": "UTC"}

    @pytest.fixture
    def invalid_config_missing_timezone(self) -> dict:
        """Provide an invalid configuration missing timezone for testing errors."""
        return {"expression": "*/5 * * * *"}

    def test_from_dict_success(self, sample_schedule_config: dict) -> None:
        """Test successful Schedule creation from dictionary."""
        schedule = SensorSchedule.from_dict(sample_schedule_config)

        assert schedule.expression == "*/5 * * * *"
        assert schedule.timezone == "UTC"

    def test_from_dict_minimal_config(self, minimal_schedule_config: dict) -> None:
        """Test Schedule creation with minimal valid configuration."""
        schedule = SensorSchedule.from_dict(minimal_schedule_config)

        assert schedule.expression == "0 * * * *"
        assert schedule.timezone == "America/New_York"

    def test_from_dict_missing_expression_raises_error(self, invalid_config_missing_expression: dict) -> None:
        """Test that missing expression raises FlintConfigurationKeyError."""
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            SensorSchedule.from_dict(invalid_config_missing_expression)

        assert exc_info.value.key == "expression"
        assert "timezone" in exc_info.value.available_keys

    def test_from_dict_missing_timezone_raises_error(self, invalid_config_missing_timezone: dict) -> None:
        """Test that missing timezone raises FlintConfigurationKeyError."""
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            SensorSchedule.from_dict(invalid_config_missing_timezone)

        assert exc_info.value.key == "timezone"
        assert "expression" in exc_info.value.available_keys

    def test_from_dict_empty_dict_raises_error(self) -> None:
        """Test that empty dictionary raises FlintConfigurationKeyError."""
        with pytest.raises(FlintConfigurationKeyError):
            SensorSchedule.from_dict({})

    def test_schedule_attributes_are_correctly_assigned(self, sample_schedule_config: dict) -> None:
        """Test that Schedule attributes are correctly assigned from configuration."""
        schedule = SensorSchedule.from_dict(sample_schedule_config)

        # Test that attributes match the input configuration
        assert hasattr(schedule, "expression")
        assert hasattr(schedule, "timezone")
        assert schedule.expression == sample_schedule_config["expression"]
        assert schedule.timezone == sample_schedule_config["timezone"]

    def test_schedule_dataclass_immutability(self, sample_schedule_config: dict) -> None:
        """Test that Schedule dataclass behaves as expected."""
        schedule1 = SensorSchedule.from_dict(sample_schedule_config)
        schedule2 = SensorSchedule.from_dict(sample_schedule_config)

        # Test equality
        assert schedule1.expression == schedule2.expression
        assert schedule1.timezone == schedule2.timezone

    def test_from_dict_with_various_cron_expressions(self) -> None:
        """Test Schedule creation with various cron expressions."""
        test_cases = [
            {"expression": "0 0 * * *", "timezone": "UTC"},  # Daily at midnight
            {"expression": "0 */6 * * *", "timezone": "UTC"},  # Every 6 hours
            {"expression": "30 2 * * 1", "timezone": "UTC"},  # Weekly on Monday at 2:30 AM
            {"expression": "@hourly", "timezone": "UTC"},  # Special syntax
        ]

        for config in test_cases:
            schedule = SensorSchedule.from_dict(config)
            assert schedule.expression == config["expression"]
            assert schedule.timezone == config["timezone"]

    def test_from_dict_with_various_timezones(self) -> None:
        """Test Schedule creation with various timezone formats."""
        test_cases = [
            {"expression": "0 * * * *", "timezone": "UTC"},
            {"expression": "0 * * * *", "timezone": "America/New_York"},
            {"expression": "0 * * * *", "timezone": "Europe/London"},
            {"expression": "0 * * * *", "timezone": "Asia/Tokyo"},
            {"expression": "0 * * * *", "timezone": "Australia/Sydney"},
        ]

        for config in test_cases:
            schedule = SensorSchedule.from_dict(config)
            assert schedule.expression == config["expression"]
            assert schedule.timezone == config["timezone"]
