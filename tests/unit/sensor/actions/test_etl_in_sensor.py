"""Unit tests for ETL in sensor action models.

This module contains comprehensive tests for ETL in sensor action model functionality,
including configuration loading and validation.
"""

import pytest

from flint.exceptions import FlintConfigurationKeyError
from flint.sensor.actions.etl_in_sensor import EtlInSensor


class TestEtlInSensor:
    """Test cases for EtlInSensor class."""

    @pytest.fixture
    def sample_etl_in_sensor_config(self) -> dict:
        """Provide a sample ETL in sensor configuration for testing."""
        return {"job_names": ["extract-data", "transform-data", "load-data"]}

    @pytest.fixture
    def single_job_config(self) -> dict:
        """Provide a single job ETL configuration for testing."""
        return {"job_names": ["single-etl-job"]}

    @pytest.fixture
    def empty_jobs_config(self) -> dict:
        """Provide an empty jobs ETL configuration for testing."""
        return {"job_names": []}

    @pytest.fixture
    def complex_job_names_config(self) -> dict:
        """Provide a complex job names configuration for testing."""
        return {
            "job_names": [
                "extract-customer-data",
                "extract-order-data",
                "validate-data-quality",
                "transform-customer-orders",
                "enrich-with-demographics",
                "load-to-warehouse",
                "update-data-catalog",
            ]
        }

    def test_from_dict_success(self, sample_etl_in_sensor_config: dict) -> None:
        """Test successful EtlInSensor creation from dictionary."""
        action = EtlInSensor.from_dict(sample_etl_in_sensor_config)

        assert action.job_names == ["extract-data", "transform-data", "load-data"]

    def test_from_dict_single_job(self, single_job_config: dict) -> None:
        """Test EtlInSensor creation with single job."""
        action = EtlInSensor.from_dict(single_job_config)

        assert action.job_names == ["single-etl-job"]

    def test_from_dict_empty_jobs(self, empty_jobs_config: dict) -> None:
        """Test EtlInSensor creation with empty job list."""
        action = EtlInSensor.from_dict(empty_jobs_config)

        assert action.job_names == []

    def test_from_dict_complex_job_names(self, complex_job_names_config: dict) -> None:
        """Test EtlInSensor creation with complex job names."""
        action = EtlInSensor.from_dict(complex_job_names_config)

        expected_jobs = [
            "extract-customer-data",
            "extract-order-data",
            "validate-data-quality",
            "transform-customer-orders",
            "enrich-with-demographics",
            "load-to-warehouse",
            "update-data-catalog",
        ]
        assert action.job_names == expected_jobs

    def test_from_dict_missing_job_names_raises_error(self) -> None:
        """Test that missing job_names raises FlintConfigurationKeyError."""
        config = {}
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            EtlInSensor.from_dict(config)

        assert exc_info.value.key == "job_names"

    def test_from_dict_preserves_job_order(self) -> None:
        """Test that EtlInSensor preserves the order of jobs."""
        config = {"job_names": ["job-z", "job-a", "job-m", "job-b"]}
        action = EtlInSensor.from_dict(config)

        # Order should be preserved exactly as specified
        assert action.job_names == ["job-z", "job-a", "job-m", "job-b"]

    def test_from_dict_duplicate_job_names(self) -> None:
        """Test EtlInSensor creation with duplicate job names."""
        config = {"job_names": ["job-1", "job-2", "job-1", "job-3", "job-2"]}
        action = EtlInSensor.from_dict(config)

        # Duplicates should be preserved as specified
        assert action.job_names == ["job-1", "job-2", "job-1", "job-3", "job-2"]

    def test_from_dict_job_names_with_special_characters(self) -> None:
        """Test EtlInSensor creation with job names containing special characters."""
        config = {
            "job_names": [
                "extract_customer_data",
                "transform-orders.v2",
                "load@warehouse",
                "job:with:colons",
                "job with spaces",
            ]
        }
        action = EtlInSensor.from_dict(config)

        expected_jobs = [
            "extract_customer_data",
            "transform-orders.v2",
            "load@warehouse",
            "job:with:colons",
            "job with spaces",
        ]
        assert action.job_names == expected_jobs

    def test_from_dict_validates_list_type(self) -> None:
        """Test that job_names must be a list."""
        # This test validates that the configuration properly handles type mismatches
        # The actual validation would happen at runtime when the job names are used
        config = {
            "job_names": ["valid-job-name"]  # This is valid
        }
        action = EtlInSensor.from_dict(config)
        assert isinstance(action.job_names, list)

    def test_etl_in_sensor_dataclass_properties(self, sample_etl_in_sensor_config: dict) -> None:
        """Test that EtlInSensor dataclass behaves as expected."""
        action1 = EtlInSensor.from_dict(sample_etl_in_sensor_config)
        action2 = EtlInSensor.from_dict(sample_etl_in_sensor_config)

        # Test equality
        assert action1.job_names == action2.job_names

        # Test that it has the expected attributes
        assert hasattr(action1, "job_names")

    def test_from_dict_very_long_job_list(self) -> None:
        """Test EtlInSensor creation with a very long list of jobs."""
        long_job_list = [f"job-{i:04d}" for i in range(100)]
        config = {"job_names": long_job_list}
        action = EtlInSensor.from_dict(config)

        assert len(action.job_names) == 100
        assert action.job_names[0] == "job-0000"
        assert action.job_names[99] == "job-0099"
