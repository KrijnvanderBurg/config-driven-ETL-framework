"""Unit tests for the JobSpark model."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from pydantic import ValidationError

from flint.exceptions import FlintJobError
from flint.runtime.jobs.spark.job import JobSpark

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="job_config")
def fixture_job_config(tmp_path: Path) -> dict[str, Any]:
    """Provide a job configuration dict using temporary files."""
    tmp_in = tmp_path / "input.json"
    tmp_in.write_bytes(b"")
    tmp_out = tmp_path / "output.json"
    tmp_out.write_bytes(b"")

    data = {
        "id": "job_dict",
        "description": "desc",
        "enabled": True,
        "engine_type": "spark",
        "extracts": [
            {
                "id": "ex2",
                "extract_type": "file",
                "method": "batch",
                "data_format": "csv",
                "options": {},
                "location": str(tmp_in),
                "schema": "",
            }
        ],
        "transforms": [{"id": "tr2", "upstream_id": "ex2", "options": {}, "functions": []}],
        "loads": [
            {
                "id": "ld2",
                "upstream_id": "tr2",
                "load_type": "file",
                "method": "batch",
                "location": str(tmp_out),
                "schema_location": "",
                "options": {},
                "mode": "append",
                "data_format": "csv",
            }
        ],
        "hooks": {
            "onStart": [],
            "onError": [],
            "onSuccess": [],
            "onFinally": [],
        },
    }

    return data


# =========================================================================== #
# ========================== VALIDATION TESTS ============================= #
# =========================================================================== #


class TestJobSparkValidation:
    """Test JobSpark model validation."""

    def test_create_job_spark__with_missing_extracts__raises_validation_error(self, job_config: dict[str, Any]) -> None:
        """Test JobSpark creation fails when extracts field is missing."""
        del job_config["extracts"]

        with pytest.raises(ValidationError):
            JobSpark(**job_config)

    def test_create_job_spark__with_missing_transforms__raises_validation_error(
        self, job_config: dict[str, Any]
    ) -> None:
        """Test JobSpark creation fails when transforms field is missing."""
        del job_config["transforms"]

        with pytest.raises(ValidationError):
            JobSpark(**job_config)

    def test_create_job_spark__with_missing_loads__raises_validation_error(self, job_config: dict[str, Any]) -> None:
        """Test JobSpark creation fails when loads field is missing."""
        del job_config["loads"]

        with pytest.raises(ValidationError):
            JobSpark(**job_config)

    def test_create_job_spark__with_invalid_engine__raises_validation_error(self, job_config: dict[str, Any]) -> None:
        """Test JobSpark creation fails with invalid engine value."""
        job_config["engine_type"] = "invalid_engine"

        with pytest.raises(ValidationError):
            JobSpark(**job_config)


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="job_spark")
def fixture_job_spark(job_config: dict[str, Any]) -> JobSpark:
    """Instantiate a JobSpark object from the provided job_config dict."""
    return JobSpark(**job_config)


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #


class TestJobSparkExecute:
    """Test JobSpark execute method."""

    def test_execute__with_empty_extracts_transforms_loads__completes_successfully(self, job_spark: JobSpark) -> None:
        """Test execute completes when all lists are empty."""
        job_spark.extracts = []
        job_spark.transforms = []
        job_spark.loads = []

        job_spark.execute()  # Should not raise

    def test_execute__when_disabled__returns_early(self, job_spark: JobSpark) -> None:
        """Test execute returns early when job is disabled (tests JobBase behavior)."""
        job_spark.enabled = False

        job_spark.execute()  # Should not raise, just return

    def test_execute__with_exception__triggers_on_error_and_wraps_in_flint_job_error(self, job_spark: JobSpark) -> None:
        """Test execute triggers onError hook and wraps exceptions in FlintJobError."""
        mock_error_action = Mock()
        job_spark.hooks.onError = [mock_error_action]
        mock_extract = Mock()
        mock_extract.id = "failing_extract"
        mock_extract.extract.side_effect = ValueError("Extract failed")
        job_spark.extracts = [mock_extract]

        with pytest.raises(FlintJobError):
            job_spark.execute()

        mock_error_action.execute.assert_called_once()

    def test_execute__with_os_error__wraps_in_flint_job_error(self, job_spark: JobSpark) -> None:
        """Test execute wraps OSError (and subclasses like FileNotFoundError, PermissionError) in FlintJobError."""
        mock_load = Mock()
        mock_load.id = "load_io_error"
        mock_load.load.side_effect = OSError("Disk full")
        job_spark.extracts = []
        job_spark.transforms = []
        job_spark.loads = [mock_load]

        with pytest.raises(FlintJobError):
            job_spark.execute()

    def test_execute__with_key_error__wraps_in_flint_job_error(self, job_spark: JobSpark) -> None:
        """Test execute wraps KeyError in FlintJobError."""
        mock_transform = Mock()
        mock_transform.id = "transform_missing_upstream"
        mock_transform.transform.side_effect = KeyError("upstream_name")
        job_spark.extracts = []
        job_spark.transforms = [mock_transform]
        job_spark.loads = []

        with pytest.raises(FlintJobError):
            job_spark.execute()


class TestJobSparkPhases:
    """Test JobSpark ETL phase methods."""

    def test_extract__calls_extract_on_all_extractors(self, job_spark: JobSpark) -> None:
        """Test that extract phase calls extract() on each extractor."""
        mock_extract1 = Mock()
        mock_extract1.id = "extract1"
        mock_extract2 = Mock()
        mock_extract2.id = "extract2"
        job_spark.extracts = [mock_extract1, mock_extract2]
        job_spark.transforms = []  # Empty transforms to avoid failures
        job_spark.loads = []  # Empty loads to avoid failures

        job_spark.execute()

        mock_extract1.extract.assert_called_once()
        mock_extract2.extract.assert_called_once()

    def test_transform__calls_transform_on_all_transformers(self, job_spark: JobSpark) -> None:
        """Test that transform phase calls transform() on each transformer."""
        mock_transform1 = Mock()
        mock_transform1.id = "transform1"
        mock_transform2 = Mock()
        mock_transform2.id = "transform2"
        job_spark.extracts = []  # Empty extracts
        job_spark.transforms = [mock_transform1, mock_transform2]
        job_spark.loads = []  # Empty loads to avoid failures

        job_spark.execute()

        mock_transform1.transform.assert_called_once()
        mock_transform2.transform.assert_called_once()

    def test_load__calls_load_on_all_loaders(self, job_spark: JobSpark) -> None:
        """Test that load phase calls load() on each loader."""
        mock_load1 = Mock()
        mock_load1.id = "load1"
        mock_load2 = Mock()
        mock_load2.id = "load2"
        job_spark.extracts = []  # Empty extracts
        job_spark.transforms = []  # Empty transforms
        job_spark.loads = [mock_load1, mock_load2]

        job_spark.execute()

        mock_load1.load.assert_called_once()
        mock_load2.load.assert_called_once()
