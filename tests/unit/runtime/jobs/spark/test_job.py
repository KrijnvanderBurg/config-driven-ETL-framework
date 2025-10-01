"""Unit tests for the JobSpark model."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest
from pydantic import ValidationError

from flint.runtime.jobs.models.model_job import JobEngine
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
        "name": "job_dict",
        "description": "desc",
        "enabled": True,
        "engine": "spark",
        "extracts": [
            {
                "name": "ex2",
                "method": "batch",
                "data_format": "csv",
                "options": {},
                "location": str(tmp_in),
                "schema_": "",
            }
        ],
        "transforms": [{"name": "tr2", "upstream_name": "ex2", "options": {}, "functions": []}],
        "loads": [
            {
                "name": "ld2",
                "upstream_name": "tr2",
                "method": "batch",
                "location": str(tmp_out),
                "schema_location": None,
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


def test_job_creation__from_config__creates_valid_model(job_config: dict) -> None:
    """Create a JobSpark from the config and assert its top-level attributes."""
    job = JobSpark(**job_config)

    assert job.name == "job_dict"
    assert job.description == "desc"
    assert job.enabled is True
    assert job.engine == JobEngine.SPARK


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


class TestJobSparkValidation:
    """Test JobSpark model validation."""

    def test_create_job_spark__with_missing_name__raises_validation_error(self, job_config: dict[str, Any]) -> None:
        """Test JobSpark creation fails when name is missing."""
        del job_config["name"]

        with pytest.raises(ValidationError):
            JobSpark(**job_config)

    def test_create_job_spark__with_invalid_engine__raises_validation_error(self, job_config: dict[str, Any]) -> None:
        """Test JobSpark creation fails with invalid engine."""
        job_config["engine"] = "invalid_engine"

        with pytest.raises(ValidationError):
            JobSpark(**job_config)


class TestJobSparkExecute:
    """Test JobSpark execute method."""

    def test_execute__calls_extract_transform_load_in_sequence(self, job_spark: JobSpark) -> None:
        """Test execute calls _extract, _transform, and _load in order."""
        with (
            patch.object(job_spark, "_extract") as mock_extract,
            patch.object(job_spark, "_transform") as mock_transform,
            patch.object(job_spark, "_load") as mock_load,
        ):
            job_spark.execute()

            mock_extract.assert_called_once()
            mock_transform.assert_called_once()
            mock_load.assert_called_once()

    def test_execute__when_extract_fails__propagates_exception(self, job_spark: JobSpark) -> None:
        """Test execute propagates exception when _extract fails."""
        with patch.object(job_spark, "_extract", side_effect=Exception("Extract failed")):
            with pytest.raises(Exception, match="Extract failed"):
                job_spark.execute()


class TestJobSparkPhases:
    """Test JobSpark ETL phase methods."""

    def test_extract__calls_extract_on_all_extractors(self, job_spark: JobSpark) -> None:
        """Test _extract calls extract() on each extractor."""
        mock_extract = Mock()
        mock_extract.name = "extract1"
        job_spark.extracts = [mock_extract]

        job_spark._extract()  # noqa: SLF001

        mock_extract.extract.assert_called_once()

    def test_transform__calls_transform_on_all_transformers(self, job_spark: JobSpark) -> None:
        """Test _transform calls transform() on each transformer."""
        mock_transform = Mock()
        mock_transform.name = "transform1"
        job_spark.transforms = [mock_transform]

        job_spark._transform()  # noqa: SLF001

        mock_transform.transform.assert_called_once()

    def test_load__calls_load_on_all_loaders(self, job_spark: JobSpark) -> None:
        """Test _load calls load() on each loader."""
        mock_load = Mock()
        mock_load.name = "load1"
        job_spark.loads = [mock_load]

        job_spark._load()  # noqa: SLF001

        mock_load.load.assert_called_once()
