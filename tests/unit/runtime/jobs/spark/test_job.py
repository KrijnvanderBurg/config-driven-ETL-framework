"""Unit tests for the JobSpark model."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from flint.runtime.jobs.models.model_job import JobEngine
from flint.runtime.jobs.spark.job import JobSpark

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="job_config")
def fixture_job_config(tmp_path: Path) -> Generator[dict[str, Any], Any, None]:
    """Provide a job configuration dict using temporary files for paths under tmp_path."""
    tmp_in = Path(tmp_path) / "input.json"
    tmp_in.write_bytes(b"")

    tmp_out = Path(tmp_path) / "output.json"
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
    }

    yield data


def test_job_creation__from_config__creates_valid_model(job_config: dict) -> None:
    """Create a JobSpark from the config and assert its top-level attributes."""
    job = JobSpark(**job_config)
    # Assert fields from config in the same order as in example job.jsonc
    # name, description, enabled, engine, extracts, transforms, loads
    assert job.name == "job_dict"
    assert job.description == "desc"
    assert job.enabled is True
    assert job.engine == JobEngine.SPARK
    assert isinstance(job.extracts, list)
    assert isinstance(job.transforms, list)
    assert isinstance(job.loads, list)


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="job_spark")
def fixture_job_spark(job_config: dict[str, Any]) -> JobSpark:
    """Instantiate a JobSpark object from the provided job_config dict.

    Returns the concrete JobSpark used by object-based tests.
    """
    return JobSpark(**job_config)


def test_job_fixture__has_extracts_and_loads(job_spark: JobSpark) -> None:
    """Validate properties on the JobSpark object fixture in expected order."""
    # Validate object fixture properties in same order as the config
    assert job_spark.name == "job_dict"
    assert job_spark.description == "desc"
    assert job_spark.enabled is True
    assert job_spark.engine == JobEngine.SPARK

    # Collections
    assert len(job_spark.extracts) == 1
    assert len(job_spark.transforms) == 1
    assert len(job_spark.loads) == 1

    # Check inner extract/load names to ensure parsing preserved config
    assert job_spark.extracts[0].name == "ex2"
    assert job_spark.loads[0].name == "ld2"


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #
