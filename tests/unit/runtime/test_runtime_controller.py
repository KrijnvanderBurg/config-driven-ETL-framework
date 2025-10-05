"""Unit tests for the RuntimeController module."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from pydantic import ValidationError

from flint.exceptions import FlintIOError, FlintRuntimeConfigurationError
from flint.runtime.controller import RuntimeController

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="runtime_config")
def fixture_runtime_config(tmp_path: Path) -> dict[str, Any]:
    """Provide a valid runtime configuration dict with temporary paths.

    Args:
        tmp_path: pytest temporary directory fixture.

    Returns:
        dict: A valid runtime configuration dictionary.
    """
    # Create temporary files for extract/load locations
    input_file = tmp_path / "input.json"
    input_file.write_text("[]", encoding="utf-8")

    output_file = tmp_path / "output.json"
    output_file.write_text("[]", encoding="utf-8")

    return {
        "id": "test-runtime",
        "description": "Test runtime configuration",
        "enabled": True,
        "jobs": [
            {
                "id": "test_job",
                "description": "Test job description",
                "enabled": True,
                "engine_type": "spark",
                "extracts": [
                    {
                        "id": "extract1",
                        "extract_type": "file",
                        "method": "batch",
                        "data_format": "json",
                        "options": {},
                        "location": str(input_file),
                        "schema_": "",
                    }
                ],
                "transforms": [
                    {
                        "id": "transform1",
                        "upstream_id": "extract1",
                        "options": {},
                        "functions": [],
                    }
                ],
                "loads": [
                    {
                        "id": "load1",
                        "upstream_id": "transform1",
                        "load_type": "file",
                        "method": "batch",
                        "location": str(output_file),
                        "schema_location": None,
                        "options": {},
                        "mode": "overwrite",
                        "data_format": "json",
                    }
                ],
                "hooks": {
                    "onStart": [],
                    "onError": [],
                    "onSuccess": [],
                    "onFinally": [],
                },
            }
        ],
    }


def test_runtime_creation__from_config__creates_valid_model(runtime_config: dict[str, Any]) -> None:
    """Create a RuntimeController from config and assert its attributes."""
    controller = RuntimeController(**runtime_config)

    assert isinstance(controller.jobs, list)
    assert len(controller.jobs) == 1
    assert controller.jobs[0].id == "test_job"


# =========================================================================== #
# ========================== VALIDATION TESTS ============================= #
# =========================================================================== #


class TestRuntimeControllerValidation:
    """Test RuntimeController model validation."""

    def test_create_runtime_controller__with_missing_jobs__raises_validation_error(
        self, runtime_config: dict[str, Any]
    ) -> None:
        """Test RuntimeController creation fails when jobs field is missing."""
        del runtime_config["jobs"]

        with pytest.raises(ValidationError):
            RuntimeController(**runtime_config)

    def test_create_runtime_controller__with_invalid_job__raises_validation_error(
        self, runtime_config: dict[str, Any]
    ) -> None:
        """Test RuntimeController creation fails with invalid job configuration."""
        runtime_config["jobs"][0]["engine_type"] = "invalid_engine"

        with pytest.raises(ValidationError):
            RuntimeController(**runtime_config)

    def test_create_runtime_controller__with_missing_name__raises_validation_error(
        self, runtime_config: dict[str, Any]
    ) -> None:
        """Test RuntimeController creation fails when name field is missing."""
        del runtime_config["id"]

        with pytest.raises(ValidationError):
            RuntimeController(**runtime_config)

    def test_create_runtime_controller__with_empty_name__raises_validation_error(
        self, runtime_config: dict[str, Any]
    ) -> None:
        """Test RuntimeController creation fails when name is empty."""
        runtime_config["id"] = ""

        with pytest.raises(ValidationError):
            RuntimeController(**runtime_config)

    def test_create_runtime_controller__with_missing_description__raises_validation_error(
        self, runtime_config: dict[str, Any]
    ) -> None:
        """Test RuntimeController creation fails when description is missing."""
        del runtime_config["description"]

        with pytest.raises(ValidationError):
            RuntimeController(**runtime_config)

    def test_create_runtime_controller__with_missing_enabled__raises_validation_error(
        self, runtime_config: dict[str, Any]
    ) -> None:
        """Test RuntimeController creation fails when enabled is missing."""
        del runtime_config["enabled"]

        with pytest.raises(ValidationError):
            RuntimeController(**runtime_config)

    def test_create_runtime_controller__with_enabled_false__succeeds(self, runtime_config: dict[str, Any]) -> None:
        """Test RuntimeController creation succeeds with enabled set to False."""
        runtime_config["enabled"] = False

        controller = RuntimeController(**runtime_config)

        assert controller.enabled is False


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="runtime_controller")
def fixture_runtime_controller(runtime_config: dict[str, Any]) -> RuntimeController:
    """Instantiate a RuntimeController from the provided config."""
    return RuntimeController(**runtime_config)


# =========================================================================== #
# ========================== FROM_FILE TESTS ============================== #
# =========================================================================== #


class TestRuntimeControllerFromFile:
    """Test RuntimeController.from_file() class method."""

    def test_from_file__with_valid_json_config__succeeds(self, runtime_config: dict[str, Any], tmp_path: Path) -> None:
        """Test from_file creates RuntimeController from valid JSON file."""
        config_file = tmp_path / "runtime_config.json"
        config_data = {"runtime": runtime_config}
        config_file.write_text(json.dumps(config_data), encoding="utf-8")

        controller = RuntimeController.from_file(config_file)

        assert isinstance(controller, RuntimeController)
        assert len(controller.jobs) == 1

    def test_from_file__with_nonexistent_file__raises_flint_io_error(self, tmp_path: Path) -> None:
        """Test from_file raises FlintIOError when file does not exist."""
        nonexistent_file = tmp_path / "nonexistent.json"

        with pytest.raises(FlintIOError):
            RuntimeController.from_file(nonexistent_file)

    def test_from_file__with_missing_runtime_section__raises_configuration_error(
        self, runtime_config: dict[str, Any], tmp_path: Path
    ) -> None:
        """Test from_file raises error when 'runtime' section is missing."""
        config_file = tmp_path / "invalid_config.json"
        config_file.write_text(json.dumps(runtime_config), encoding="utf-8")

        with pytest.raises(FlintRuntimeConfigurationError):
            RuntimeController.from_file(config_file)


# =========================================================================== #
# ========================== EXECUTE_ALL TESTS ============================ #
# =========================================================================== #


class TestRuntimeControllerExecuteAll:
    """Test RuntimeController.execute_all() method."""

    def test_execute_all__when_disabled__skips_job_execution(self, runtime_controller: RuntimeController) -> None:
        """Test execute_all skips all jobs when runtime is disabled."""
        runtime_controller.enabled = False
        mock_job = Mock()
        mock_job.id = "mock_job"
        runtime_controller.jobs = [mock_job]

        runtime_controller.execute_all()

        # Job should not be executed
        mock_job.execute.assert_not_called()

    def test_execute_all__with_single_job__calls_job_execute(self, runtime_controller: RuntimeController) -> None:
        """Test execute_all calls execute on single job."""
        mock_job = Mock()
        mock_job.id = "mock_job"
        runtime_controller.jobs = [mock_job]

        runtime_controller.execute_all()

        mock_job.execute.assert_called_once()

    def test_execute_all__with_multiple_jobs__calls_execute_on_all(self, runtime_controller: RuntimeController) -> None:
        """Test execute_all calls execute on all jobs in order."""
        mock_job1 = Mock()
        mock_job1.id = "job1"
        mock_job2 = Mock()
        mock_job2.id = "job2"
        runtime_controller.jobs = [mock_job1, mock_job2]

        runtime_controller.execute_all()

        mock_job1.execute.assert_called_once()
        mock_job2.execute.assert_called_once()

    def test_execute_all__when_job_fails__propagates_exception(self, runtime_controller: RuntimeController) -> None:
        """Test execute_all propagates exception when a job fails."""
        mock_job = Mock()
        mock_job.id = "failing_job"
        mock_job.execute.side_effect = Exception("Job execution failed")
        runtime_controller.jobs = [mock_job]

        with pytest.raises(Exception):
            runtime_controller.execute_all()
