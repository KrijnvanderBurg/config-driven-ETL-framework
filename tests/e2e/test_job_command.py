"""E2E test module for job execution.

This test module verifies that Samara jobs execute correctly via CLI commands
and produce expected outputs. All job configurations under tests/e2e/job/ are
automatically discovered and tested.

The test framework:
1. Redirects output paths to temporary directories to avoid repository pollution
2. Executes jobs via CLI exactly as in production (with coverage tracking)
3. Verifies both data and schema match expected results using Spark DataFrames
"""

import glob
import json
import logging
import subprocess
from pathlib import Path

import pytest
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType
from pyspark.testing import assertDataFrameEqual

from samara.workflow.jobs.spark.session import SparkHandler

logger = logging.getLogger(__name__)


class PathRedirector:
    """Redirects paths in job configurations to isolated test directory."""

    def __init__(self, source_dir: Path, target_dir: Path) -> None:
        """Initialize path redirector.

        Args:
            source_dir: Original test directory containing job.json
            target_dir: Target temporary directory for isolation
        """
        self.source_dir = source_dir
        self.target_dir = target_dir

    def redirect_config(self, config: dict) -> dict:
        """Redirect output paths in configuration to tmp directory.

        Args:
            config: Original job configuration

        Returns:
            Modified configuration with redirected output paths
        """
        for job in config["workflow"]["jobs"]:
            # Leave input paths as-is (no need to copy read-only files)

            # Redirect load paths (outputs) to tmp
            for load in job["loads"]:
                load["location"] = self._redirect_output_path(load["location"])
                load["schema_export"] = self._redirect_output_path(load["schema_export"])

        return config

    def _redirect_output_path(self, path: str) -> str:
        """Redirect output path to tmp outputs directory."""
        return str(self.target_dir / "outputs" / Path(path).name)


class JobTestExecutor:
    """Executes job configurations in isolated test environment."""

    def __init__(self, job_path: Path, tmp_path: Path) -> None:
        """Initialize executor with job configuration path.

        Args:
            job_path: Path to job.json file
            tmp_path: Temporary directory for test execution
        """
        self.job_path = job_path
        self.job_dir = job_path.parent
        self.tmp_path = tmp_path
        self.isolated_config_path = tmp_path / "isolated_job.json"

    def prepare_isolated_config(self) -> dict:
        """Create isolated job configuration with redirected paths.

        Returns:
            Modified job configuration with tmp paths
        """
        with open(self.job_path, encoding="utf-8") as f:
            config = json.load(f)

        # Create path redirector for this test
        redirector = PathRedirector(source_dir=self.job_dir, target_dir=self.tmp_path)

        # Redirect output paths to tmp (inputs stay as-is)
        isolated_config = redirector.redirect_config(config)

        return isolated_config

    def execute(self) -> dict:
        """Execute job using CLI in production-like manner.

        Returns:
            The isolated config with tmp paths
        """
        # Prepare isolated configuration
        isolated_config = self.prepare_isolated_config()

        # Write isolated config to tmp location
        with open(self.isolated_config_path, "w", encoding="utf-8") as f:
            json.dump(isolated_config, f, indent=2)

        # Execute via CLI exactly as in production (with coverage tracking)
        result = subprocess.run(
            [
                "coverage",
                "run",
                "--parallel-mode",
                "--branch",
                "--source=samara",
                "-m",
                "samara",
                "--otlp-traces-endpoint",
                "http://otel-collector:4318/v1/traces",
                "--otlp-logs-endpoint",
                "http://otel-collector:4318/v1/logs",
                "run",
                "--workflow-filepath",
                str(self.isolated_config_path),
                "--alert-filepath",
                str(self.isolated_config_path),
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            logger.error("Command failed with exit code %d", result.returncode)
            logger.error("STDOUT:\n%s", result.stdout)
            logger.error("STDERR:\n%s", result.stderr)

        assert result.returncode == 0

        logger.debug("Job executed successfully:\n%s", result.stdout)

        return isolated_config


class ResultVerifier:
    """Verifies test execution results against expected outputs."""

    def __init__(self, job_dir: Path) -> None:
        """Initialize verifier with job directory.

        Args:
            job_dir: Directory containing job.json and expected outputs
        """
        self.job_dir = job_dir
        self.spark: SparkSession = SparkHandler().session

    def verify_outputs(self, isolated_config: dict) -> None:
        """Verify all outputs match expected results.

        Args:
            isolated_config: Job configuration with tmp output paths
        """
        for job in isolated_config["workflow"]["jobs"]:
            for load in job["loads"]:
                expected_dir = self.job_dir / load["id"]

                self._verify_single_output(
                    load_name=load["id"],
                    actual_data_path=Path(load["location"]),
                    actual_schema_path=Path(load["schema_export"]),
                    expected_dir=expected_dir,
                )

    def _verify_single_output(
        self, load_name: str, actual_data_path: Path, actual_schema_path: Path, expected_dir: Path
    ) -> None:
        """Verify a single output against expected results.

        Args:
            load_name: Name of the load output
            actual_data_path: Path to actual output data
            actual_schema_path: Path to actual output schema
            expected_dir: Directory containing expected outputs
        """
        # Find expected files
        expected_schema_path = expected_dir / "expected_schema.json"
        expected_data_files = list(expected_dir.glob("expected_output.*"))

        if not expected_data_files:
            logger.warning("No expected data file found for load '%s'", load_name)
            return

        expected_data_path = expected_data_files[0]
        data_format = expected_data_path.suffix.lstrip(".")

        # Verify schema
        if expected_schema_path.exists():
            self._verify_schema(actual_schema_path, expected_schema_path)

        # Verify data
        self._verify_data(
            actual_data_path=actual_data_path,
            expected_data_path=expected_data_path,
            data_format=data_format,
            schema_path=actual_schema_path,
        )

    def _verify_schema(self, actual_path: Path, expected_path: Path) -> None:
        """Compare actual and expected schemas.

        Args:
            actual_path: Path to actual schema
            expected_path: Path to expected schema
        """
        with open(actual_path, encoding="utf-8") as f:
            actual_schema = StructType.fromJson(json.load(f))

        with open(expected_path, encoding="utf-8") as f:
            expected_schema = StructType.fromJson(json.load(f))

        assert actual_schema == expected_schema, f"Schema mismatch: {actual_schema} != {expected_schema}"

    def _verify_data(
        self, actual_data_path: Path, expected_data_path: Path, data_format: str, schema_path: Path
    ) -> None:
        """Compare actual and expected data.

        Args:
            actual_data_path: Path to actual data
            expected_data_path: Path to expected data
            data_format: Data format (e.g., 'csv', 'json')
            schema_path: Path to schema file
        """
        # Load schema
        with open(schema_path, encoding="utf-8") as f:
            schema = StructType.fromJson(json.load(f))

        # Read actual data
        df_actual = self.spark.read.format(data_format).schema(schema).load(str(actual_data_path))

        # Read expected data
        df_expected = self.spark.read.format(data_format).schema(schema).load(str(expected_data_path))

        # Compare DataFrames
        assertDataFrameEqual(actual=df_actual, expected=df_expected)


class TestJobExecution:
    """E2E tests for job execution via CLI."""

    @pytest.mark.parametrize("job_path", glob.glob("tests/e2e/job/**/job.json", recursive=True))
    def test_job_command__execute_and_verify__matches_expected_output(self, tmp_path: Path, job_path: str) -> None:
        """Test job execution produces expected outputs.

        Args:
            tmp_path: Pytest temporary directory fixture
            job_path: Path to job.json configuration file
        """
        job_path_obj = Path(job_path)

        # Execute job in isolated environment
        executor = JobTestExecutor(job_path_obj, tmp_path)
        isolated_config = executor.execute()

        # Verify outputs match expected results
        verifier = ResultVerifier(job_path_obj.parent)
        verifier.verify_outputs(isolated_config)
