"""E2E test module for job execution."""

import glob
from pathlib import Path

import pytest

from tests.e2e.framework.executor import JobTestExecutor
from tests.e2e.framework.verifier import ResultVerifier


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
