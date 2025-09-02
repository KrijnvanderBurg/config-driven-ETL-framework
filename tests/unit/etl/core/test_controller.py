"""Unit tests for the ETL controller module.

This module contains tests for the Etl class, which is the central component
that manages multiple ETL jobs and orchestrates their execution.

The tests verify various aspects of Etl functionality including:
- Creation from configuration files and dictionaries
- Validation of all jobs in the pipeline
- Execution of all jobs in sequence
- Proper handling of multiple jobs
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from flint.etl.core.controller import Etl
from flint.etl.core.job import Job
from flint.exceptions import FlintConfigurationKeyError


class TestEtl:
    """Unit tests for the Etl class.

    Tests cover:
        - Creation from configuration files and dictionaries
        - Validation and execution of multiple jobs
        - Error handling for missing configuration keys
    """

    @patch("flint.utils.file.FileHandlerContext.from_filepath")
    def test_from_file(self, mock_from_filepath: MagicMock) -> None:
        """Test creating an Etl instance from a configuration file."""
        # Arrange
        mock_file_handler = MagicMock()
        mock_file_handler.read.return_value = {
            "etl": [
                {
                    "name": "job1",
                    "extracts": [{"name": "test_extract"}],
                    "transforms": [{"name": "test_transform", "upstream_name": "test_extract", "options": {}}],
                    "loads": [{"name": "test_load", "upstream_name": "test_transform"}],
                },
                {
                    "name": "job2",
                    "extracts": [{"name": "test_extract2"}],
                    "transforms": [{"name": "test_transform2", "upstream_name": "test_extract2", "options": {}}],
                    "loads": [{"name": "test_load2", "upstream_name": "test_transform2"}],
                }
            ]
        }
        mock_from_filepath.return_value = mock_file_handler

        # Mock the Job.from_dict method
        with patch.object(Job, "from_dict") as mock_job_from_dict:
            mock_job1 = MagicMock(spec=Job)
            mock_job2 = MagicMock(spec=Job)
            mock_job_from_dict.side_effect = [mock_job1, mock_job2]

            # Act
            etl = Etl.from_file(Path("test.json"))

            # Assert
            assert len(etl.jobs) == 2
            assert etl.jobs[0] == mock_job1
            assert etl.jobs[1] == mock_job2

            # Verify file operations
            mock_from_filepath.assert_called_once_with(filepath=Path("test.json"))
            mock_file_handler.read.assert_called_once()

            # Verify Job.from_dict calls
            assert mock_job_from_dict.call_count == 2

    def test_from_dict(self) -> None:
        """Test creating an Etl instance from a configuration dictionary."""
        # Arrange
        etl_dict: dict = {
            "etl": [
                {
                    "name": "job1",
                    "extracts": [{"name": "test_extract"}],
                    "transforms": [{"name": "test_transform", "upstream_name": "test_extract", "options": {}}],
                    "loads": [{"name": "test_load", "upstream_name": "test_transform"}],
                },
                {
                    "name": "job2",
                    "extracts": [{"name": "test_extract2"}],
                    "transforms": [{"name": "test_transform2", "upstream_name": "test_extract2", "options": {}}],
                    "loads": [{"name": "test_load2", "upstream_name": "test_transform2"}],
                }
            ]
        }

        # Mock the Job.from_dict method
        with patch.object(Job, "from_dict") as mock_job_from_dict:
            mock_job1 = MagicMock(spec=Job)
            mock_job2 = MagicMock(spec=Job)
            mock_job_from_dict.side_effect = [mock_job1, mock_job2]

            # Act
            etl = Etl.from_dict(etl_dict)

            # Assert
            assert len(etl.jobs) == 2
            assert etl.jobs[0] == mock_job1
            assert etl.jobs[1] == mock_job2

            # Verify Job.from_dict calls
            expected_calls = [
                mock_job_from_dict.call_args_list[0][1]["dict_"],
                mock_job_from_dict.call_args_list[1][1]["dict_"]
            ]
            assert expected_calls[0]["name"] == "job1"
            assert expected_calls[1]["name"] == "job2"

    def test_from_dict_single_job(self) -> None:
        """Test creating an Etl instance with a single job."""
        # Arrange
        etl_dict: dict = {
            "etl": [
                {
                    "name": "single_job",
                    "extracts": [{"name": "test_extract"}],
                    "transforms": [{"name": "test_transform", "upstream_name": "test_extract", "options": {}}],
                    "loads": [{"name": "test_load", "upstream_name": "test_transform"}],
                }
            ]
        }

        # Mock the Job.from_dict method
        with patch.object(Job, "from_dict") as mock_job_from_dict:
            mock_job = MagicMock(spec=Job)
            mock_job_from_dict.return_value = mock_job

            # Act
            etl = Etl.from_dict(etl_dict)

            # Assert
            assert len(etl.jobs) == 1
            assert etl.jobs[0] == mock_job

            # Verify Job.from_dict call
            mock_job_from_dict.assert_called_once()

    def test_from_dict_empty_jobs(self) -> None:
        """Test creating an Etl instance with no jobs."""
        # Arrange
        etl_dict: dict = {"etl": []}

        # Act
        etl = Etl.from_dict(etl_dict)

        # Assert
        assert len(etl.jobs) == 0

    def test_from_dict_missing_etl_key(self) -> None:
        """Test that missing 'etl' key raises FlintConfigurationKeyError."""
        # Arrange
        etl_dict: dict = {"other_key": "value"}

        # Act & Assert
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            Etl.from_dict(etl_dict)

        assert "etl" in str(exc_info.value)

    def test_validate_all(self) -> None:
        """Test validation of all jobs in the ETL pipeline."""
        # Arrange
        mock_job1 = MagicMock(spec=Job)
        mock_job1.name = "job1"
        mock_job2 = MagicMock(spec=Job)
        mock_job2.name = "job2"
        
        etl = Etl(jobs=[mock_job1, mock_job2])

        # Act
        etl.validate_all()

        # Assert
        mock_job1.validate.assert_called_once()
        mock_job2.validate.assert_called_once()

    def test_validate_all_empty_jobs(self) -> None:
        """Test validation with no jobs (should not fail)."""
        # Arrange
        etl = Etl(jobs=[])

        # Act & Assert (should not raise)
        etl.validate_all()

    def test_execute_all(self) -> None:
        """Test execution of all jobs in the ETL pipeline."""
        # Arrange
        mock_job1 = MagicMock(spec=Job)
        mock_job1.name = "job1"
        mock_job2 = MagicMock(spec=Job)
        mock_job2.name = "job2"
        
        etl = Etl(jobs=[mock_job1, mock_job2])

        # Act
        etl.execute_all()

        # Assert
        mock_job1.execute.assert_called_once()
        mock_job2.execute.assert_called_once()

    def test_execute_all_empty_jobs(self) -> None:
        """Test execution with no jobs (should not fail)."""
        # Arrange
        etl = Etl(jobs=[])

        # Act & Assert (should not raise)
        etl.execute_all()

    def test_execute_all_maintains_order(self) -> None:
        """Test that jobs are executed in the order they appear in the list."""
        # Arrange
        execution_order = []
        
        mock_job1 = MagicMock(spec=Job)
        mock_job1.name = "job1"
        mock_job1.execute.side_effect = lambda: execution_order.append("job1")
        
        mock_job2 = MagicMock(spec=Job)
        mock_job2.name = "job2"
        mock_job2.execute.side_effect = lambda: execution_order.append("job2")
        
        mock_job3 = MagicMock(spec=Job)
        mock_job3.name = "job3"
        mock_job3.execute.side_effect = lambda: execution_order.append("job3")
        
        etl = Etl(jobs=[mock_job1, mock_job2, mock_job3])

        # Act
        etl.execute_all()

        # Assert
        assert execution_order == ["job1", "job2", "job3"]

    def test_validate_all_maintains_order(self) -> None:
        """Test that jobs are validated in the order they appear in the list."""
        # Arrange
        validation_order = []
        
        mock_job1 = MagicMock(spec=Job)
        mock_job1.name = "job1"
        mock_job1.validate.side_effect = lambda: validation_order.append("job1")
        
        mock_job2 = MagicMock(spec=Job)
        mock_job2.name = "job2"
        mock_job2.validate.side_effect = lambda: validation_order.append("job2")
        
        mock_job3 = MagicMock(spec=Job)
        mock_job3.name = "job3"
        mock_job3.validate.side_effect = lambda: validation_order.append("job3")
        
        etl = Etl(jobs=[mock_job1, mock_job2, mock_job3])

        # Act
        etl.validate_all()

        # Assert
        assert validation_order == ["job1", "job2", "job3"]
