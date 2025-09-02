"""Unit tests for the job module.

This module contains tests for the Job class, which is the central component
of the ingestion framework responsible for orchestrating the ETL pipeline.

The tests verify various aspects of Job functionality including:
- Initialization with extracts, transforms, and loads
- Creation from configuration dictionaries
- Execution of the complete ETL pipeline
- Proper sequencing of extract, transform, and load operations
"""

from unittest.mock import MagicMock, patch

import pytest

from flint.etl.core.extract import Extract
from flint.etl.core.job import Job
from flint.etl.core.load import Load
from flint.etl.core.transform import Transform
from flint.exceptions import FlintConfigurationKeyError


class TestJob:
    """Unit tests for the Job class.

    Tests cover:
        - Initialization with extract, transform, and load components
        - Creation from configuration files and dictionaries
        - Execution and sequencing of ETL pipeline phases
        - Error handling for missing configuration keys
    """

    def test_from_dict(self) -> None:
        """Test creating a Job from a configuration dictionary."""
        # Arrange
        job_dict: dict = {
            "name": "test_job",
            "extracts": [{"name": "test_extract"}],
            "transforms": [{"name": "test_transform", "upstream_name": "test_extract", "options": {}}],
            "loads": [{"name": "test_load", "upstream_name": "test_transform"}],
        }

        # Mock the from_dict methods
        with (
            patch.object(Extract, "from_dict") as mock_extract_from_dict,
            patch.object(Transform, "from_dict") as mock_transform_from_dict,
            patch.object(Load, "from_dict") as mock_load_from_dict,
        ):
            # Configure mocks
            mock_extract = MagicMock(spec=Extract)
            mock_transform = MagicMock(spec=Transform)
            mock_load = MagicMock(spec=Load)

            mock_extract_from_dict.return_value = mock_extract
            mock_transform_from_dict.return_value = mock_transform
            mock_load_from_dict.return_value = mock_load

            # Act
            job = Job.from_dict(job_dict)

            # Assert
            assert job.name == "test_job"
            assert len(job.extracts) == 1
            assert len(job.transforms) == 1
            assert len(job.loads) == 1
            assert job.extracts[0] == mock_extract
            assert job.transforms[0] == mock_transform
            assert job.loads[0] == mock_load

            # Verify method calls
            mock_extract_from_dict.assert_called_once_with(dict_={"name": "test_extract"})
            mock_transform_from_dict.assert_called_once_with(
                dict_={"name": "test_transform", "upstream_name": "test_extract", "options": {}}
            )
            mock_load_from_dict.assert_called_once_with(dict_={"name": "test_load", "upstream_name": "test_transform"})

    def test_from_dict_missing_key_error(self) -> None:
        """Test that missing required keys raise FlintConfigurationKeyError."""
        # Arrange
        job_dict: dict = {"invalid": "structure"}

        # Act & Assert
        with pytest.raises(FlintConfigurationKeyError):
            Job.from_dict(job_dict)

    @patch("flint.etl.core.job.ValidateModelNamesAreUnique")
    @patch("flint.etl.core.job.ValidateUpstreamNamesExist")
    def test_validate(self, mock_validate_upstream: MagicMock, mock_validate_unique: MagicMock) -> None:
        """Test job validation calls validation functions."""
        # Arrange
        job = Job(name="test_job", extracts=[], transforms=[], loads=[])

        # Act
        job.validate()

        # Assert
        mock_validate_unique.assert_called_once_with(data=job)
        mock_validate_upstream.assert_called_once_with(data=job)

    def test_execute(self) -> None:
        """Test job execution calls all phases."""
        # Arrange
        mock_extract = MagicMock(spec=Extract)
        mock_extract.model = MagicMock()
        mock_extract.model.name = "test_extract"
        mock_transform = MagicMock(spec=Transform)
        mock_transform.model = MagicMock()
        mock_transform.model.name = "test_transform"
        mock_load = MagicMock(spec=Load)
        mock_load.model = MagicMock()
        mock_load.model.name = "test_load"

        job = Job(name="test_job", extracts=[mock_extract], transforms=[mock_transform], loads=[mock_load])

        # Act
        job.execute()

        # Assert
        mock_extract.extract.assert_called_once()
        mock_transform.transform.assert_called_once()
        mock_load.load.assert_called_once()

    def test_extract_phase(self) -> None:
        """Test the _extract method calls extract on all extractors."""
        # Arrange
        mock_extract1 = MagicMock(spec=Extract)
        mock_extract1.model = MagicMock()
        mock_extract1.model.name = "extract1"
        mock_extract2 = MagicMock(spec=Extract)
        mock_extract2.model = MagicMock()
        mock_extract2.model.name = "extract2"

        job = Job(name="test_job", extracts=[mock_extract1, mock_extract2], transforms=[], loads=[])

        # Act
        job._extract()

        # Assert
        mock_extract1.extract.assert_called_once()
        mock_extract2.extract.assert_called_once()

    def test_transform_phase(self) -> None:
        """Test the _transform method calls transform on all transformers."""
        # Arrange
        mock_transform1 = MagicMock(spec=Transform)
        mock_transform1.model = MagicMock()
        mock_transform1.model.name = "transform1"
        mock_transform2 = MagicMock(spec=Transform)
        mock_transform2.model = MagicMock()
        mock_transform2.model.name = "transform2"

        job = Job(name="test_job", extracts=[], transforms=[mock_transform1, mock_transform2], loads=[])

        # Act
        job._transform()

        # Assert
        mock_transform1.transform.assert_called_once()
        mock_transform2.transform.assert_called_once()

    def test_load_phase(self) -> None:
        """Test the _load method calls load on all loaders."""
        # Arrange
        mock_load1 = MagicMock(spec=Load)
        mock_load1.model = MagicMock()
        mock_load1.model.name = "load1"
        mock_load2 = MagicMock(spec=Load)
        mock_load2.model = MagicMock()
        mock_load2.model.name = "load2"

        job = Job(name="test_job", extracts=[], transforms=[], loads=[mock_load1, mock_load2])

        # Act
        job._load()

        # Assert
        mock_load1.load.assert_called_once()
        mock_load2.load.assert_called_once()
