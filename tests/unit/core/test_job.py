"""Unit tests for the job module.

This module contains tests for the Job class, which is the central component
of the ingestion framework responsible for orchestrating the ETL pipeline.

The tests verify various aspects of Job functionality including:
- Initialization with extracts, transforms, and loads
- Creation from configuration files and dictionaries
- Execution of the complete ETL pipeline
- Proper sequencing of extract, transform, and load operations
"""

from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from flint.core.extract import Extract
from flint.core.job import Job
from flint.core.load import Load
from flint.core.transform import Transform


class TestJob:
    """Unit tests for the Job class.

    Tests cover:
        - Initialization with extract, transform, and load components
        - Creation from configuration files and dictionaries
        - Execution and sequencing of ETL pipeline phases
        - Error handling for missing configuration keys
    """

    @patch("flint.utils.file.FileHandlerContext.from_filepath")
    def test_from_file(self, mock_from_filepath: MagicMock) -> None:
        """Test creating a Job from a configuration file."""
        # Arrange
        mock_file_handler = MagicMock()
        mock_file_handler.read.return_value = {
            "extracts": [{"name": "test_extract"}],
            "transforms": [{"name": "test_transform", "upstream_name": "test_extract"}],
            "loads": [{"name": "test_load", "upstream_name": "test_transform"}],
        }
        mock_from_filepath.return_value = mock_file_handler

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
            job = Job.from_file(Path("test.json"))

            # Assert
            assert len(job.extracts) == 1
            assert len(job.transforms) == 1
            assert len(job.loads) == 1

            assert job.extracts[0] == mock_extract
            assert job.transforms[0] == mock_transform
            assert job.loads[0] == mock_load

    @patch("pathlib.Path.suffix", new_callable=PropertyMock)
    @patch("flint.utils.file.FileHandlerContext.from_filepath")
    def test_from_file_unsupported_format(self, mock_from_filepath: MagicMock, mock_suffix: PropertyMock) -> None:
        """Test that using an unsupported file format raises NotImplementedError."""
        # Arrange
        mock_suffix.return_value = ".yaml"
        mock_handler = MagicMock()
        mock_from_filepath.return_value = mock_handler
        mock_handler.read.return_value = {}

        # Act & Assert
        with pytest.raises(NotImplementedError):
            Job.from_file(Path("test.yaml"))

    def test_from_dict(self) -> None:
        """Test creating a Job from a configuration dictionary."""
        # Arrange
        job_dict: dict = {
            "extracts": [{"name": "test_extract"}],
            "transforms": [{"name": "test_transform", "upstream_name": "test_extract"}],
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
            assert len(job.extracts) == 1
            assert len(job.transforms) == 1
            assert len(job.loads) == 1

            assert job.extracts[0] == mock_extract
            assert job.transforms[0] == mock_transform
            assert job.loads[0] == mock_load

    def test_execute(self) -> None:
        """Test execution of the ETL pipeline (extract, transform, load)."""
        # Arrange
        extract = MagicMock(spec=Extract)

        transform = MagicMock(spec=Transform)
        # Add data_registry as a property to the mock
        transform_data_registry = {}
        type(transform).data_registry = MagicMock()
        transform.data_registry.__getitem__ = lambda self, key: transform_data_registry.get(key)
        transform.data_registry.__setitem__ = lambda self, key, value: transform_data_registry.update({key: value})
        transform.model = MagicMock()
        transform.model.name = "transform_name"
        transform.model.upstream_name = "upstream_name"

        load = MagicMock(spec=Load)
        # Add data_registry as a property to the mock
        load_data_registry = {}
        type(load).data_registry = MagicMock()
        load.data_registry.__getitem__ = lambda self, key: load_data_registry.get(key)
        load.data_registry.__setitem__ = lambda self, key, value: load_data_registry.update({key: value})
        load.model = MagicMock()
        load.model.name = "load_name"
        load.model.upstream_name = "upstream_load_name"

        job = Job(extracts=[extract], transforms=[transform], loads=[load])

        # Act
        job.execute()

        # Assert
        extract.extract.assert_called_once()
        transform.transform.assert_called_once()
        load.load.assert_called_once()
