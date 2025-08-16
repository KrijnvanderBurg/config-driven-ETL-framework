"""Unit tests for the validation module.

This module contains tests for validation classes responsible for ensuring
data integrity and configuration correctness in ETL jobs.

The tests verify various validation scenarios including:
- Model name uniqueness across all ETL stages
- Edge cases with empty collections
- Different duplicate combinations across extract, transform, and load stages
"""

from unittest.mock import MagicMock

import pytest

from flint.job.core.job import Job
from flint.job.core.validation import ValidateModelNamesAreUnique, ValidateUpstreamNamesExist


class TestValidateModelNamesAreUnique:
    """Unit tests for the ValidateModelNamesAreUnique validator."""

    @pytest.mark.parametrize(
        "extract_names,transform_names,load_names,should_raise",
        [
            # Valid cases
            (["extract1", "extract2"], ["transform1", "transform2"], ["load1", "load2"], False),
            ([], [], [], False),
            (["single"], [], [], False),
            (["Case"], ["case"], [], False),  # Case sensitive
            # Duplicates within same stage
            (["duplicate", "duplicate"], [], [], True),
            ([], ["duplicate", "duplicate"], [], True),
            ([], [], ["duplicate", "duplicate"], True),
            # Duplicates across stages
            (["duplicate"], ["duplicate"], [], True),
            (["duplicate"], [], ["duplicate"], True),
            ([], ["duplicate"], ["duplicate"], True),
            # Multiple names with one duplicate
            (["a", "b"], ["c"], ["b"], True),
        ],
    )
    def test_validate_model_names(
        self, extract_names: list[str], transform_names: list[str], load_names: list[str], should_raise: bool
    ) -> None:
        """Test validation of model names."""
        # Arrange
        extracts = []
        for name in extract_names:
            extract_mock = MagicMock()
            extract_mock.model.name = name
            extracts.append(extract_mock)

        transforms = []
        for name in transform_names:
            transform_mock = MagicMock()
            transform_mock.model.name = name
            transforms.append(transform_mock)

        loads = []
        for name in load_names:
            load_mock = MagicMock()
            load_mock.model.name = name
            loads.append(load_mock)

        job = Job(extracts=extracts, transforms=transforms, loads=loads)

        # Act & Assert
        if should_raise:
            with pytest.raises(ValueError):
                ValidateModelNamesAreUnique(data=job)
        else:
            ValidateModelNamesAreUnique(data=job)


class TestValidateUpstreamNamesExist:
    """Unit tests for the ValidateUpstreamNamesExist validator."""

    @pytest.mark.parametrize(
        "extract_names,transform_configs,load_configs,should_raise",
        [
            # Empty job
            ([], [], [], False),
            # Only extracts
            (["extract1", "extract2"], [], [], False),
            # Valid cases
            (
                ["extract1", "extract2"],
                [("transform1", "extract1"), ("transform2", "extract2")],
                [("load1", "transform1"), ("load2", "transform2")],
                False,
            ),
            # Transform references valid extract
            (
                ["source"],
                [("transform1", "source")],
                [],
                False,
            ),
            # Load references valid transform
            (
                ["source"],
                [("transform1", "source")],
                [("load1", "transform1")],
                False,
            ),
            # Chain: extract -> transform -> load
            (
                ["data"],
                [("processed", "data")],
                [("output", "processed")],
                False,
            ),
            # Invalid transform upstream
            (
                ["extract1"],
                [("transform1", "nonexistent")],
                [],
                True,
            ),
            # Invalid load upstream
            (
                ["extract1"],
                [],
                [("load1", "nonexistent")],
                True,
            ),
            # Multiple stages, one invalid upstream
            (
                ["extract1", "extract2"],
                [("transform1", "extract1"), ("transform2", "missing")],
                [],
                True,
            ),
        ],
    )
    def test_validate_upstream_names(
        self,
        extract_names: list[str],
        transform_configs: list[tuple[str, str]],
        load_configs: list[tuple[str, str]],
        should_raise: bool,
    ) -> None:
        """Test validation of upstream names in transforms and loads."""
        # Arrange
        extracts = []
        for name in extract_names:
            extract_mock = MagicMock()
            extract_mock.model.name = name
            extracts.append(extract_mock)

        transforms = []
        for name, upstream in transform_configs:
            transform_mock = MagicMock()
            transform_mock.model.name = name
            transform_mock.model.upstream_name = upstream
            transforms.append(transform_mock)

        loads = []
        for name, upstream in load_configs:
            load_mock = MagicMock()
            load_mock.model.name = name
            load_mock.model.upstream_name = upstream
            loads.append(load_mock)

        job = Job(extracts=extracts, transforms=transforms, loads=loads)

        # Act & Assert
        if should_raise:
            with pytest.raises(ValueError):
                ValidateUpstreamNamesExist(data=job)
        else:
            ValidateUpstreamNamesExist(data=job)
