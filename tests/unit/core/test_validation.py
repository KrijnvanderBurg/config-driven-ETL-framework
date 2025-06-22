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

from flint.core.job import Job
from flint.core.validation import ValidateModelNamesAreUnique


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
