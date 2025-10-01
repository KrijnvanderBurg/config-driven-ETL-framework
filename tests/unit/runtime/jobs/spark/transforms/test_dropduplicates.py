"""Tests for DropDuplicates transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import pytest

from flint.runtime.jobs.models.transforms.model_dropduplicates import DropDuplicatesArgs
from flint.runtime.jobs.spark.transforms.dropduplicates import DropDuplicatesFunction

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="dropduplicates_config")
def fixture_dropduplicates_config() -> dict[str, Any]:
    """Return a config dict for DropDuplicatesFunction.

    The config uses an explicit columns list to exercise the model parsing.
    """
    return {"function": "dropduplicates", "arguments": {"columns": ["test_col"]}}


def test_dropduplicates_creation__from_config__creates_valid_model(dropduplicates_config: dict[str, Any]) -> None:
    """Ensure DropDuplicatesFunction can be created from a config dict."""
    f = DropDuplicatesFunction(**dropduplicates_config)
    assert f.function == "dropduplicates"
    assert isinstance(f.arguments, DropDuplicatesArgs)
    assert f.arguments.columns == ["test_col"]


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="dropduplicates_func")
def fixture_dropduplicates_func(dropduplicates_config: dict[str, Any]) -> DropDuplicatesFunction:
    """Instantiate a DropDuplicatesFunction from the config dict.

    This object fixture is used by the object-based assertions below.
    """
    return DropDuplicatesFunction(**dropduplicates_config)


def test_dropduplicates_fixture(dropduplicates_func: DropDuplicatesFunction) -> None:
    """Assert the instantiated fixture has the expected columns list."""
    assert dropduplicates_func.function == "dropduplicates"
    assert isinstance(dropduplicates_func.arguments, DropDuplicatesArgs)
    assert dropduplicates_func.arguments.columns == ["test_col"]


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #


class TestDropDuplicatesFunctionTransform:
    """Test DropDuplicatesFunction transform behavior."""

    def test_transform__returns_callable(self, dropduplicates_func: DropDuplicatesFunction) -> None:
        """Test transform returns a callable function."""
        # Act
        transform_fn = dropduplicates_func.transform()

        # Assert
        assert callable(transform_fn)

    def test_transform__applies_drop_duplicates(self, dropduplicates_func: DropDuplicatesFunction) -> None:
        """Test transform applies dropDuplicates operation."""

        # Arrange
        mock_df = Mock()
        mock_df.dropDuplicates.return_value = mock_df

        # Act
        transform_fn = dropduplicates_func.transform()
        transform_fn(mock_df)

        # Assert
        mock_df.dropDuplicates.assert_called_once_with(["test_col"])

    def test_transform__with_no_columns__applies_drop_duplicates_on_all_columns(
        self, dropduplicates_config: dict[str, Any]
    ) -> None:
        """Test transform applies dropDuplicates without columns when none specified."""

        # Arrange
        dropduplicates_config["arguments"]["columns"] = None
        dropduplicates_func = DropDuplicatesFunction(**dropduplicates_config)
        mock_df = Mock()
        mock_df.dropDuplicates.return_value = mock_df

        # Act
        transform_fn = dropduplicates_func.transform()
        transform_fn(mock_df)

        # Assert
        mock_df.dropDuplicates.assert_called_once_with()
