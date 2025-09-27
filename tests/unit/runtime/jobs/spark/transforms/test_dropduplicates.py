"""Tests for DropDuplicates transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any

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
