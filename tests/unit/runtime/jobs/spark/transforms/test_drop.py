"""Tests for Drop transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any

import pytest

from flint.runtime.jobs.models.transforms.model_drop import DropArgs
from flint.runtime.jobs.spark.transforms.drop import DropFunction

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="drop_config")
def fixture_drop_config() -> dict[str, Any]:
    """Configuration dict for DropFunction."""
    return {"function": "drop", "arguments": {"columns": ["temp_col"]}}


def test_drop_creation__from_config__creates_valid_model(drop_config: dict[str, Any]) -> None:
    """Ensure DropFunction can be created from a config dict."""
    f = DropFunction(**drop_config)
    assert f.function == "drop"
    assert isinstance(f.arguments, DropArgs)
    assert f.arguments.columns == ["temp_col"]


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="drop_func")
def fixture_drop_func(drop_config: dict[str, Any]) -> DropFunction:
    """Instantiate DropFunction from config."""
    return DropFunction(**drop_config)


def test_drop_fixture(drop_func: DropFunction) -> None:
    """Check the instantiated object fixture contains the expected columns."""
    assert drop_func.function == "drop"
    assert isinstance(drop_func.arguments, DropArgs)
    assert drop_func.arguments.columns == ["temp_col"]


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #
