"""Tests for Select transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any

import pytest

from flint.runtime.jobs.models.transforms.model_select import SelectArgs
from flint.runtime.jobs.spark.transforms.select import SelectFunction

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="select_config")
def fixture_select_config() -> dict[str, Any]:
    """Return a config dict for SelectFunction."""
    return {"function": "select", "arguments": {"columns": ["id", "name"]}}


def test_select_creation__from_config__creates_valid_model(select_config: dict[str, Any]) -> None:
    """Instantiate from config only to test dict-based initialization."""
    f = SelectFunction(**select_config)
    assert f.function == "select"
    assert isinstance(f.arguments, SelectArgs)
    assert f.arguments.columns == ["id", "name"]


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="select_func")
def select_func(select_config: dict[str, Any]) -> SelectFunction:
    """Instantiate a SelectFunction from the config dict."""
    return SelectFunction(**select_config)


def test_select_fixture(select_func: SelectFunction) -> None:
    """Assert the instantiated fixture has expected columns."""
    assert select_func.function == "select"
    assert isinstance(select_func.arguments, SelectArgs)
    assert select_func.arguments.columns == ["id", "name"]


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #
