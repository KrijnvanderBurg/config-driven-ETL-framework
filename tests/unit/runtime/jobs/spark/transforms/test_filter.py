"""Tests for Filter transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any

import pytest

from flint.runtime.jobs.models.transforms.model_filter import FilterArgs
from flint.runtime.jobs.spark.transforms.filter import FilterFunction

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="filter_config")
def fixture_filter_config() -> dict:
    """Return a config dict for FilterFunction."""
    return {"function": "filter", "arguments": {"condition": "age > 18"}}


def test_filter_creation__from_config__creates_valid_model(filter_config: dict[str, Any]) -> None:
    """Instantiate from config only to test dict-based initialization."""
    f = FilterFunction(**filter_config)
    assert f.function == "filter"
    assert isinstance(f.arguments, FilterArgs)
    assert f.arguments.condition == "age > 18"


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="filter_func")
def fixture_filter_func(filter_config: dict[str, Any]) -> FilterFunction:
    """Instantiate a FilterFunction from the config dict."""
    return FilterFunction(**filter_config)


def test_filter_fixture(filter_func: FilterFunction) -> None:
    """Assert the instantiated fixture has the expected condition."""
    assert filter_func.function == "filter"
    assert isinstance(filter_func.arguments, FilterArgs)
    assert filter_func.arguments.condition == "age > 18"


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #
