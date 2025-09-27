"""Tests for WithColumn transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any

import pytest

from flint.runtime.jobs.models.transforms.model_withcolumn import WithColumnArgs
from flint.runtime.jobs.spark.transforms.withcolumn import WithColumnFunction

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="withcolumn_config")
def fixture_withcolumn_config() -> dict[str, Any]:
    """Return a config dict for WithColumnFunction."""
    return {
        "function": "withColumn",
        "arguments": {"col_name": "full_name", "col_expr": "concat(first_name, ' ', last_name)"},
    }


def test_withcolumn_creation__from_config__creates_valid_model(withcolumn_config: dict[str, Any]) -> None:
    """Instantiate from config only to test dict-based initialization."""
    f = WithColumnFunction(**withcolumn_config)
    assert f.function == "withColumn"
    assert isinstance(f.arguments, WithColumnArgs)
    assert f.arguments.col_name == "full_name"
    assert f.arguments.col_expr.startswith("concat")


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="withcolumn_func")
def fixture_withcolumn_func(withcolumn_config: dict[str, Any]) -> WithColumnFunction:
    """Instantiate a WithColumnFunction from the config dict."""
    return WithColumnFunction(**withcolumn_config)


def test_withcolumn_fixture__values(withcolumn_func: WithColumnFunction) -> None:
    """Assert the instantiated fixture has expected column name and expression."""
    assert withcolumn_func.function == "withColumn"
    assert isinstance(withcolumn_func.arguments, WithColumnArgs)
    assert withcolumn_func.arguments.col_name == "full_name"
    assert withcolumn_func.arguments.col_expr.startswith("concat")


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #
