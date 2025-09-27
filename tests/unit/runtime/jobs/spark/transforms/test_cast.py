"""Tests for Cast transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any

import pytest

from flint.runtime.jobs.models.transforms.model_cast import CastArgs
from flint.runtime.jobs.spark.transforms.cast import CastFunction

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="cast_config")
def fixture_cast_config() -> dict[str, Any]:
    """Configuration dict for CastFunction."""
    return {"function": "cast", "arguments": {"columns": [{"column_name": "age", "cast_type": "int"}]}}


def test_cast_creation__from_config__creates_valid_model(cast_config: dict[str, Any]) -> None:
    """Ensure the CastFunction can be created from a config dict."""
    f = CastFunction(**cast_config)
    assert f.function == "cast"
    assert isinstance(f.arguments, CastArgs)
    assert f.arguments.columns[0].column_name == "age"
    assert f.arguments.columns[0].cast_type == "int"


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="cast_func")
def fixture_cast_func(cast_config: dict[str, Any]) -> CastFunction:
    """Instantiate a CastFunction from the config dict.

    The object fixture is used to assert runtime field values without
    re-creating the config.
    """
    return CastFunction(**cast_config)


def test_cast_fixture(cast_func: CastFunction) -> None:
    """Sanity-check the instantiated fixture has the expected arguments."""
    assert cast_func.function == "cast"
    assert isinstance(cast_func.arguments, CastArgs)
    assert cast_func.arguments.columns[0].column_name == "age"
    assert cast_func.arguments.columns[0].cast_type == "int"


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #
