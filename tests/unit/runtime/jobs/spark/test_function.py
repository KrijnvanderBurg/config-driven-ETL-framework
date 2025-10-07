"""Unit tests for a concrete Function subclass used in Spark transforms."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

import pytest

from flint.runtime.jobs.models.model_transform import ArgsModel
from flint.runtime.jobs.spark.function import Function


class _DummyArgs(ArgsModel):
    """Minimal args model for DummyFunction."""

    test_arg: str | None = None


class DummyFunction(Function):
    """A tiny concrete Function implementation for testing."""

    function: Literal["dummy"]
    arguments: _DummyArgs

    def transform(self) -> Callable[..., Any | None]:
        def _f(df=None) -> Any | None:  # return the dataframe unchanged
            return df

        return _f


# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="function_config")
def fixture_function_config() -> dict:
    return {"function": "dummy", "arguments": {"test_arg": "example"}}


def test_function_creation__from_config__creates_valid_model(function_config: dict) -> None:
    f = DummyFunction(**function_config)
    assert f.function == "dummy"
    assert isinstance(f.arguments, _DummyArgs)
    assert f.arguments.test_arg == "example"


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="function_obj")
def fixture_function_obj(function_config: dict[str, Any]) -> DummyFunction:
    return DummyFunction(**function_config)


def test_function_fixture(function_obj: DummyFunction) -> None:
    assert function_obj.function == "dummy"
    assert isinstance(function_obj.arguments, _DummyArgs)
    assert function_obj.arguments.test_arg == "example"


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #


def test_function_fixture__transform_returns_input(function_obj: DummyFunction) -> None:
    fn = function_obj.transform()
    assert isinstance(fn, Callable)
    sample = object()
    assert fn(sample) is sample
