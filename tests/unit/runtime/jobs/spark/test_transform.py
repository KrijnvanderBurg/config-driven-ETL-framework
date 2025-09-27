"""Unit tests for the TransformSpark model."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import ExitStack
from typing import Any

import pytest

from flint.runtime.jobs.spark.transform import TransformSpark

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="transform_config")
def fixture_transform_config() -> Generator[dict[str, Any], Any, None]:
    """Provide a transform configuration dict.

    Returning a Generator mirrors the `test_extract.py` layout and keeps the
    fixtures consistent across the Spark tests.
    """
    # Using ExitStack here keeps the pattern consistent even when no tempfiles
    # are required for this fixture.
    stack = ExitStack()

    data: dict[str, Any] = {"name": "tr_conf", "upstream_name": "ex_src", "options": {"opt": True}, "functions": []}

    yield data
    stack.close()


def test_transform_creation__from_config__creates_valid_model(transform_config: dict[str, Any]) -> None:
    """Ensure a TransformSpark can be created from a plain config dict and all fields are set in the expected order."""
    t = TransformSpark(**transform_config)
    # Assert fields in order: name, upstream_name, options, functions
    assert t.name == "tr_conf"
    assert t.upstream_name == "ex_src"
    assert t.options == {"opt": True}
    assert isinstance(t.functions, list)


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="transform_spark")
def fixture_transform_spark(transform_config: dict[str, Any]) -> TransformSpark:
    """Instantiate a TransformSpark object from the provided config dict (model fixture)."""
    return TransformSpark(**transform_config)


def test_transform_properties__after_creation__match_expected_values(transform_spark: TransformSpark) -> None:
    """Validate properties on the TransformSpark object fixture in the expected order."""
    assert transform_spark.name == "tr_conf"
    assert transform_spark.upstream_name == "ex_src"
    assert transform_spark.options == {"opt": True}
    assert isinstance(transform_spark.functions, list)


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #
