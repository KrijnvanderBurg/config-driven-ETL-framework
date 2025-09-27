"""Unit tests for the LoadFileSpark model.

These tests follow the canonical pattern used across the Spark test suite:
CONFIG (dict) fixture → creation-from-config test (immediately after) →
MODEL fixture → fixture-based assertions. Temporary files are used for
path-like fields and no Spark cluster is started.
"""

from __future__ import annotations

import tempfile
from collections.abc import Generator
from contextlib import ExitStack
from typing import Any

import pytest

from flint.runtime.jobs.models.model_load import LoadFormat, LoadMethod, LoadMode
from flint.runtime.jobs.spark.load import LoadFileSpark

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="load_config")
def fixture_load_config() -> Generator[dict[str, Any], Any, None]:
    """Provide a load config dict with a real tempfile for the location."""
    stack = ExitStack()
    temp_out = stack.enter_context(tempfile.NamedTemporaryFile(suffix=".json", mode="w+b"))
    # write minimal JSON so handlers/readers won't fail on empty files
    temp_out.write(b"[]")
    temp_out.flush()

    data = {
        "name": "load_dict",
        "upstream_name": "transform_obj",
        "method": "batch",
        "location": temp_out.name,
        "schema_location": None,
        "options": {},
        "mode": "append",
        "data_format": "json",
    }

    yield data
    stack.close()


def test_load_creation__from_config__creates_valid_model(load_config: dict[str, Any]) -> None:
    """Create a LoadFileSpark from the config dict and assert top-level fields in order."""
    load_model = LoadFileSpark(**load_config)

    # Assert fields from config in the order: name, upstream_name, method, location,
    # schema_location, options, mode, data_format
    assert load_model.name == "load_dict"
    assert load_model.upstream_name == "transform_obj"
    assert load_model.method == LoadMethod.BATCH
    assert isinstance(load_model.location, str) and load_model.location.endswith(".json")
    assert load_model.schema_location is None
    assert load_model.options == {}
    assert load_model.mode == LoadMode.APPEND
    assert load_model.data_format == LoadFormat.JSON


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="load_spark")
def fixture_load_spark(load_config: dict[str, Any]) -> LoadFileSpark:
    """Instantiate a LoadFileSpark object from the provided config dict."""
    return LoadFileSpark(**load_config)


def test_load_properties__after_creation__match_expected_values(load_spark: LoadFileSpark) -> None:
    """Validate properties on the LoadFileSpark object fixture in the expected order."""
    # Assert object fixture attributes in same order as config
    assert load_spark.name == "load_dict"
    assert load_spark.upstream_name == "transform_obj"
    assert load_spark.method == LoadMethod.BATCH
    assert isinstance(load_spark.location, str) and load_spark.location.endswith(".json")
    assert load_spark.schema_location is None
    assert load_spark.options == {}
    assert load_spark.mode == LoadMode.APPEND
    assert load_spark.data_format == LoadFormat.JSON


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #
