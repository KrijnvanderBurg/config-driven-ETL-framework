"""Unit tests for the TransformSpark model."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import ExitStack
from typing import Any

import pytest
from pydantic import ValidationError

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


class TestTransformSparkValidation:
    """Test TransformSpark model validation and instantiation."""

    def test_transform_creation__from_config__creates_valid_model(self, transform_config: dict[str, Any]) -> None:
        """Ensure a TransformSpark can be created from a plain config dict and all fields are set in the expected order."""
        t = TransformSpark(**transform_config)
        # Assert fields in order: name, upstream_name, options, functions
        assert t.name == "tr_conf"
        assert t.upstream_name == "ex_src"
        assert t.options == {"opt": True}
        assert isinstance(t.functions, list)

    def test_transform_creation__with_empty_functions__creates_valid_model(
        self, transform_config: dict[str, Any]
    ) -> None:
        """Ensure a TransformSpark can be created with an empty functions list."""
        transform_config["functions"] = []
        t = TransformSpark(**transform_config)
        assert t.name == "tr_conf"
        assert t.upstream_name == "ex_src"
        assert t.options == {"opt": True}
        assert isinstance(t.functions, list)
        assert len(t.functions) == 0

    def test_transform_creation__with_missing_functions__raises_validation_error(
        self, transform_config: dict[str, Any]
    ) -> None:
        """Test TransformSpark creation fails without functions field."""
        del transform_config["functions"]

        with pytest.raises(ValidationError):
            TransformSpark(**transform_config)

    def test_transform_creation__with_missing_name__raises_validation_error(
        self, transform_config: dict[str, Any]
    ) -> None:
        """Test TransformSpark creation fails without name field."""
        del transform_config["name"]

        with pytest.raises(ValidationError):
            TransformSpark(**transform_config)

    def test_transform_creation__with_empty_name__raises_validation_error(
        self, transform_config: dict[str, Any]
    ) -> None:
        """Test TransformSpark creation fails with empty name."""
        transform_config["name"] = ""

        with pytest.raises(ValidationError):
            TransformSpark(**transform_config)

    def test_transform_creation__with_missing_upstream_name__raises_validation_error(
        self, transform_config: dict[str, Any]
    ) -> None:
        """Test TransformSpark creation fails when upstream_name is missing."""
        del transform_config["upstream_name"]

        with pytest.raises(ValidationError):
            TransformSpark(**transform_config)

    def test_transform_creation__with_empty_upstream_name__succeeds(self, transform_config: dict[str, Any]) -> None:
        """Test TransformSpark creation succeeds with empty upstream_name (no min_length set)."""
        transform_config["upstream_name"] = ""

        t = TransformSpark(**transform_config)
        assert t.upstream_name == ""

    def test_transform_creation__with_missing_options__raises_validation_error(
        self, transform_config: dict[str, Any]
    ) -> None:
        """Test TransformSpark creation fails when options is missing."""
        del transform_config["options"]

        with pytest.raises(ValidationError):
            TransformSpark(**transform_config)

    def test_transform_creation__with_empty_options__succeeds(self, transform_config: dict[str, Any]) -> None:
        """Test TransformSpark creation with empty options dict succeeds."""
        transform_config["options"] = {}

        t = TransformSpark(**transform_config)
        assert t.options == {}

    def test_transform_creation__with_invalid_functions_type__raises_validation_error(
        self, transform_config: dict[str, Any]
    ) -> None:
        """Test TransformSpark creation fails when functions is not a list."""
        transform_config["functions"] = "not-a-list"

        with pytest.raises(ValidationError):
            TransformSpark(**transform_config)


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
