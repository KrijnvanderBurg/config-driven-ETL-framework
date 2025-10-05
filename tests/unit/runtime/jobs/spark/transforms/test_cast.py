"""Tests for Cast transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import pytest
from pydantic import ValidationError

from flint.runtime.jobs.models.transforms.model_cast import CastArgs
from flint.runtime.jobs.spark.transforms.cast import CastFunction

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="cast_config")
def fixture_cast_config() -> dict[str, Any]:
    """Configuration dict for CastFunction."""
    return {"function_type": "cast", "arguments": {"columns": [{"column_name": "age", "cast_type": "int"}]}}


def test_cast_creation__from_config__creates_valid_model(cast_config: dict[str, Any]) -> None:
    """Ensure the CastFunction can be created from a config dict."""
    f = CastFunction(**cast_config)
    assert f.function_type == "cast"
    assert isinstance(f.arguments, CastArgs)
    assert len(f.arguments.columns) == 1
    assert f.arguments.columns[0].column_name == "age"
    assert f.arguments.columns[0].cast_type == "int"


# =========================================================================== #
# ========================== VALIDATION TESTS ============================= #
# =========================================================================== #


class TestCastFunctionValidation:
    """Test CastFunction model validation and instantiation."""

    def test_create_cast_function__with_missing_function__raises_validation_error(
        self, cast_config: dict[str, Any]
    ) -> None:
        """Test CastFunction creation fails without function field."""
        del cast_config["function_type"]

        with pytest.raises(ValidationError):
            CastFunction(**cast_config)

    def test_create_cast_function__with_wrong_function_name__raises_validation_error(
        self, cast_config: dict[str, Any]
    ) -> None:
        """Test CastFunction creation fails with wrong function name."""
        cast_config["function_type"] = "wrong_name"

        with pytest.raises(ValidationError):
            CastFunction(**cast_config)

    def test_create_cast_function__with_missing_arguments__raises_validation_error(
        self, cast_config: dict[str, Any]
    ) -> None:
        """Test CastFunction creation fails without arguments field."""
        del cast_config["arguments"]

        with pytest.raises(ValidationError):
            CastFunction(**cast_config)

    def test_create_cast_function__with_empty_columns__succeeds(self, cast_config: dict[str, Any]) -> None:
        """Test CastFunction creation succeeds with empty columns list."""
        cast_config["arguments"]["columns"] = []

        # Act
        cast_function = CastFunction(**cast_config)

        # Assert
        assert cast_function.arguments.columns == []

    def test_create_cast_function__with_missing_column_name__raises_validation_error(
        self, cast_config: dict[str, Any]
    ) -> None:
        """Test CastFunction creation fails with missing column_name."""
        del cast_config["arguments"]["columns"][0]["column_name"]

        with pytest.raises(ValidationError):
            CastFunction(**cast_config)

    def test_create_cast_function__with_missing_cast_type__raises_validation_error(
        self, cast_config: dict[str, Any]
    ) -> None:
        """Test CastFunction creation fails with missing cast_type."""
        del cast_config["arguments"]["columns"][0]["cast_type"]

        with pytest.raises(ValidationError):
            CastFunction(**cast_config)


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
    assert cast_func.function_type == "cast"
    assert isinstance(cast_func.arguments, CastArgs)
    assert cast_func.arguments.columns[0].column_name == "age"
    assert cast_func.arguments.columns[0].cast_type == "int"


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #


class TestCastFunctionTransform:
    """Test CastFunction transform behavior."""

    def test_transform__returns_callable(self, cast_func: CastFunction) -> None:
        """Test transform returns a callable function."""
        # Act
        transform_fn = cast_func.transform()

        # Assert
        assert callable(transform_fn)

    def test_transform__applies_cast_to_column(self, cast_func: CastFunction) -> None:
        """Test transform applies cast operation to specified column."""

        # Arrange
        mock_df = Mock()
        mock_df.withColumn.return_value = mock_df

        # Act
        transform_fn = cast_func.transform()
        transform_fn(mock_df)

        # Assert
        assert mock_df.withColumn.called
        call_args = mock_df.withColumn.call_args[0]
        assert call_args[0] == "age"
