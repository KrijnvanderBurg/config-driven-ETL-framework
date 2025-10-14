"""Tests for WithColumn transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import pytest
from samara.runtime.jobs.models.transforms.model_withcolumn import WithColumnArgs
from samara.runtime.jobs.spark.transforms.withcolumn import WithColumnFunction

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="withcolumn_config")
def fixture_withcolumn_config() -> dict[str, Any]:
    """Return a config dict for WithColumnFunction."""
    return {
        "function_type": "withColumn",
        "arguments": {"col_name": "full_name", "col_expr": "concat(first_name, ' ', last_name)"},
    }


def test_withcolumn_creation__from_config__creates_valid_model(withcolumn_config: dict[str, Any]) -> None:
    """Instantiate from config only to test dict-based initialization."""
    f = WithColumnFunction(**withcolumn_config)
    assert f.function_type == "withColumn"
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
    assert withcolumn_func.function_type == "withColumn"
    assert isinstance(withcolumn_func.arguments, WithColumnArgs)
    assert withcolumn_func.arguments.col_name == "full_name"
    assert withcolumn_func.arguments.col_expr.startswith("concat")


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #


class TestWithColumnFunctionTransform:
    """Test WithColumnFunction transform behavior."""

    def test_transform__returns_callable(self, withcolumn_func: WithColumnFunction) -> None:
        """Test transform returns a callable function."""
        # Act
        transform_fn = withcolumn_func.transform()

        # Assert
        assert callable(transform_fn)

    def test_transform__applies_withcolumn_operation(self, withcolumn_func: WithColumnFunction) -> None:
        """Test transform applies withColumn with correct parameters."""

        # Arrange
        mock_df = Mock()
        mock_df.withColumn.return_value = mock_df

        # Act
        transform_fn = withcolumn_func.transform()
        transform_fn(mock_df)

        # Assert
        assert mock_df.withColumn.called
        call_args = mock_df.withColumn.call_args[0]
        assert call_args[0] == "full_name"
