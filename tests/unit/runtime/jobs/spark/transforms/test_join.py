"""Tests for Join transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import pytest

from flint.runtime.jobs.models.transforms.model_join import JoinArgs
from flint.runtime.jobs.spark.transforms.join import JoinFunction

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="join_config")
def fixture_join_config() -> dict[str, Any]:
    """Return a config dict for JoinFunction."""
    return {"function": "join", "arguments": {"other_upstream_name": "other_df", "on": "id", "how": "inner"}}


def test_join_creation__from_config__creates_valid_model(join_config: dict[str, Any]) -> None:
    """Instantiate from config only to test dict-based initialization."""
    f = JoinFunction(**join_config)
    assert f.function == "join"
    assert isinstance(f.arguments, JoinArgs)
    assert f.arguments.other_upstream_name == "other_df"
    assert f.arguments.on == "id"
    assert f.arguments.how == "inner"


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="join_func")
def fixture_join_func(join_config: dict[str, Any]) -> JoinFunction:
    """Instantiate a JoinFunction from the config dict."""
    return JoinFunction(**join_config)


def test_join_fixture__args(join_func: JoinFunction) -> None:
    """Assert the instantiated fixture has expected join arguments."""
    assert join_func.function == "join"
    assert isinstance(join_func.arguments, JoinArgs)
    assert join_func.arguments.other_upstream_name == "other_df"
    assert join_func.arguments.on == "id"
    assert join_func.arguments.how == "inner"


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #


class TestJoinFunctionTransform:
    """Test JoinFunction transform behavior."""

    def test_transform__returns_callable(self, join_func: JoinFunction) -> None:
        """Test transform returns a callable function."""
        # Act
        transform_fn = join_func.transform()

        # Assert
        assert callable(transform_fn)

    def test_transform__applies_join_operation(self, join_func: JoinFunction) -> None:
        """Test transform applies join with correct parameters."""

        # Arrange
        mock_df = Mock()
        mock_other_df = Mock()
        mock_df.join.return_value = mock_df
        mock_df.count.return_value = 100  # Mock count for logging
        mock_other_df.count.return_value = 50  # Mock count for logging
        mock_other_df.columns = ["id", "city"]  # Mock columns for logging

        JoinFunction.data_registry["other_df"] = mock_other_df

        # Act
        transform_fn = join_func.transform()
        transform_fn(mock_df)

        # Assert
        mock_df.join.assert_called_once_with(mock_other_df, on="id", how="inner")
