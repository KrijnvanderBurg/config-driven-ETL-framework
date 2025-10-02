"""Unit tests for the Hooks controller."""

from unittest.mock import Mock

import pytest

from flint.actions.base import ActionBase
from flint.runtime.jobs.hooks import Hooks


def test_hooks_initializes_with_empty_lists() -> None:
    """Test that Hooks initializes with empty action lists by default."""
    hooks = Hooks()

    assert hooks.onStart == []
    assert hooks.onError == []
    assert hooks.onSuccess == []
    assert hooks.onFinally == []


def test_on_start_executes_action() -> None:
    """Test that on_start executes the action."""
    action = Mock(spec=ActionBase)
    hooks = Hooks(onStart=[action])

    hooks.on_start()

    action.execute.assert_called_once()


def test_on_error_executes_action() -> None:
    """Test that on_error executes the action."""
    action = Mock(spec=ActionBase)
    hooks = Hooks(onError=[action])

    hooks.on_error()

    action.execute.assert_called_once()


def test_on_success_executes_action() -> None:
    """Test that on_success executes the action."""
    action = Mock(spec=ActionBase)
    hooks = Hooks(onSuccess=[action])

    hooks.on_success()

    action.execute.assert_called_once()


def test_on_finally_executes_action() -> None:
    """Test that on_finally executes the action."""
    action = Mock(spec=ActionBase)
    hooks = Hooks(onFinally=[action])

    hooks.on_finally()

    action.execute.assert_called_once()


def test_hooks_only_execute_their_own_actions() -> None:
    """Test that each hook type only executes its own actions."""
    start_action = Mock(spec=ActionBase)
    error_action = Mock(spec=ActionBase)

    hooks = Hooks(onStart=[start_action], onError=[error_action])
    hooks.on_start()

    start_action.execute.assert_called_once()
    error_action.execute.assert_not_called()


def test_hooks_propagate_exceptions() -> None:
    """Test that hooks propagate exceptions from failing actions."""
    action = Mock(spec=ActionBase)
    action.execute.side_effect = RuntimeError("Action failed")

    hooks = Hooks(onStart=[action])

    with pytest.raises(RuntimeError):
        hooks.on_start()
