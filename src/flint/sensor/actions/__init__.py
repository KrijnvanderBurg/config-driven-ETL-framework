"""Actions submodule for sensor management.

This module provides different types of actions that can be triggered
by sensor watchers when their conditions are met.
"""

from flint.sensor.action import SensorAction
from flint.sensor.actions.http_post import HttpPostAction
from flint.sensor.actions.trigger_job import TriggerJobAction

__all__ = [
    "SensorAction",
    "HttpPostAction",
    "TriggerJobAction",
]
