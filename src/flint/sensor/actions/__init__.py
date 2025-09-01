"""Actions submodule for sensor management.

This module provides different types of actions that can be triggered
by sensor watchers when their conditions are met.
"""

from flint.sensor.actions.base import BaseAction, SensorAction
from flint.sensor.actions.etl_in_sensor import EtlInSensor
from flint.sensor.actions.http_post import HttpPostAction

__all__ = [
    "BaseAction",
    "SensorAction",
    "HttpPostAction",
    "EtlInSensor",
]
