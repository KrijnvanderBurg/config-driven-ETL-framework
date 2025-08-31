"""Sensor module for configuration-driven sensor management.

This module provides the sensor system for the Flint framework, enabling
configuration-driven monitoring and triggering of actions based on various
conditions like file system changes and HTTP polling.

The sensor system supports:
- Cron-like scheduling for sensor execution
- File system watchers for monitoring directory changes
- HTTP polling watchers for monitoring API endpoints
- Configurable actions that can be triggered by watchers
- Flexible trigger conditions for different watcher types

Main Classes:
    SensorController: Root controller class for sensor configurations
    SensorSchedule: Cron-like scheduling configuration
    Watcher: Individual watcher configurations
    SensorAction: Action configurations for triggering responses
"""

from flint.sensor.actions import SensorAction
from flint.sensor.controller import SensorController
from flint.sensor.schedule import SensorSchedule
from flint.sensor.watchers import Watcher

__all__ = [
    "SensorController",
    "SensorSchedule",
    "Watcher",
    "SensorAction",
]
