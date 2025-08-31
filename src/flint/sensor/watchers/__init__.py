"""Watchers submodule for sensor management.

This module provides different types of watchers that can monitor
various conditions and trigger actions when those conditions are met.
"""

from flint.sensor.watchers.base import BaseWatcher, Watcher
from flint.sensor.watchers.file_system import FileSystemWatcher
from flint.sensor.watchers.http_polling import HttpPollingWatcher

__all__ = [
    "BaseWatcher",
    "Watcher",
    "FileSystemWatcher",
    "HttpPollingWatcher",
]