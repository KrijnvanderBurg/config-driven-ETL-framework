"""File system watcher configuration for sensor management.

This module provides dataclasses for file system watcher configurations including
location settings, file patterns, and trigger conditions.
It follows the from_dict pattern consistent with other components in the Flint framework.
"""

import logging
from dataclasses import dataclass
from typing import Any, Final, Self

from flint.exceptions import FlintConfigurationKeyError
from flint.job.models import Model
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

# Location constants
PATH: Final[str] = "path"
RECURSIVE: Final[str] = "recursive"


@dataclass
class WatcherLocation(Model):
    """Configuration for watcher location settings.

    This class manages location configuration for file system watchers,
    defining where to watch for changes.

    Attributes:
        path: File system path to watch
        recursive: Whether to watch subdirectories recursively
    """

    path: str
    recursive: bool

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a WatcherLocation instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing location configuration with keys:
                  - path: File system path to watch
                  - recursive: Whether to watch recursively

        Returns:
            A WatcherLocation instance configured from the dictionary

        Raises:
            FlintConfigurationKeyError: If a required key is missing.

        Examples:
            >>> config = {
            ...     "path": "incoming/daily/",
            ...     "recursive": True
            ... }
            >>> location = WatcherLocation.from_dict(config)
        """
        logger.debug("Creating WatcherLocation from configuration dictionary")

        try:
            path = dict_[PATH]
            recursive = dict_[RECURSIVE]
        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            path=path,
            recursive=recursive,
        )


# File patterns constants
INCLUDE_FNMATCH_PATTERNS: Final[str] = "include_fnmatch_patterns"
EXCLUDE_FNMATCH_PATTERNS: Final[str] = "exclude_fnmatch_patterns"


@dataclass
class FilePatterns(Model):
    """Configuration for file pattern matching.

    This class manages file pattern configuration for filtering
    which files should trigger watcher actions.

    Attributes:
        include_fnmatch_patterns: List of patterns to include
        exclude_fnmatch_patterns: List of patterns to exclude
    """

    include_fnmatch_patterns: list[str]
    exclude_fnmatch_patterns: list[str]

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a FilePatterns instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing pattern configuration with keys:
                  - include_fnmatch_patterns: Patterns to include
                  - exclude_fnmatch_patterns: Patterns to exclude

        Returns:
            A FilePatterns instance configured from the dictionary

        Raises:
            FlintConfigurationKeyError: If a required key is missing.

        Examples:
            >>> config = {
            ...     "include_fnmatch_patterns": ["*.csv", "*.json"],
            ...     "exclude_fnmatch_patterns": [".*", "*.tmp"]
            ... }
            >>> patterns = FilePatterns.from_dict(config)
        """
        logger.debug("Creating FilePatterns from configuration dictionary")

        try:
            include_fnmatch_patterns = dict_[INCLUDE_FNMATCH_PATTERNS]
            exclude_fnmatch_patterns = dict_[EXCLUDE_FNMATCH_PATTERNS]
        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            include_fnmatch_patterns=include_fnmatch_patterns,
            exclude_fnmatch_patterns=exclude_fnmatch_patterns,
        )


# File system trigger conditions constants
MINIMUM_FILE_COUNT: Final[str] = "minimum_file_count"
MINIMUM_TOTAL_SIZE_MB: Final[str] = "minimum_total_size_mb"
MAXIMUM_FILE_AGE_HOURS: Final[str] = "maximum_file_age_hours"


@dataclass
class FileSystemTriggerConditions(Model):
    """Configuration for file system trigger conditions.

    This class manages trigger conditions for file system watchers,
    defining when actions should be triggered based on file metrics.

    Attributes:
        minimum_file_count: Minimum number of files required
        minimum_total_size_mb: Minimum total size in MB
        maximum_file_age_hours: Maximum file age in hours
    """

    minimum_file_count: int
    minimum_total_size_mb: int
    maximum_file_age_hours: int

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a FileSystemTriggerConditions instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing trigger conditions with keys:
                  - minimum_file_count: Minimum file count
                  - minimum_total_size_mb: Minimum total size
                  - maximum_file_age_hours: Maximum file age

        Returns:
            A FileSystemTriggerConditions instance configured from the dictionary

        Raises:
            FlintConfigurationKeyError: If a required key is missing.

        Examples:
            >>> config = {
            ...     "minimum_file_count": 5,
            ...     "minimum_total_size_mb": 100,
            ...     "maximum_file_age_hours": 1
            ... }
            >>> conditions = FileSystemTriggerConditions.from_dict(config)
        """
        logger.debug("Creating FileSystemTriggerConditions from configuration dictionary")

        try:
            minimum_file_count = dict_[MINIMUM_FILE_COUNT]
            minimum_total_size_mb = dict_[MINIMUM_TOTAL_SIZE_MB]
            maximum_file_age_hours = dict_[MAXIMUM_FILE_AGE_HOURS]
        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            minimum_file_count=minimum_file_count,
            minimum_total_size_mb=minimum_total_size_mb,
            maximum_file_age_hours=maximum_file_age_hours,
        )


# Config sub-constants
LOCATION: Final[str] = "location"
FILE_PATTERNS: Final[str] = "file_patterns"
TRIGGER_CONDITIONS: Final[str] = "trigger_conditions"


@dataclass
class FileSystemWatcher(Model):
    """Configuration for file system watchers.

    This class manages complete file system watcher configuration including
    location, file patterns, and trigger conditions.

    Attributes:
        location: Location configuration for watching
        file_patterns: File pattern matching configuration
        trigger_conditions: Conditions for triggering actions
    """

    location: WatcherLocation
    file_patterns: FilePatterns
    trigger_conditions: FileSystemTriggerConditions

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a FileSystemWatcher instance from a dictionary configuration.

        Args:
            dict_: Dictionary containing file system watcher configuration with keys:
                  - location: Location configuration
                  - file_patterns: File pattern configuration
                  - trigger_conditions: Trigger condition configuration

        Returns:
            A FileSystemWatcher instance configured from the dictionary

        Raises:
            FlintConfigurationKeyError: If a required key is missing.

        Examples:
            >>> config = {
            ...     "location": {...},
            ...     "file_patterns": {...},
            ...     "trigger_conditions": {...}
            ... }
            >>> watcher = FileSystemWatcher.from_dict(config)
        """
        logger.debug("Creating FileSystemWatcher from configuration dictionary")

        try:
            location = WatcherLocation.from_dict(dict_[LOCATION])
            file_patterns = FilePatterns.from_dict(dict_[FILE_PATTERNS])
            trigger_conditions = FileSystemTriggerConditions.from_dict(dict_[TRIGGER_CONDITIONS])
        except KeyError as e:
            raise FlintConfigurationKeyError(key=e.args[0], dict_=dict_) from e

        return cls(
            location=location,
            file_patterns=file_patterns,
            trigger_conditions=trigger_conditions,
        )
