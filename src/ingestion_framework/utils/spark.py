"""PySpark session management utilities.

This module provides a singleton implementation of SparkSession management to ensure
that only one active Spark context exists within the application. It includes:

- SparkHandler class for creating and accessing a shared SparkSession
- Utility functions for configuring Spark with sensible defaults
- Helper methods for common Spark operations

The singleton pattern ensures resource efficiency and prevents issues that can
arise from multiple concurrent Spark contexts.
"""

import logging

from pyspark.sql import SparkSession

from ingestion_framework.types import Singleton
from ingestion_framework.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class SparkHandler(metaclass=Singleton):
    """Singleton handler for SparkSession management.

    Ensures that only one SparkSession is active throughout the application
    lifecycle, preventing resource conflicts and improving performance.

    This class uses the Singleton metaclass to ensure that only one instance
    is created regardless of how many times it's initialized.

    Attributes:
        _session: The managed PySpark SparkSession instance
    """

    _session: SparkSession

    def __init__(
        self,
        app_name: str = "ingestion_framework",
        options: dict[str, str] | None = None,
    ) -> None:
        """Initialize the SparkHandler with app name and configuration options.

        Creates a new SparkSession with the specified application name and
        configuration options. If a SparkSession already exists, it will be
        reused due to the singleton pattern.

        Args:
            app_name: Name of the Spark application, used for tracking and monitoring
            options: Optional dictionary of Spark configuration options as key-value pairs
        """
        builder = SparkSession.Builder().appName(name=app_name)

        if options:
            for key, value in options.items():
                builder = builder.config(key=key, value=value)

        self.session = builder.getOrCreate()

    @property
    def session(self) -> SparkSession:
        """Get the current managed SparkSession instance.

        Provides access to the singleton SparkSession instance managed by this handler.
        This is the main entry point for all Spark operations.

        Returns:
            The current active SparkSession instance
        """
        return self._session

    @session.setter
    def session(self, session: SparkSession) -> None:
        """Set the managed SparkSession instance.

        Updates the internal reference to the SparkSession instance.
        This is typically only used internally during initialization.

        Args:
            session: The SparkSession instance to use
        """
        self._session = session

    @session.deleter
    def session(self) -> None:
        """Stop and delete the current SparkSession.

        Properly terminates the SparkSession and removes the internal reference.
        This ensures that all Spark resources are released cleanly.

        This should be called when the SparkSession is no longer needed,
        typically at the end of the application lifecycle.
        """
        self._session.stop()
        del self._session

    def add_configs(self, options: dict[str, str]) -> None:
        """Add configuration options to the active SparkSession.

        Updates the configuration of the current SparkSession with new options.
        This can be used to modify Spark behavior at runtime, although not all
        configuration options can be changed after the session is created.

        Args:
            options: Dictionary of configuration key-value pairs to apply

        Note:
            Some Spark configurations can only be set during initialization
            and cannot be changed using this method after the SparkSession
            has been created.
        """
        for key, value in options.items():
            self.session.conf.set(key=key, value=value)
