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

from flint.types import Singleton
from flint.utils.logger import get_logger

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
        app_name: str = "flint",
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
        logger.debug("Initializing SparkHandler with app_name: %s", app_name)

        builder = SparkSession.Builder().appName(name=app_name)

        if options:
            for key, value in options.items():
                logger.debug("Setting Spark config: %s = %s", key, value)
                builder = builder.config(key=key, value=value)

        logger.debug("Creating/retrieving SparkSession")
        self.session = builder.getOrCreate()
        logger.info("SparkHandler initialized successfully with app: %s", app_name)

    @property
    def session(self) -> SparkSession:
        """Get the current managed SparkSession instance.

        Provides access to the singleton SparkSession instance managed by this handler.
        This is the main entry point for all Spark operations.

        Returns:
            The current active SparkSession instance
        """
        logger.debug("Accessing SparkSession instance")
        return self._session

    @session.setter
    def session(self, session: SparkSession) -> None:
        """Set the managed SparkSession instance.

        Updates the internal reference to the SparkSession instance.
        This is typically only used internally during initialization.

        Args:
            session: The SparkSession instance to use
        """
        logger.debug(
            "Setting SparkSession instance - app name: %s, version: %s", session.sparkContext.appName, session.version
        )
        self._session = session

    @session.deleter
    def session(self) -> None:
        """Stop and delete the current SparkSession.

        Properly terminates the SparkSession and removes the internal reference.
        This ensures that all Spark resources are released cleanly.

        This should be called when the SparkSession is no longer needed,
        typically at the end of the application lifecycle.
        """
        logger.info("Stopping SparkSession: %s", self._session.sparkContext.appName)
        self._session.stop()
        del self._session
        logger.info("SparkSession stopped and cleaned up successfully")

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
        logger.debug("Adding %d configuration options to SparkSession", len(options))

        for key, value in options.items():
            logger.debug("Setting runtime config: %s = %s", key, value)
            self.session.conf.set(key=key, value=value)

        logger.info("Successfully applied %d configuration options", len(options))
