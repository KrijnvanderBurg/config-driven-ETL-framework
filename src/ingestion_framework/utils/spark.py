"""
Module for managing SparkSession as a singleton to ensure a single execution environment.
"""

import logging

from pyspark.sql import SparkSession

from ingestion_framework.types import Singleton
from ingestion_framework.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class SparkHandler(metaclass=Singleton):
    """
    Singleton handler for SparkSession to ensure only one active session.
    """

    _session: SparkSession

    def __init__(
        self,
        app_name: str = "ingestion_framework",
        options: dict[str, str] | None = None,
    ) -> None:
        """
        Initialize the SparkHandler with app name and configuration options.

        Args:
            app_name (str): Name of the Spark application.
            options (dict[str, str] | None): Optional Spark configuration options.
        """
        builder = SparkSession.Builder().appName(name=app_name)

        if options:
            for key, value in options.items():
                builder = builder.config(key=key, value=value)

        self.session = builder.getOrCreate()

    @property
    def session(self) -> SparkSession:
        """
        Get the current SparkSession.

        Returns:
            SparkSession: The current SparkSession instance.
        """
        return self._session

    @session.setter
    def session(self, session: SparkSession) -> None:
        """
        Set the SparkSession.

        Args:
            session (SparkSession): The SparkSession to use.
        """
        self._session = session

    @session.deleter
    def session(self) -> None:
        """
        Stop and delete the current SparkSession.
        """
        self._session.stop()
        del self._session

    def add_configs(self, options: dict[str, str]) -> None:
        """
        Add configuration options to the active SparkSession.

        Args:
            options (dict[str, str]): Configuration key-value pairs.
        """
        for key, value in options.items():
            self.session.conf.set(key=key, value=value)
