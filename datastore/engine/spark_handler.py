"""
Module to take care of creating a singleton of the execution environment class.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from pyspark.sql import SparkSession

from datastore.logger import set_logger

logger = set_logger(__name__)


class SparkHandler:
    """Spark session logic."""

    session: SparkSession

    @classmethod
    def get_or_create(
        cls,
        session: SparkSession | None = None,
        app_name: str = "datastore",
        config: dict | None = None,
    ) -> SparkSession:
        """
        Get or create a Spark session.

        Args:
            session (SparkSession, optional): An existing Spark session.
            app_name (str, optional): Name of the Spark application. Defaults to "datastore".
            config (dict, optional): Configuration options for the Spark session.

        Returns:
            SparkSession: The Spark session.
        """
        if session:
            logger.info("Using existing Spark session: %s.", session.builder.appName)
            cls.session = session
            return cls.session

        logger.info("Creating new Spark session with name: %s.", app_name)
        builder = SparkSession.builder.appName(app_name)  # type: ignore

        # Apply additional configuration if provided
        if config:
            for key, value in config.items():
                builder.config(key, value)

        cls.session = builder.getOrCreate()
        logger.info("New Spark session created.")
        return cls.session
