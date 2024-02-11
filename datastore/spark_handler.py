"""
Module to take care of creating a singleton of the execution environment class.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from datastore.logger import set_logger
from pyspark.sql import SparkSession

logger = set_logger(__name__)


class SparkHandler:
    """Spark session logic."""

    session: SparkSession

    @classmethod
    def get_or_create(
        cls,
        session: SparkSession | None = None,
    ) -> SparkSession:
        """
        Get or create an execution environment session (currently Spark).

        Args:
            session: spark session.

        Returns:
            SparSession: Spark session.
        """

        app_name = "datastore"

        if session:
            cls.session = session
        else:
            # get application name of active session
            if active_session := SparkSession.getActiveSession():
                app_name = active_session.sparkContext.appName or "datastore"

            session_builder = SparkSession.builder.appName(app_name)
            cls.session = session_builder.getOrCreate()

        return cls.session
