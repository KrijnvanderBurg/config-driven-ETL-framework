"""
Module to take care of creating a singleton of the execution environment class.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED 
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""
from pyspark.sql import SparkSession

import logger

logger = logger.set_logger(__name__)


class Spark(object):
    """Spark session logic."""

    session: SparkSession

    @classmethod
    def get_or_create(
        cls,
        session: SparkSession = None,
    ) -> None:
        """
        Get or create an execution environment session (currently Spark).

        Args:
            session: spark session.
        """

        if session:
            cls.session = session
        else:
            # get application name of active session
            if SparkSession.getActiveSession():
                app_name = SparkSession.getActiveSession().sparkContext.appName

            elif not SparkSession.getActiveSession() and not app_name:
                app_name = "datastore"

            session_builder = SparkSession.builder.appName(app_name)
            cls.session = session_builder.getOrCreate()
