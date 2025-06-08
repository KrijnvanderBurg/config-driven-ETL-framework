"""
Module to take care of creating a singleton of the execution environment class.


==============================================================================
Copyright Krijn van der Burg. All rights reserved.

This software is proprietary and confidential. No reproduction, distribution,
or transmission is allowed without prior written permission. Unauthorized use,
disclosure, or distribution is strictly prohibited.

For inquiries and permission requests, contact Krijn van der Burg at
krijnvdburg@protonmail.com.
==============================================================================
"""

import logging

from pyspark.sql import SparkSession

from stratum.utils.log_handler import set_logger
from stratum.utils.singleton_meta import SingletonType

logger: logging.Logger = set_logger(__name__)


class SparkHandler(metaclass=SingletonType):
    # _session: SparkSession

    def __init__(self, app_name: str = "stratum", options: dict[str, str] | None = None) -> None:
        builder = SparkSession.Builder().appName(name=app_name)

        if options:
            for key, value in options.items():
                builder = builder.config(key=key, value=value)

        self.session = builder.getOrCreate()

    @property
    def session(self) -> SparkSession:
        return self._session

    @session.setter
    def session(self, session: SparkSession) -> None:
        self._session = session

    @session.deleter
    def session(self) -> None:
        self._session.stop()
        del self._session

    def add_configs(self, options: dict[str, str]) -> None:
        for key, value in options.items():
            self.session.conf.set(key=key, value=value)
