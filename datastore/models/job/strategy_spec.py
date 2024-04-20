"""
This module defines abstract classes for defining job strategies and their specifications.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from abc import ABC
from enum import Enum
from typing import Self

from datastore.utils.json_handler import JsonHandler


class StrategyEngine(Enum):
    """Enumeration for job engines."""

    PYSPARK = "pyspark"


class StrategySpecAbstract(ABC):
    """
    Abstract base class for defining job strategy specifications.

    Args:
        engine (str): The type of job engine.
    """

    confeti_schema: dict = {
        "type": "object",
        "properties": {
            "engine": {
                "type": "string",
                "enum": ["pyspark"],
            },
            "options": {"type": "object"},
        },
        "required": ["engine"],
    }

    def __init__(self, engine: str) -> None:
        """
        Initialize a job strategy specification.

        Args:
            engine (str): The type of job engine.
        """
        self.engine: StrategyEngine = StrategyEngine(value=engine)

    @classmethod
    def from_confeti(cls, confeti: dict) -> Self:
        """
        Create a job strategy specification from a confeti dictionary.

        Args:
            confeti (dict): The confeti dictionary containing the job strategy specification.

        Returns:
            StrategySpecAbstract: An instance of the job strategy specification.
        """
        JsonHandler.validate_json(json=confeti, schema=cls.confeti_schema)
        return cls(**confeti)


class StrategySpecPyspark(StrategySpecAbstract):
    """
    Job strategy specification for PySpark.

    Args:
        engine (str): The type of job engine, must be 'pyspark'.
        options (dict): Additional options for configuring the PySpark job strategy.

    Example:
        >>> confeti_dict = {
        >>>     "engine": "pyspark",
        >>>     "options": {},
        >>> }
        >>> strategy_spec = StrategySpecPyspark.from_confeti(confeti_dict)
    """

    def __init__(self, engine: str, options: dict | None = None):
        """
        Initialize a PySpark job strategy specification.

        Args:
            engine (str): The type of job engine, must be 'pyspark'.
            options (dict, optional): Additional options for configuring the PySpark job strategy.
        """
        self.options: dict = options if options is not None else {}
        super().__init__(engine=engine)
