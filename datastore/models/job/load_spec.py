"""
Data Load Module.

This module defines an abstract class `Load` along with supporting enumerations for configuring data load operations.
It provides data writing, allowing customization through implementing `Load` abstract methods.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from abc import ABC
from enum import Enum

from datastore.utils.json_handler import JsonHandler


class LoadMethod(Enum):
    """Enumeration for methods of load modes."""

    BATCH = "batch"
    STREAMING = "streaming"


class LoadOperation(Enum):
    """Enumeration for methods of load operations."""

    COMPLETE = "complete"
    APPEND = "append"
    UPDATE = "update"


class LoadFormat(Enum):
    """Enumeration for methods of input and structures for the load."""

    PARQUET = "parquet"
    JSON = "json"
    CSV = "csv"


class LoadSpecAbstract(ABC):
    """
    Specification of the sink input.

    Args:
        name (str): ID of the sink specification.
        method (str): Type of sink load mode.
        operation (str): Type of sink operation.
        data_format (str): Format of the sink input.
        location (str): URI that identifies where to load data in the specified format.
    """

    # pylint: disable=duplicate-code, locally-disabled
    confeti_schema: dict = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "method": {"type": "string", "enum": ["batch", "streaming"]},
            "operation": {"type": "string", "enum": ["complete", "update", "append"]},
            "data_format": {"type": "string", "enum": ["parquet", "csv", "json"]},
            "location": {"type": "string"},
        },
        "required": ["name", "method", "operation", "data_format", "location"],
    }

    def __init__(
        self,
        name: str,
        method: str,
        operation: str,
        data_format: str,
        location: str,
    ) -> None:
        """
        Initialize LoadSpecAbstract with the specified parameters.
        """
        self.name: str = name
        self.method: LoadMethod = LoadMethod(value=method)
        self.operation: LoadOperation = LoadOperation(value=operation)
        self.data_format: LoadFormat = LoadFormat(value=data_format)
        self.location: str = location

    @classmethod
    def from_confeti(cls, confeti: dict):
        """
        Get the load specifications from confeti.

        Args:
            confeti (dict): load operation configuration.

        Returns:
            LoadSpecAbstract: The LoadSpecAbstract object created from the Confeti dictionary.
        """
        JsonHandler.validate_json(json=confeti, schema=cls.confeti_schema)

        return cls(**confeti)


class LoadSpecPyspark(LoadSpecAbstract):
    """
    Specification of the sink input.

    Args:
        name (str): ID of the sink specification.
        method (str): Type of sink load mode.
        operation (str): Type of sink operation.
        data_format (str): Format of the sink input.
        location (str): URI that identifies where to load data in the specified format.
        options (dict): Execution options.
    """

    def __init__(
        self,
        name: str,
        method: str,
        operation: str,
        data_format: str,
        location: str,
        options: dict | None = None,
    ) -> None:
        """
        Initialize LoadSpecPyspark with the specified parameters.
        """
        self.options: dict = options or {}
        super().__init__(
            name=name,
            method=method,
            operation=operation,
            data_format=data_format,
            location=location,
        )
