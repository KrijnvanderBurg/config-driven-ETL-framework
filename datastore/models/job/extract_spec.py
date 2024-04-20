"""
IO extract interface and strategy, extract implementations are in module datastore.loads.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

import json
from abc import ABC
from enum import Enum

from pyspark.sql.types import StructType

from datastore.utils.file_handler import FileHandler
from datastore.utils.json_handler import JsonHandler


class ExtractMethod(Enum):
    """
    Types of extract operations.
    """

    BATCH = "batch"
    STREAMING = "streaming"


class ExtractFormat(Enum):
    """Types of input and structures for extract."""

    PARQUET = "parquet"
    JSON = "json"
    CSV = "csv"


class ExtractSpecAbstract(ABC):
    """
    ExtractSpec class.

    Args:
        name (str): ID of the extract specification.
        method (str): method of extract operation.
        data_format (str): format of the extract.
        location (str): uri that identifies from where to extract data in the specified format.
    """

    # pylint: disable=duplicate-code, locally-disabled
    confeti_schema: dict = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "method": {"type": "string", "enum": ["batch", "streaming"]},
            "data_format": {"type": "string", "enum": ["parquet", "csv", "json"]},
            "location": {"type": "string"},
            "schema": {"type": "string"},
        },
        "required": ["name", "method", "data_format", "location"],
    }

    def __init__(
        self,
        name: str,
        method: str,
        data_format: str,
        location: str,
    ) -> None:
        """
        Initialize ExtractSpecAbstract with the specified parameters.
        """
        self.name: str = name
        self.method: ExtractMethod = ExtractMethod(value=method)
        self.data_format: ExtractFormat = ExtractFormat(value=data_format)
        self.location: str = location

    @classmethod
    def from_confeti(cls, confeti: dict):
        """
        Create an ExtractSpecAbstract object from a Confeti dictionary.

        Args:
            confeti (dict): The Confeti dictionary.

        Returns:
            ExtractSpecAbstract: The ExtractSpecAbstract object created from the Confeti dictionary.
        """
        JsonHandler.validate_json(json=confeti, schema=cls.confeti_schema)

        return cls(**confeti)


class ExtractSpecPyspark(ExtractSpecAbstract):
    """
    ExtractSpec pyspark class.

    Args:
        name (str): ID of the extract specification.
        method (ExtractMethod): ReadType method of extract operation.
        data_format (ExtractFormat): format of the extract.
        location (str): uri that identifies from where to extract data in the specified format.
        schema (str): schema to be parsed to StructType.
    """

    def __init__(
        self,
        name: str,
        method: str,
        data_format: str,
        location: str,
        schema: str = "",
    ) -> None:
        """
        Initialize ExtractSpecPyspark with the specified parameters.
        """
        self.schema: StructType | None = self.schema_factory(schema=schema)
        super().__init__(name=name, method=method, data_format=data_format, location=location)

    @staticmethod
    def schema_factory(schema: str) -> StructType | None:
        """
        Get the appropriate schema handler based on the schema.

        Args:
            schema (str): The schema attribute string.

        Returns:
            StructType: The schema handler object.

        Raises:
            NotImplementedError: If the schema value is not recognized or not supported.
        """
        if schema == "":
            return None

        supported_extensions: dict = {
            ".json": ExtractSpecPyspark.schema_from_json_file,
        }

        for key, value in supported_extensions.items():
            if schema.endswith(key):
                return value(filepath=schema)

        if FileHandler.is_json(schema):
            return ExtractSpecPyspark.schema_from_json(schema=schema)

        raise NotImplementedError(f"No schema handling strategy recognised or supported for value: {schema}")

    @staticmethod
    def schema_from_json(schema: str) -> StructType:
        """
        Read JSON schema string.

        Args:
            schema (str): schema json.

        Returns:
            StructType: The test JSON schema.

        Raises:
            ValueError: If there's an error decoding the JSON schema.
        """
        try:
            json_content = json.loads(s=schema)
            schema_read = StructType.fromJson(json=json_content)
            return schema_read
        except json.JSONDecodeError as e:
            # Using ValueError instead of JSONDecodeError due to complexity in supplying additional arguments.
            raise ValueError(f"Error decoding JSON schema '{schema}': {e}") from e

    @staticmethod
    def schema_from_json_file(filepath: str) -> StructType:
        """
        Read JSON schema file.

        Args:
            filepath (str): path to schema file.

        Returns:
            StructType: The test JSON schema.

        Raises:
            FileNotFoundError: If the schema file is not found.
            PermissionError: If permission is denied for accessing the schema file.
            ValueError: If there's an error decoding the JSON schema.
        """
        try:
            with open(filepath, "r", encoding="utf-8") as file:
                json_content = json.load(fp=file)
            schema_parsed = StructType.fromJson(json=json_content)
            return schema_parsed
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Schema '{filepath}' not found.") from e
        except PermissionError as e:
            raise PermissionError(f"Permission denied for schema '{filepath}'.") from e
        except json.JSONDecodeError as e:
            # Using ValueError instead of JSONDecodeError due to complexity in supplying additional arguments.
            raise ValueError(f"Error decoding JSON schema '{filepath}': {e}") from e
