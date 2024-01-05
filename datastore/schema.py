"""
Schema handler static class.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

import json
from pathlib import Path

from datastore.file import File
from pyspark.sql.types import StructType


class Schema:
    """
    Uutility class for creating Spark schemas from input.
    """

    @staticmethod
    def from_spec(schema: str | None, filepath: str | None) -> StructType | None:
        """
        Spark schema from schema json string or else from schema json file.

        Args:
            schema (str): json string to parse to StructType.
            filepath (str): json file to parse to schema.

        Returns:
            StructType | None: schema or none.
        """
        if schema:
            return Schema.from_json(schema=schema)

        if filepath:
            return Schema.from_file(filepath=filepath)

        return None

    @staticmethod
    def from_json(schema: str) -> StructType:
        """
        Spark schema from json string.

        Args:
            schema (str): json string to parse to StructType.

        Returns:
            StructType: schema.
        """
        j = json.loads(s=schema)
        return StructType.fromJson(json=j)

    @staticmethod
    def from_file(filepath: str) -> StructType:
        """
        Spark schema from json file.

        Args:
            filepath (str): json file to parse to schema.

        Returns:
            StructType: schema.
        """
        p = Path(filepath)

        if p.suffix == ".json":
            j = File.from_json(filepath=p)
            return Schema.from_json(schema=j)

        raise NotImplementedError(f"file extension '{p.suffix}' is not supported.")
