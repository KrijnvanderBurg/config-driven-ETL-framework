"""
File handler static class.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

import json
from pathlib import Path


class File:
    """A utility class for extracting files."""

    @staticmethod
    def from_json(filepath: Path, encoding="utf-8") -> str:
        """
        Reads the content of a JSON file and returns the parsed JSON data.

        Parameters:
            filepath (Path): The filepath of the JSON file.
            encoding (str): File encoding type, default utf-8.

        Returns:
            str: JSON data.
        """
        if not filepath.is_file():
            raise FileNotFoundError(f"The file '{filepath}' does not exist.")

        # If path exists and is a file
        with open(file=filepath, mode="r", encoding=encoding) as f:
            j = json.load(f)
        return j
