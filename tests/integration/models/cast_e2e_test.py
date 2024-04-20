"""
Column cast class tests.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0O International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

import json
from pathlib import Path

from datastore.models.job_spec import JobSpecFactory, JobSpecPyspark


class Teste2eCastPyspark:
    """
    Test class for TODO
    """

    def test__e2e__from_confeti(self, tmp_path: Path) -> None:
        """
        Asserts that CastPyspark's from_confeti method returns a valid JobSpecPyspark.

        Args:
            tmp_path (py.path): Path to a temporary directory provided by pytest.
        """
        # Arrange
        confeti = {
            "strategy": {
                "engine": "pyspark",
            },
            "extract": {
                "name": "bronze-test-extract-dev",
                "method": "batch",
                "data_format": "parquet",
                "location": "/input.parquet",
            },
            "transform": {
                "name": "bronze-test-transform-dev",
                "transforms": [
                    {"function": "cast", "arguments": {"columns": {"age": "string"}}},
                ],
            },
            "load": {
                "name": "silver-test-load-dev",
                "method": "batch",
                "data_format": "parquet",
                "operation": "complete",
                "location": "/output.parquet",
            },
        }

        # Write dictionary to a JSON file in tmp_path
        temp_file_path = tmp_path / "confeti.json"
        with open(file=temp_file_path, mode="w", encoding="utf-8") as temp_file:
            json.dump(confeti, temp_file)

        filepath: str = temp_file_path.as_posix()

        # Act
        job_cast = JobSpecFactory.from_file(filepath=filepath)

        # Assert
        assert isinstance(job_cast, JobSpecPyspark)
