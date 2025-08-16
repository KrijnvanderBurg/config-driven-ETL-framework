"""
Test module for the main function.
"""

import argparse
import glob
import json
from pathlib import Path
from unittest import mock

import pytest
from pyspark.sql.types import StructType
from pyspark.testing import assertDataFrameEqual

from flint.__main__ import main
from flint.job.core.job import JOB, LOADS
from flint.job.core.load import DATA_FORMAT, LOCATION, SCHEMA_LOCATION
from flint.utils.spark import SparkHandler


@pytest.mark.parametrize("job_path", glob.glob("tests/e2e/**/job.json", recursive=True))
def test__main(tmp_path: Path, job_path: str) -> None:
    """Test main function with different configurations."""
    # Arrange
    job_tmp_path = Path(tmp_path, "job.json")

    # Step 1: Read the original JSON file
    with open(file=job_path, mode="r", encoding="utf-8") as file:
        data: dict = json.load(file)

    # Step 2: Prepend the load location filepath with tmp_path to write results to temporary directory
    for load in data[JOB][LOADS]:
        load[LOCATION] = str(Path(tmp_path, load[LOCATION]))
        load[SCHEMA_LOCATION] = str(Path(tmp_path, load[SCHEMA_LOCATION]))

    # Step 3: Overwrite the modified data to the existing JSON file
    with open(file=job_tmp_path, mode="w", encoding="utf-8") as file:
        json.dump(data, file)

    # Step 4: Create a Spark session, only needed for reading output files to assert equal
    SparkHandler()

    # Step 5: Use the modified file path for the test
    # Include the command="run" argument to match the new CLI structure
    args = argparse.Namespace(config_filepath=str(job_tmp_path), command="run", log_level="INFO")

    with mock.patch.object(argparse.ArgumentParser, "parse_args", return_value=args):
        # Act
        main()

        # Assert
        for load in data[JOB][LOADS]:
            test_output_path_relative = Path(load[LOCATION]).relative_to(tmp_path)

            load_output_actual = Path(load[LOCATION])
            load_output_expected = Path(test_output_path_relative, "expected_output").with_suffix(
                f".{load[DATA_FORMAT]}"
            )

            # Step 6: Read the content of both files into DataFrames
            schema_path_actual = Path(load[SCHEMA_LOCATION])
            with open(schema_path_actual, "r", encoding="utf-8") as file:
                json_content = json.load(fp=file)
            schema_actual = StructType.fromJson(json=json_content)

            expected_schema_path = Path(test_output_path_relative, "expected_schema").with_suffix(".json")
            with open(expected_schema_path, "r", encoding="utf-8") as file:
                json_content = json.load(fp=file)
            schema_expected = StructType.fromJson(json=json_content)

            # Step 7: Compare actual and expected DataFrames
            df_actual = (
                SparkHandler()
                .session.read.format(load[DATA_FORMAT])
                .schema(schema_actual)
                .load(str(load_output_actual))
            )
            df_expected = (
                SparkHandler()
                .session.read.format(load[DATA_FORMAT])
                .schema(schema_expected)
                .load(str(load_output_expected))
            )

            assertDataFrameEqual(actual=df_actual, expected=df_expected)
