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
from flint.runtime.jobs.spark.session import SparkHandler


@pytest.mark.parametrize("job_path", glob.glob("tests/e2e/job/**/job.json", recursive=True))
def test_job_command(tmp_path: Path, job_path: str) -> None:
    """Test main function with different configurations."""
    # Arrange
    runtime_tmp_path = Path(tmp_path, "runtime.json")
    alert_tmp_path = Path(tmp_path, "alert.json")

    # Step 1: Read the original JSON file
    with open(file=job_path, mode="r", encoding="utf-8") as file:
        data: dict = json.load(file)

    # Step 2: Update the configuration for temporary paths
    for job in data["runtime"]["jobs"]:
        for load in job["loads"]:
            load["location"] = str(Path(tmp_path, load["location"]))
            load["schema_location"] = str(Path(tmp_path, load.get("schema_location", "")))

    # Step 3: Write the configurations to temporary files
    with open(file=runtime_tmp_path, mode="w", encoding="utf-8") as file:
        json.dump({"runtime": data["runtime"]}, file)

    with open(file=alert_tmp_path, mode="w", encoding="utf-8") as file:
        json.dump(data["alert"], file)

    # Step 4: Use the modified file paths for the test with new CLI structure
    args = argparse.Namespace(
        runtime_filepath=str(runtime_tmp_path), alert_filepath=str(alert_tmp_path), command="run", log_level="INFO"
    )

    with mock.patch.object(argparse.ArgumentParser, "parse_args", return_value=args):
        # Act
        main()

        # Assert
        for job in data["runtime"]["jobs"]:
            for load in job["loads"]:
                test_output_path_relative = Path(load["location"]).relative_to(tmp_path)
                load_output_actual = Path(load["location"])
                load_output_expected = Path(test_output_path_relative, "expected_output").with_suffix(
                    f".{load['data_format']}"
                )

                # Step 5: Compare schemas - read actual schema vs expected schema
                # Read the actual schema (written by the job)
                schema_path_actual = Path(load["schema_location"])
                with open(schema_path_actual, "r", encoding="utf-8") as file:
                    json_content = json.load(fp=file)
                schema_actual = StructType.fromJson(json=json_content)

                # Read the expected schema from the load folder (load-name/expected_schema.json)
                expected_schema_path = Path(test_output_path_relative, "expected_schema.json")
                with open(expected_schema_path, "r", encoding="utf-8") as file:
                    json_content = json.load(fp=file)
                schema_expected = StructType.fromJson(json=json_content)

                # Compare schemas
                assert schema_actual == schema_expected

                # Step 6: Compare actual and expected DataFrames
                df_actual = (
                    SparkHandler()
                    .session.read.format(load["data_format"])
                    .schema(schema_actual)
                    .load(str(load_output_actual))
                )
                df_expected = (
                    SparkHandler()
                    .session.read.format(load["data_format"])
                    .schema(schema_expected)
                    .load(str(load_output_expected))
                )

                assertDataFrameEqual(actual=df_actual, expected=df_expected)
