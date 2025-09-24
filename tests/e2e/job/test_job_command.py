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
    # Read and modify job config
    with open(file=job_path, mode="r", encoding="utf-8") as file:
        data: dict = json.load(file)

    # Update paths to temp directory
    for job in data["runtime"]["jobs"]:
        for load in job["loads"]:
            load["location"] = str(Path(tmp_path, load["location"]))
            schema_filename = Path(load["schema_location"]).name
            load["schema_location"] = str(Path(tmp_path, schema_filename))

    # Write config and run job
    config_path = Path(tmp_path, "config.json")
    with open(file=config_path, mode="w", encoding="utf-8") as file:
        json.dump(data, file)

    args = argparse.Namespace(
        runtime_filepath=str(config_path), alert_filepath=str(config_path), command="run", log_level="INFO"
    )

    with mock.patch.object(argparse.ArgumentParser, "parse_args", return_value=args):
        main()

    # Verify results
    for job in data["runtime"]["jobs"]:
        for load in job["loads"]:
            # Compare schemas
            actual_schema_path = Path(load["schema_location"])
            with open(actual_schema_path, "r", encoding="utf-8") as file:
                actual_schema = StructType.fromJson(json.load(file))

            expected_schema_path = Path(job_path).parent / load["name"] / "expected_schema.json"
            with open(expected_schema_path, "r", encoding="utf-8") as file:
                expected_schema = StructType.fromJson(json.load(file))

            assert actual_schema == expected_schema

            # Compare data
            actual_data_path = Path(load["location"])
            expected_data_path = Path(job_path).parent / load["name"] / f"expected_output.{load['data_format']}"

            df_actual = (
                SparkHandler()
                .session.read.format(load["data_format"])
                .schema(actual_schema)
                .load(str(actual_data_path))
            )
            df_expected = (
                SparkHandler()
                .session.read.format(load["data_format"])
                .schema(expected_schema)
                .load(str(expected_data_path))
            )

            assertDataFrameEqual(actual=df_actual, expected=df_expected)
