"""Tests for Extract Spark implementation.

These tests verify that ExtractFileSpark instances can be correctly created from
configuration and function as expected.
"""

import json
import tempfile
from collections.abc import Generator
from contextlib import ExitStack
from typing import Any

import pytest
from pyspark.sql.types import StructType

from flint.runtime.jobs.models.model_extract import ExtractFormat, ExtractMethod
from flint.runtime.jobs.spark.extract import ExtractFileSpark

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="extract_config")
def fixture_extract_config() -> Generator[dict[str, Any], Any, None]:
    """Provide a representative extract configuration with proper named tempfiles."""
    # Use ExitStack to manage multiple temporary resources
    stack = ExitStack()

    # Create a named temporary file for the data source
    temp_data_file = stack.enter_context(tempfile.NamedTemporaryFile(suffix=".json", mode="w+b"))
    temp_data_file.write(b"")
    temp_data_file.flush()

    # Schema as filepath exercises the SchemaFilepathHandler path code.
    temp_schema_file = stack.enter_context(tempfile.NamedTemporaryFile(suffix=".json", mode="w+b"))
    schema_json_bytes = json.dumps(StructType().jsonValue()).encode("utf-8")
    temp_schema_file.write(schema_json_bytes)
    temp_schema_file.flush()

    config = {
        "name": "sales_data",
        "method": "batch",
        "data_format": "json",
        "options": {
            "multiLine": True,
            "inferSchema": True,
        },
        "location": temp_data_file.name,
        "schema_": temp_schema_file.name,
    }

    # Return both the config and the stack so we can properly clean up later
    yield config

    # Close all temporary files automatically
    stack.close()


def test_extract_creation__from_config__creates_valid_model(extract_config: dict[str, Any]) -> None:
    """Test specifically for the creation process itself."""
    # Act
    extract = ExtractFileSpark(**extract_config)

    # Assert
    assert extract.name == "sales_data"
    assert extract.method == ExtractMethod.BATCH
    assert extract.data_format == ExtractFormat.JSON
    assert extract.options == {"multiLine": True, "inferSchema": True}
    assert isinstance(extract.location, str) and extract.location.endswith(".json")
    assert isinstance(extract.schema_, str) and extract.schema_.endswith(".json")


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="extract_spark")
def fixture_extract_spark(extract_config: dict[str, Any]) -> ExtractFileSpark:
    """Create ExtractFileSpark instance from configuration.

    Args:
        extract_config: config dictionary fixture with tempfile paths

    Returns:
        ExtractFileSpark instance initialized from config
    """
    return ExtractFileSpark(**extract_config)


def test_extract_properties__after_creation__match_expected_values(extract_spark: ExtractFileSpark) -> None:
    """Test model properties using the instantiated object fixture.

    Use hard-coded expected values to avoid comparing fixtures directly.
    """
    # Assert attributes in the same order as the creation test
    assert extract_spark.name == "sales_data"
    assert extract_spark.method == ExtractMethod.BATCH
    assert extract_spark.data_format == ExtractFormat.JSON
    assert extract_spark.options == {"multiLine": True, "inferSchema": True}

    # Location and schema_ must be filepaths ending with .json
    assert isinstance(extract_spark.location, str)
    assert extract_spark.location.endswith(".json")

    assert isinstance(extract_spark.schema_, str)
    assert extract_spark.schema_.endswith(".json")


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #
