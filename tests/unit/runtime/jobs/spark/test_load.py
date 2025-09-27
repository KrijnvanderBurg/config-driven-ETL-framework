"""Tests for LoadFileSpark functionality.

These tests verify LoadFileSpark creation, validation, data loading,
and error handling scenarios.
"""

import json
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError
from pyspark.sql.types import StringType, StructField, StructType

from flint.runtime.jobs.models.model_load import LoadFormat, LoadMethod, LoadMode
from flint.runtime.jobs.spark.load import LoadFileSpark

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="valid_load_config")
def fixture_valid_load_config(tmp_path: Path) -> Generator[dict[str, Any], Any, None]:
    """Provide a valid load configuration with real test files using tmp_path.

    Args:
        tmp_path: pytest temporary directory fixture.

    Yields:
        dict: configuration dictionary pointing to files under tmp_path.
    """

    # Create output location file under tmp_path
    output_file = Path(tmp_path) / "output.json"

    # Create schema location file under tmp_path
    schema_file = Path(tmp_path) / "schema.json"
    # Provide a simple schema placeholder
    schema = StructType([StructField("id", StringType(), True)])
    schema_file.write_text(json.dumps(schema.jsonValue()), encoding="utf-8")

    config = {
        "name": "customer_data_output",
        "upstream_name": "customer_transform",
        "method": "batch",
        "location": str(output_file),
        "schema_location": str(schema_file),
        "options": {
            "header": True,
        },
        "mode": "overwrite",
        "data_format": "json",
    }

    yield config


# =========================================================================== #
# ========================== VALIDATION TESTS ============================= #
# =========================================================================== #


class TestLoadFileSparkValidation:
    """Test LoadFileSpark model validation and instantiation."""

    def test_create_load_file_spark__with_valid_config__succeeds(self, valid_load_config: dict[str, Any]) -> None:
        """Test LoadFileSpark creation with valid configuration."""
        # Act
        load = LoadFileSpark(**valid_load_config)

        # Assert
        assert load.name == "customer_data_output"
        assert load.upstream_name == "customer_transform"
        assert load.method == LoadMethod.BATCH
        assert load.data_format == LoadFormat.JSON
        assert load.mode == LoadMode.OVERWRITE
        assert load.options == {"header": True}
        assert isinstance(load.location, str) and load.location.endswith(".json")
        assert isinstance(load.schema_location, str) and load.schema_location.endswith(".json")

    def test_create_load_file_spark__with_missing_name__raises_validation_error(
        self, valid_load_config: dict[str, Any]
    ) -> None:
        """Test LoadFileSpark creation fails when name is missing."""
        # Arrange
        del valid_load_config["name"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            LoadFileSpark(**valid_load_config)

    def test_create_load_file_spark__with_empty_name__raises_validation_error(
        self, valid_load_config: dict[str, Any]
    ) -> None:
        """Test LoadFileSpark creation fails with empty name."""
        # Arrange
        valid_load_config["name"] = ""

        # Assert
        with pytest.raises(ValidationError):
            # Act
            LoadFileSpark(**valid_load_config)

    def test_create_load_file_spark__with_invalid_method__raises_validation_error(
        self, valid_load_config: dict[str, Any]
    ) -> None:
        """Test LoadFileSpark creation fails with invalid load method."""
        # Arrange
        valid_load_config["method"] = "invalid_method"

        # Assert
        with pytest.raises(ValidationError):
            # Act
            LoadFileSpark(**valid_load_config)

    def test_create_load_file_spark__with_invalid_mode__raises_validation_error(
        self, valid_load_config: dict[str, Any]
    ) -> None:
        """Test LoadFileSpark creation fails with invalid load mode."""
        # Arrange
        valid_load_config["mode"] = "invalid_mode"

        # Assert
        with pytest.raises(ValidationError):
            # Act
            LoadFileSpark(**valid_load_config)

    def test_create_load_file_spark__with_invalid_format__raises_validation_error(
        self, valid_load_config: dict[str, Any]
    ) -> None:
        """Test LoadFileSpark creation fails with invalid data format."""
        # Arrange
        valid_load_config["data_format"] = "invalid_format"

        # Assert
        with pytest.raises(ValidationError):
            # Act
            LoadFileSpark(**valid_load_config)

    def test_create_load_file_spark__with_missing_upstream_name__raises_validation_error(
        self, valid_load_config: dict[str, Any]
    ) -> None:
        """Test LoadFileSpark creation fails when upstream_name is missing."""
        # Arrange
        del valid_load_config["upstream_name"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            LoadFileSpark(**valid_load_config)

    def test_create_load_file_spark__with_missing_location__raises_validation_error(
        self, valid_load_config: dict[str, Any]
    ) -> None:
        """Test LoadFileSpark creation fails when location is missing."""
        # Arrange
        del valid_load_config["location"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            LoadFileSpark(**valid_load_config)

    def test_create_load_file_spark__with_streaming_method__succeeds(self, valid_load_config: dict[str, Any]) -> None:
        """Test LoadFileSpark creation with streaming method."""
        # Arrange
        valid_load_config["method"] = "streaming"

        # Act
        load = LoadFileSpark(**valid_load_config)

        # Assert
        assert load.method == LoadMethod.STREAMING

    def test_create_load_file_spark__with_csv_format__succeeds(self, valid_load_config: dict[str, Any]) -> None:
        """Test LoadFileSpark creation with CSV data format."""
        # Arrange
        valid_load_config["data_format"] = "csv"

        # Act
        load = LoadFileSpark(**valid_load_config)

        # Assert
        assert load.data_format == LoadFormat.CSV

    def test_create_load_file_spark__with_parquet_format__succeeds(self, valid_load_config: dict[str, Any]) -> None:
        """Test LoadFileSpark creation with Parquet data format."""
        # Arrange
        valid_load_config["data_format"] = "parquet"

        # Act
        load = LoadFileSpark(**valid_load_config)

        # Assert
        assert load.data_format == LoadFormat.PARQUET

    def test_create_load_file_spark__with_none_schema_location__succeeds(
        self, valid_load_config: dict[str, Any]
    ) -> None:
        """Test LoadFileSpark creation with None schema location."""
        # Arrange
        valid_load_config["schema_location"] = None

        # Act
        load = LoadFileSpark(**valid_load_config)

        # Assert
        assert load.schema_location is None

    def test_create_load_file_spark__with_empty_options__succeeds(self, valid_load_config: dict[str, Any]) -> None:
        """Test LoadFileSpark creation with empty options."""
        # Arrange
        valid_load_config["options"] = {}

        # Act
        load = LoadFileSpark(**valid_load_config)

        # Assert
        assert load.options == {}


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="load_file_spark")
def fixture_load_file_spark(valid_load_config: dict[str, Any]) -> LoadFileSpark:
    """Create LoadFileSpark instance from valid configuration."""
    return LoadFileSpark(**valid_load_config)


# =========================================================================== #
# ============================ LOAD TESTS ================================== #
# =========================================================================== #


class TestLoadFileSparkLoad:
    """Test LoadFileSpark load functionality."""

    def test_load__with_batch_method__loads_data_to_file_successfully(self, load_file_spark: LoadFileSpark) -> None:
        """Test batch load writes DataFrame data to file location."""
        # Arrange - create test data in the registry under upstream_name
        spark = load_file_spark.spark.session
        test_data = [("Alice", 30), ("Bob", 25)]
        upstream_dataframe = spark.createDataFrame(test_data, ["name", "age"])
        load_file_spark.data_registry[load_file_spark.upstream_name] = upstream_dataframe

        # Act
        load_file_spark.load()

        # Assert - data was copied to load_file_spark.name in registry
        assert load_file_spark.name in load_file_spark.data_registry
        loaded_dataframe = load_file_spark.data_registry[load_file_spark.name]

        # Verify the data is the same as upstream
        loaded_rows = loaded_dataframe.collect()
        assert len(loaded_rows) == 2
        assert loaded_rows[0]["name"] == "Alice"
        assert loaded_rows[1]["name"] == "Bob"

    def test_load__with_streaming_method__creates_streaming_query(
        self, valid_load_config: dict[str, Any], tmp_path: Path
    ) -> None:
        """Test streaming load creates streaming query and stores it in registry."""
        # Arrange
        valid_load_config["method"] = "streaming"
        valid_load_config["mode"] = "append"  # Use valid streaming mode
        # Add checkpoint location for streaming
        checkpoint_dir = tmp_path / "checkpoint"
        valid_load_config["options"]["checkpointLocation"] = str(checkpoint_dir)

        load_spark = LoadFileSpark(**valid_load_config)

        # Create streaming test data
        spark = load_spark.spark.session
        # Convert to streaming DataFrame by reading from a stream-like source
        streaming_df = spark.readStream.format("rate").load()
        load_spark.data_registry[load_spark.upstream_name] = streaming_df

        # Act
        load_spark.load()

        # Assert - streaming query was created and stored
        assert load_spark.name in load_spark.streaming_query_registry
        streaming_query = load_spark.streaming_query_registry[load_spark.name]

        # Verify it's a streaming query
        assert streaming_query.isActive

        # Clean up
        streaming_query.stop()

    def test_load__with_unsupported_method__raises_value_error(self, load_file_spark: LoadFileSpark) -> None:
        """Test load with unsupported method raises ValueError."""
        # Arrange - create upstream data
        spark = load_file_spark.spark.session
        upstream_dataframe = spark.createDataFrame([("test", 1)], ["name", "value"])
        load_file_spark.data_registry[load_file_spark.upstream_name] = upstream_dataframe

        # Create unsupported method
        from unittest.mock import Mock

        unsupported_method = Mock()
        unsupported_method.value = "unsupported"
        load_file_spark.method = unsupported_method

        # Assert
        with pytest.raises(ValueError, match="Loading method .* is not supported for PySpark"):
            # Act
            load_file_spark.load()

    def test_load__copies_dataframe_from_upstream_to_current(self, load_file_spark: LoadFileSpark) -> None:
        """Test load copies DataFrame from upstream registry entry to current name."""
        # Arrange
        spark = load_file_spark.spark.session
        original_dataframe = spark.createDataFrame([("original", 42)], ["type", "value"])
        load_file_spark.data_registry[load_file_spark.upstream_name] = original_dataframe

        # Act
        load_file_spark.load()

        # Assert
        copied_dataframe = load_file_spark.data_registry[load_file_spark.name]
        copied_rows = copied_dataframe.collect()
        original_rows = original_dataframe.collect()

        # Verify same data
        assert len(copied_rows) == len(original_rows) == 1
        assert copied_rows[0]["type"] == original_rows[0]["type"] == "original"
        assert copied_rows[0]["value"] == original_rows[0]["value"] == 42

    def test_load__with_no_schema_location__skips_schema_export(self, valid_load_config: dict[str, Any]) -> None:
        """Test load with None schema_location skips schema export."""
        # Arrange
        valid_load_config["schema_location"] = None
        load_spark = LoadFileSpark(**valid_load_config)

        spark = load_spark.spark.session
        upstream_dataframe = spark.createDataFrame([("test", 1)], ["name", "value"])
        load_spark.data_registry[load_spark.upstream_name] = upstream_dataframe

        # Act
        load_spark.load()

        # Assert - load completes successfully without schema export
        assert load_spark.name in load_spark.data_registry
        # Verify schema_location is None (no file should be created)
        assert load_spark.schema_location is None

    def test_load__with_schema_location__exports_schema_to_file(self, load_file_spark: LoadFileSpark) -> None:
        """Test load with schema_location exports DataFrame schema to file."""
        # Arrange
        spark = load_file_spark.spark.session
        upstream_dataframe = spark.createDataFrame([("Alice", 30), ("Bob", 25)], ["name", "age"])
        load_file_spark.data_registry[load_file_spark.upstream_name] = upstream_dataframe

        # Act
        load_file_spark.load()

        # Assert - schema file was created
        assert Path(load_file_spark.schema_location).exists()

        # Verify schema content
        schema_content = Path(load_file_spark.schema_location).read_text(encoding="utf-8")
        schema_dict = json.loads(schema_content)

        # Check that it's a valid PySpark schema JSON structure
        assert "type" in schema_dict
        assert schema_dict["type"] == "struct"
        assert "fields" in schema_dict
        assert len(schema_dict["fields"]) == 2  # name and age fields
