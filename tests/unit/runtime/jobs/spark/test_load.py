"""Tests for LoadFileSpark functionality.

These tests verify LoadFileSpark creation, validation, data loading,
and error handling scenarios.
"""

from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest
from pydantic import ValidationError

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

    def test_load__with_batch_method__calls_load_batch(self, load_file_spark: LoadFileSpark) -> None:
        """Test load method calls _load_batch for batch loading."""
        # Arrange
        load_file_spark.schema_location = None

        with (
            patch("flint.runtime.jobs.spark.load.LoadSpark.data_registry"),
            patch.object(load_file_spark, "_load_batch") as mock_load_batch,
            patch.object(load_file_spark, "_load_schema") as mock_load_schema,
        ):
            # Act
            load_file_spark.load()

            # Assert
            mock_load_batch.assert_called_once()
            mock_load_schema.assert_called_once()

    def test_load__with_streaming_method__calls_load_streaming(self, valid_load_config: dict[str, Any]) -> None:
        """Test load method calls _load_streaming for streaming loading."""
        # Arrange
        valid_load_config["method"] = "streaming"
        load_streaming = LoadFileSpark(**valid_load_config)
        mock_query = Mock()

        with (
            patch("flint.runtime.jobs.spark.load.LoadSpark.data_registry"),
            patch.object(load_streaming, "_load_streaming", return_value=mock_query) as mock_load_streaming,
            patch.object(load_streaming, "_load_schema") as mock_load_schema,
        ):
            # Act
            load_streaming.load()

            # Assert
            mock_load_streaming.assert_called_once()
            mock_load_schema.assert_called_once()

    def test_load__with_invalid_method__raises_value_error(self, load_file_spark: LoadFileSpark) -> None:
        """Test load method raises ValueError for unsupported loading method."""
        # Arrange
        mock_method = Mock()
        mock_method.value = "invalid_method"

        with (
            patch("flint.runtime.jobs.spark.load.LoadSpark.data_registry"),
            patch.object(load_file_spark, "method", mock_method),
        ):
            # Assert
            with pytest.raises(ValueError, match="is not supported for PySpark"):
                # Act
                load_file_spark.load()

    def test_load__with_batch_method__calls_dataframe_write_save(self, load_file_spark: LoadFileSpark) -> None:
        """Test load method with batch calls dataframe.write.save with correct parameters."""
        # Arrange
        mock_dataframe = Mock()
        mock_dataframe.count.return_value = 5
        mock_dataframe.schema.jsonValue.return_value = {"type": "struct", "fields": []}
        # Provide a write.save mock directly on the dataframe
        mock_dataframe.write = Mock()
        mock_dataframe.write.save = Mock()

        # Patch the class-level registry to a simple dict that returns our mock dataframe
        with patch.object(LoadFileSpark, "data_registry", {load_file_spark.upstream_name: mock_dataframe}):
            # Act
            load_file_spark.load()

            # Assert
            mock_dataframe.write.save.assert_called_once_with(
                path=load_file_spark.location,
                format=load_file_spark.data_format.value,
                mode=load_file_spark.mode.value,
                **load_file_spark.options,
            )

    def test_load__with_streaming_method__calls_dataframe_write_stream_start(
        self, valid_load_config: dict[str, Any]
    ) -> None:
        """Test load method with streaming calls dataframe.writeStream.start with correct parameters."""
        # Arrange
        valid_load_config["method"] = "streaming"
        load_streaming = LoadFileSpark(**valid_load_config)
        mock_dataframe = Mock()
        mock_dataframe.schema.jsonValue.return_value = {"type": "struct", "fields": []}
        mock_streaming_query = Mock()
        mock_streaming_query.id = "test-query-id"
        # attach a writeStream mock with a start() that returns a query
        mock_write_stream = Mock()
        mock_write_stream.start.return_value = mock_streaming_query
        mock_dataframe.writeStream = mock_write_stream

        # Use simple dicts for the registries so LoadFileSpark can read/set entries
        with (
            patch.object(LoadFileSpark, "data_registry", {load_streaming.upstream_name: mock_dataframe}),
            patch.object(LoadFileSpark, "streaming_query_registry", {}),
        ):
            # Act
            load_streaming.load()

            # Assert
            mock_dataframe.writeStream.start.assert_called_once_with(
                path=load_streaming.location,
                format=load_streaming.data_format.value,
                outputMode=load_streaming.mode.value,
                **load_streaming.options,
            )
