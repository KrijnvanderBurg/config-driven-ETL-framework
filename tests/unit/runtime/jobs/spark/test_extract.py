"""Tests for Extract Spark implementation.

These tests verify ExtractFileSpark creation, validation, data extraction,
and error handling scenarios.
"""

import json
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest
from pydantic import ValidationError
from pyspark.sql.types import StringType, StructField, StructType

from flint.runtime.jobs.models.model_extract import ExtractFormat, ExtractMethod
from flint.runtime.jobs.spark.extract import ExtractFileSpark

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="valid_extract_config")
def fixture_valid_extract_config(tmp_path: Path) -> Generator[dict[str, Any], Any, None]:
    """Provide a valid extract configuration with real test data using tmp_path.

    Args:
        tmp_path: pytest temporary directory fixture.

    Yields:
        dict: configuration dictionary pointing to files under tmp_path.
    """
    # Create a data file under the tmp_path
    data_file = Path(tmp_path, "test_data.json")
    test_data = [{"id": "Alice", "age": 30}, {"id": "Bob", "age": 25}]
    data_file.write_text(json.dumps(test_data), encoding="utf-8")

    # Create schema file under the tmp_path
    schema_file = Path(tmp_path, "schema.json")
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", StringType(), True),  # Use StringType since JSON parsing can be flexible
        ]
    )
    schema_file.write_text(json.dumps(schema.jsonValue()), encoding="utf-8")

    config = {
        "id": "test_data",
        "method": "batch",
        "data_format": "json",
        "options": {
            "multiLine": True,
        },
        "location": str(data_file),
        "schema_": str(schema_file),
    }

    yield config


# =========================================================================== #
# ========================== VALIDATION TESTS ============================= #
# =========================================================================== #


class TestExtractFileSparkValidation:
    """Test ExtractFileSpark model validation and instantiation."""

    def test_create_extract_file_spark__with_valid_config__succeeds(self, valid_extract_config: dict[str, Any]) -> None:
        """Test ExtractFileSpark creation with valid configuration."""
        # Act
        extract = ExtractFileSpark(**valid_extract_config)

        # Assert
        assert extract.id == "test_data"
        assert extract.method == ExtractMethod.BATCH
        assert extract.data_format == ExtractFormat.JSON
        assert extract.options == {"multiLine": True}
        assert isinstance(extract.location, str) and extract.location.endswith(".json")
        assert isinstance(extract.schema_, str) and extract.schema_.endswith(".json")

    def test_create_extract_file_spark__with_missing_name__raises_validation_error(
        self, valid_extract_config: dict[str, Any]
    ) -> None:
        """Test ExtractFileSpark creation fails when name is missing."""
        # Arrange
        del valid_extract_config["id"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            ExtractFileSpark(**valid_extract_config)

    def test_create_extract_file_spark__with_empty_name__raises_validation_error(
        self, valid_extract_config: dict[str, Any]
    ) -> None:
        """Test ExtractFileSpark creation fails with empty name."""
        # Arrange
        valid_extract_config["id"] = ""

        # Assert
        with pytest.raises(ValidationError):
            # Act
            ExtractFileSpark(**valid_extract_config)

    def test_create_extract_file_spark__with_invalid_method__raises_validation_error(
        self, valid_extract_config: dict[str, Any]
    ) -> None:
        """Test ExtractFileSpark creation fails with invalid extraction method."""
        # Arrange
        valid_extract_config["method"] = "invalid_method"

        # Assert
        with pytest.raises(ValidationError):
            # Act
            ExtractFileSpark(**valid_extract_config)

    def test_create_extract_file_spark__with_invalid_format__raises_validation_error(
        self, valid_extract_config: dict[str, Any]
    ) -> None:
        """Test ExtractFileSpark creation fails with invalid data format."""
        # Arrange
        valid_extract_config["data_format"] = "invalid_format"

        # Assert
        with pytest.raises(ValidationError):
            # Act
            ExtractFileSpark(**valid_extract_config)

    def test_create_extract_file_spark__with_missing_location__raises_validation_error(
        self, valid_extract_config: dict[str, Any]
    ) -> None:
        """Test ExtractFileSpark creation fails when location is missing."""
        # Arrange
        del valid_extract_config["location"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            ExtractFileSpark(**valid_extract_config)

    def test_create_extract_file_spark__with_empty_location__succeeds(
        self, valid_extract_config: dict[str, Any]
    ) -> None:
        """Test ExtractFileSpark creation succeeds with empty location."""
        # Arrange
        valid_extract_config["location"] = ""

        # Act
        extract = ExtractFileSpark(**valid_extract_config)

        # Assert
        assert extract.location == ""

    def test_create_extract_file_spark__with_streaming_method__succeeds(
        self, valid_extract_config: dict[str, Any]
    ) -> None:
        """Test ExtractFileSpark creation with streaming method."""
        # Arrange
        valid_extract_config["method"] = "streaming"

        # Act
        extract = ExtractFileSpark(**valid_extract_config)

        # Assert
        assert extract.method == ExtractMethod.STREAMING

    def test_create_extract_file_spark__with_csv_format__succeeds(self, valid_extract_config: dict[str, Any]) -> None:
        """Test ExtractFileSpark creation with CSV data format."""
        # Arrange
        valid_extract_config["data_format"] = "csv"

        # Act
        extract = ExtractFileSpark(**valid_extract_config)

        # Assert
        assert extract.data_format == ExtractFormat.CSV

    def test_create_extract_file_spark__with_parquet_format__succeeds(
        self, valid_extract_config: dict[str, Any]
    ) -> None:
        """Test ExtractFileSpark creation with Parquet data format."""
        # Arrange
        valid_extract_config["data_format"] = "parquet"

        # Act
        extract = ExtractFileSpark(**valid_extract_config)

        # Assert
        assert extract.data_format == ExtractFormat.PARQUET

    def test_create_extract_file_spark__with_empty_schema__succeeds(self, valid_extract_config: dict[str, Any]) -> None:
        """Test ExtractFileSpark creation with empty schema."""
        # Arrange
        valid_extract_config["schema_"] = ""

        # Act
        extract = ExtractFileSpark(**valid_extract_config)

        # Assert
        assert extract.schema_ == ""

    def test_create_extract_file_spark__with_json_schema_string__succeeds(
        self, valid_extract_config: dict[str, Any]
    ) -> None:
        """Test ExtractFileSpark creation with JSON schema string."""
        # Arrange
        valid_extract_config["schema_"] = '{"type":"struct","fields":[]}'

        # Act
        extract = ExtractFileSpark(**valid_extract_config)

        # Assert
        assert extract.schema_ == '{"type":"struct","fields":[]}'


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="extract_file_spark")
def fixture_extract_file_spark(valid_extract_config: dict[str, Any]) -> ExtractFileSpark:
    """Create ExtractFileSpark instance from valid configuration."""
    return ExtractFileSpark(**valid_extract_config)


# =========================================================================== #
# ============================ EXTRACT TESTS =============================== #
# =========================================================================== #


class TestExtractFileSparkExtract:
    """Test ExtractFileSpark extraction functionality."""

    def test_extract__with_batch_method__creates_dataframe_in_registry(
        self, extract_file_spark: ExtractFileSpark
    ) -> None:
        """Test extract method creates DataFrame for batch extraction."""
        mock_dataframe = Mock()
        mock_dataframe.count.return_value = 10
        mock_read = Mock()
        mock_read.load.return_value = mock_dataframe
        mock_session = Mock()
        mock_session.read = mock_read

        with patch("flint.runtime.jobs.spark.extract.ExtractSpark.spark") as mock_spark_handler:
            mock_spark_handler.session = mock_session
            mock_spark_handler.add_configs = Mock()

            extract_file_spark.extract()

            mock_read.load.assert_called_once()

    def test_extract__with_streaming_method__creates_streaming_query(
        self, valid_extract_config: dict[str, Any]
    ) -> None:
        """Test extract method creates StreamingQuery for streaming extraction."""
        valid_extract_config["method"] = "streaming"
        extract_streaming = ExtractFileSpark(**valid_extract_config)
        mock_dataframe = Mock()
        mock_read_stream = Mock()
        mock_read_stream.load.return_value = mock_dataframe
        mock_session = Mock()
        mock_session.readStream = mock_read_stream

        with patch("flint.runtime.jobs.spark.extract.ExtractSpark.spark") as mock_spark_handler:
            mock_spark_handler.session = mock_session
            mock_spark_handler.add_configs = Mock()

            extract_streaming.extract()

            mock_read_stream.load.assert_called_once()

    def test_extract__with_invalid_method__raises_value_error(self, extract_file_spark: ExtractFileSpark) -> None:
        """Test extract method raises ValueError for unsupported extraction method."""
        mock_method = Mock()
        mock_method.value = "invalid_method"

        with patch.object(extract_file_spark, "method", mock_method):
            with pytest.raises(ValueError):
                extract_file_spark.extract()
