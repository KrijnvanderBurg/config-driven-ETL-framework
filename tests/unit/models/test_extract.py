"""Unit tests for the ExtractModel and ExtractFileModel classes."""

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from flint.models.model_extract import DictKeyError, ExtractFileModel, ExtractFormat, ExtractMethod
from flint.utils.schema import SchemaFilepathHandler


@pytest.fixture
def valid_extract_dict() -> dict[str, Any]:
    """Return a valid dictionary for creating an ExtractFileModel."""
    return {
        "name": "test_extract_file",
        "method": "batch",
        "data_format": "parquet",
        "location": "s3://test-bucket/path",
        "options": {"inferSchema": "true"},
        "schema": "path/to/schema.json",
    }


@pytest.fixture
def extract_file_model_cls() -> ExtractFileModel:
    """Return an initialized ExtractFileModel instance."""
    return ExtractFileModel(
        name="test_extract_file",
        method=ExtractMethod.BATCH,
        data_format=ExtractFormat.PARQUET,
        location="s3://test-bucket/path",
        options={"inferSchema": "true"},
        schema=None,
    )


@pytest.fixture
def test_schema() -> StructType:
    """Return a test schema for ExtractFileModel."""
    return StructType([StructField("column1", StringType())])


class TestExtractFileModel:
    """Tests for ExtractFileModel class."""

    def test_extract_file_model_cls_initialization(self, extract_file_model_cls) -> None:
        """Test that ExtractFileModel can be initialized with valid parameters."""
        assert extract_file_model_cls.name == "test_extract_file"
        assert extract_file_model_cls.method == ExtractMethod.BATCH
        assert extract_file_model_cls.data_format == ExtractFormat.PARQUET
        assert extract_file_model_cls.location == "s3://test-bucket/path"
        assert extract_file_model_cls.options == {"inferSchema": "true"}
        assert extract_file_model_cls.schema is None

    @patch.object(SchemaFilepathHandler, "parse")
    def test_from_dict_valid(self, mock_schema_parse, valid_extract_dict, test_schema) -> None:
        """Test from_dict method with valid dictionary."""
        # Setup
        mock_schema_parse.return_value = test_schema

        # Execute
        model = ExtractFileModel.from_dict(valid_extract_dict)

        # Assert
        assert model.name == "test_extract_file"
        assert model.method == ExtractMethod.BATCH
        assert model.data_format == ExtractFormat.PARQUET
        assert model.location == "s3://test-bucket/path"
        assert model.options == {"inferSchema": "true"}
        assert model.schema == test_schema
        mock_schema_parse.assert_called_once_with(schema=Path("path/to/schema.json"))

    def test_from_dict_missing_key(self, valid_extract_dict) -> None:
        """Test from_dict method with missing key."""
        # Remove required 'name' key
        invalid_dict = valid_extract_dict.copy()
        del invalid_dict["name"]

        # Execute and Assert
        with pytest.raises(DictKeyError):
            ExtractFileModel.from_dict(invalid_dict)

    def test_from_dict_invalid_enum(self, valid_extract_dict) -> None:
        """Test from_dict method with invalid enum value."""
        # Use invalid 'method' value
        invalid_dict = valid_extract_dict.copy()
        invalid_dict["method"] = "invalid_method"

        # Execute and Assert
        with pytest.raises(ValueError):
            ExtractFileModel.from_dict(invalid_dict)
