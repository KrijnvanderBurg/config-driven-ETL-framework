"""Unit tests for the LoadModelFile class."""

from typing import Any

import pytest

from flint.models.model_load import DictKeyError, LoadFormat, LoadMethod, LoadMode, LoadModelFile


@pytest.fixture
def valid_load_dict() -> dict[str, Any]:
    """Return a valid dictionary for creating a LoadModelFile."""
    return {
        "name": "test_load",
        "upstream_name": "test_transform",
        "method": "batch",
        "mode": "append",
        "data_format": "parquet",
        "location": "s3://test-bucket/output",
        "schema_location": "s3://test-bucket/schema",
        "options": {"partitionBy": "date"},
    }


@pytest.fixture
def load_model_cls_file() -> LoadModelFile:
    """Return an initialized LoadModelFile instance."""
    return LoadModelFile(
        name="test_load",
        upstream_name="test_transform",
        method=LoadMethod.BATCH,
        mode=LoadMode.APPEND,
        data_format=LoadFormat.PARQUET,
        location="s3://test-bucket/output",
        schema_location="s3://test-bucket/schema",
        options={"partitionBy": "date"},
    )


class TestLoadModelFile:
    """Tests for LoadModelFile class."""

    def test_load_model_cls_file_initialization(self, load_model_cls_file: LoadModelFile) -> None:
        """Test that LoadModelFile can be initialized with valid parameters."""
        assert load_model_cls_file.name == "test_load"
        assert load_model_cls_file.upstream_name == "test_transform"
        assert load_model_cls_file.method == LoadMethod.BATCH
        assert load_model_cls_file.mode == LoadMode.APPEND
        assert load_model_cls_file.data_format == LoadFormat.PARQUET
        assert load_model_cls_file.location == "s3://test-bucket/output"
        assert load_model_cls_file.schema_location == "s3://test-bucket/schema"
        assert load_model_cls_file.options == {"partitionBy": "date"}

    def test_load_model_cls_file_from_dict_valid(self, valid_load_dict: dict[str, Any]) -> None:
        """Test from_dict method with valid dictionary."""
        # Execute
        model = LoadModelFile.from_dict(valid_load_dict)

        # Assert
        assert model.name == "test_load"
        assert model.upstream_name == "test_transform"
        assert model.method == LoadMethod.BATCH
        assert model.mode == LoadMode.APPEND
        assert model.data_format == LoadFormat.PARQUET
        assert model.location == "s3://test-bucket/output"
        assert model.schema_location == "s3://test-bucket/schema"
        assert model.options == {"partitionBy": "date"}

    def test_load_model_cls_file_from_dict_missing_optional(self, valid_load_dict: dict[str, Any]) -> None:
        """Test from_dict method with missing optional fields."""
        # Remove optional fields schema_location and options
        minimal_dict = valid_load_dict.copy()
        del minimal_dict["schema_location"]
        del minimal_dict["options"]

        # Execute
        model = LoadModelFile.from_dict(minimal_dict)

        # Assert
        assert model.name == "test_load"
        assert model.upstream_name == "test_transform"
        assert model.method == LoadMethod.BATCH
        assert model.mode == LoadMode.APPEND
        assert model.data_format == LoadFormat.PARQUET
        assert model.location == "s3://test-bucket/output"
        assert model.schema_location is None
        assert model.options == {}

    def test_load_model_cls_file_from_dict_missing_required(self, valid_load_dict: dict[str, Any]) -> None:
        """Test from_dict method with missing required key."""
        # Remove required 'name' key
        del valid_load_dict["name"]

        # Execute and Assert
        with pytest.raises(DictKeyError):
            LoadModelFile.from_dict(valid_load_dict)

    def test_load_model_cls_file_from_dict_invalid_enum(self, valid_load_dict: dict[str, Any]) -> None:
        """Test from_dict method with invalid enum value."""
        # Use invalid 'method' value
        valid_load_dict["method"] = "invalid_method"

        # Execute and Assert
        with pytest.raises(ValueError):
            LoadModelFile.from_dict(valid_load_dict)
