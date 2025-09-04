"""Unit tests for the load module."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.streaming.query import StreamingQuery

from flint.etl.core.load import (
    DATA_FORMAT,
    LOCATION,
    METHOD,
    MODE,
    NAME,
    OPTIONS,
    SCHEMA_LOCATION,
    UPSTREAM_NAME,
    Load,
    LoadFile,
    LoadRegistry,
)
from flint.etl.models.model_load import LoadFormat, LoadMethod, LoadMode, LoadModelFile
from flint.types import DataFrameRegistry, StreamingQueryRegistry


@pytest.fixture
def batch_load_model() -> LoadModelFile:
    """Create a batch LoadModelFile for testing."""
    return LoadModelFile(
        name="test_load",
        upstream_name="source",
        method=LoadMethod.BATCH,
        mode=LoadMode.OVERWRITE,
        data_format=LoadFormat.CSV,
        location="/path/to/output.csv",
        schema_location=None,
        options={"header": "true"},
    )


@pytest.fixture
def streaming_load_model() -> LoadModelFile:
    """Create a streaming LoadModelFile for testing."""
    return LoadModelFile(
        name="streaming_load",
        upstream_name="source",
        method=LoadMethod.STREAMING,
        mode=LoadMode.APPEND,
        data_format=LoadFormat.JSON,
        location="/path/to/streaming.json",
        schema_location=None,
        options={"multiLine": "true"},
    )


@pytest.fixture
def load_model_with_schema() -> LoadModelFile:
    """Create a LoadModelFile with schema location for testing."""
    return LoadModelFile(
        name="test_load_with_schema",
        upstream_name="source",
        method=LoadMethod.BATCH,
        mode=LoadMode.OVERWRITE,
        data_format=LoadFormat.CSV,
        location="/path/to/output.csv",
        schema_location="/path/to/schema.json",
        options={"header": "true"},
    )


class TestLoadRegistry:
    """Unit tests for the LoadRegistry class."""

    def test_registry__singleton_pattern__returns_same_instance(self):
        """Test that LoadRegistry follows singleton pattern."""
        registry1 = LoadRegistry()
        registry2 = LoadRegistry()

        assert registry1 is registry2
        assert id(registry1) == id(registry2)

    def test_register_and_get__valid_format__returns_registered_class(self):
        """Test registering and retrieving a load class."""
        registry = LoadRegistry()

        # LoadFile should already be registered for CSV format
        retrieved_class = registry.get(LoadFormat.CSV)
        assert retrieved_class == LoadFile

    def test_get__nonexistent_format__raises_key_error(self):
        """Test that getting a non-existent load raises KeyError."""
        registry = LoadRegistry()

        # Test with a format that doesn't exist - need to create a fake one
        with pytest.raises(KeyError):
            # This should raise KeyError as no format is registered with this enum value
            fake_format = MagicMock()
            fake_format.value = "nonexistent_format"
            registry.get(fake_format)


class TestLoadFile:
    """Unit tests for the LoadFile concrete implementation."""

    def test_init__valid_model__initializes_correctly(self, batch_load_model: LoadModelFile):
        """Test LoadFile initialization with valid model."""
        load = LoadFile(model=batch_load_model)

        assert load.model == batch_load_model
        assert isinstance(load.data_registry, DataFrameRegistry)
        assert isinstance(load.streaming_query_registry, StreamingQueryRegistry)

    def test_from_dict__concrete_class__creates_instance_with_correct_model(self):
        """Test creating LoadFile from dict using concrete class."""
        load_dict = {
            NAME: "test_load",
            UPSTREAM_NAME: "source",
            METHOD: "batch",
            MODE: "overwrite",
            DATA_FORMAT: "csv",
            LOCATION: "/path/to/output.csv",
            SCHEMA_LOCATION: None,
            OPTIONS: {"header": "true"},
        }

        load = LoadFile.from_dict(load_dict)

        assert isinstance(load, LoadFile)
        assert load.model.name == "test_load"
        assert load.model.upstream_name == "source"
        assert load.model.method == LoadMethod.BATCH
        assert load.model.data_format == LoadFormat.CSV
        assert load.model.location == "/path/to/output.csv"
        assert load.model.options == {"header": "true"}

    def test_load__copies_dataframe_to_load_name(self, batch_load_model: LoadModelFile):
        """Test that load method copies dataframe from upstream to load name."""
        load = LoadFile(model=batch_load_model)

        # Create real dataframe registry with mock dataframe (DataFrame requires Spark)
        mock_df = MagicMock(spec=DataFrame)
        load.data_registry["source"] = mock_df

        # Mock justified: SparkHandler and file operations require external Spark session
        with (
            patch("flint.etl.core.load.SparkHandler") as mock_spark_handler,
            patch.object(mock_df, "write") as mock_write,
        ):
            mock_handler_instance = MagicMock()
            mock_spark_handler.return_value = mock_handler_instance
            mock_write.save = MagicMock()
            mock_df.count.return_value = 100

            load.load()

            # Verify dataframe was copied to load name
            assert load.data_registry["test_load"] == mock_df

    def test_load__batch_method__calls_spark_configurations(self, batch_load_model: LoadModelFile):
        """Test that batch load adds Spark configurations."""
        load = LoadFile(model=batch_load_model)

        mock_df = MagicMock(spec=DataFrame)
        load.data_registry["source"] = mock_df

        # Mock justified: SparkHandler is external dependency requiring Spark session setup
        with (
            patch("flint.etl.core.load.SparkHandler") as mock_spark_handler,
            patch.object(mock_df, "write") as mock_write,
        ):
            mock_handler_instance = MagicMock()
            mock_spark_handler.return_value = mock_handler_instance
            mock_write.save = MagicMock()
            mock_df.count.return_value = 100

            load.load()

            # Verify Spark configurations were added
            mock_handler_instance.add_configs.assert_called_once_with(options=batch_load_model.options)

    def test_load__batch_method__calls_dataframe_write_with_correct_parameters(self, batch_load_model: LoadModelFile):
        """Test that batch load calls DataFrame write with correct parameters."""
        load = LoadFile(model=batch_load_model)

        mock_df = MagicMock(spec=DataFrame)
        load.data_registry["source"] = mock_df

        # Mock justified: SparkHandler and DataFrame write require external Spark operations
        with (
            patch("flint.etl.core.load.SparkHandler"),
            patch.object(mock_df, "write") as mock_write,
        ):
            mock_write.save = MagicMock()
            mock_df.count.return_value = 100

            load.load()

            # Verify write was called with correct parameters
            mock_write.save.assert_called_once_with(
                path="/path/to/output.csv", format="csv", mode="overwrite", header="true"
            )

    def test_load__streaming_method__starts_writestream_with_correct_parameters(
        self, streaming_load_model: LoadModelFile
    ):
        """Test that streaming load starts writeStream with correct parameters."""
        load = LoadFile(model=streaming_load_model)

        mock_df = MagicMock(spec=DataFrame)
        mock_streaming_query = MagicMock(spec=StreamingQuery)
        mock_streaming_query.id = "test_query_id"

        load.data_registry["source"] = mock_df

        # Mock justified: SparkHandler and DataFrame writeStream require external Spark operations
        with (
            patch("flint.etl.core.load.SparkHandler"),
            patch.object(mock_df, "writeStream") as mock_write_stream,
        ):
            mock_write_stream.start = MagicMock(return_value=mock_streaming_query)

            load.load()

            # Verify writeStream was called with correct parameters
            mock_write_stream.start.assert_called_once_with(
                path="/path/to/streaming.json", format="json", outputMode="append", multiLine="true"
            )

    def test_load__streaming_method__registers_streaming_query(self, streaming_load_model: LoadModelFile):
        """Test that streaming load registers the streaming query."""
        load = LoadFile(model=streaming_load_model)

        mock_df = MagicMock(spec=DataFrame)
        mock_streaming_query = MagicMock(spec=StreamingQuery)
        mock_streaming_query.id = "test_query_id"

        load.data_registry["source"] = mock_df

        # Mock justified: SparkHandler and DataFrame writeStream require external Spark operations
        with (
            patch("flint.etl.core.load.SparkHandler"),
            patch.object(mock_df, "writeStream") as mock_write_stream,
        ):
            mock_write_stream.start = MagicMock(return_value=mock_streaming_query)

            load.load()

            # Verify streaming query was registered
            assert load.streaming_query_registry["streaming_load"] == mock_streaming_query

    def test_load__invalid_method__raises_value_error_with_method_details(self, batch_load_model: LoadModelFile):
        """Test load method with invalid loading method raises ValueError."""
        load = LoadFile(model=batch_load_model)

        mock_df = MagicMock(spec=DataFrame)
        load.data_registry["source"] = mock_df

        # Create invalid method by mocking the enum value
        invalid_method = MagicMock()
        invalid_method.value = "invalid_method"
        load.model.method = invalid_method

        # Mock justified: SparkHandler is external dependency requiring Spark session setup
        with patch("flint.etl.core.load.SparkHandler"):
            with pytest.raises(ValueError) as exc_info:
                load.load()

            assert "Loading method" in str(exc_info.value)
            assert "is not supported for PySpark" in str(exc_info.value)

    def test_load__schema_location_provided__exports_schema_to_file(self, load_model_with_schema: LoadModelFile):
        """Test that schema is exported when schema_location is provided."""
        load = LoadFile(model=load_model_with_schema)

        mock_df = MagicMock(spec=DataFrame)
        mock_schema = MagicMock()
        mock_schema.jsonValue.return_value = {"fields": [{"name": "col1", "type": "string"}]}
        mock_df.schema = mock_schema
        load.data_registry["source"] = mock_df

        # Use named temporary file instead of mocking file operations
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as temp_file:
            temp_schema_path = temp_file.name
            load.model.schema_location = temp_schema_path

            # Mock justified: SparkHandler and DataFrame operations require external Spark
            with (
                patch("flint.etl.core.load.SparkHandler"),
                patch.object(mock_df, "write") as mock_write,
            ):
                mock_write.save = MagicMock()
                mock_df.count.return_value = 100

                load.load()

            # Verify schema was written to file
            written_content = Path(temp_schema_path).read_text(encoding="utf-8")
            expected_schema = {"fields": [{"name": "col1", "type": "string"}]}
            assert json.loads(written_content) == expected_schema

        # Clean up
        Path(temp_schema_path).unlink()

    def test_load__no_schema_location__skips_schema_export(self, batch_load_model: LoadModelFile):
        """Test that schema export is skipped when schema_location is None."""
        load = LoadFile(model=batch_load_model)  # Uses fixture with schema_location=None

        mock_df = MagicMock(spec=DataFrame)
        load.data_registry["source"] = mock_df

        # Mock justified: SparkHandler and DataFrame operations require external Spark
        with (
            patch("flint.etl.core.load.SparkHandler"),
            patch.object(mock_df, "write") as mock_write,
        ):
            mock_write.save = MagicMock()
            mock_df.count.return_value = 100

            # No assertion needed - if schema export happens with None location, it would raise an error
            load.load()  # Should complete without error

    def test_model_cls__load_file__returns_correct_model_class(self):
        """Test that LoadFile.model_cls references the correct model class."""
        assert LoadFile.model_cls == LoadModelFile


class TestLoad:
    """Unit tests for the Load base class."""

    def test_from_dict__base_class_with_csv_format__creates_correct_subclass(self):
        """Test Load.from_dict with CSV format creates LoadFile instance."""
        config = {
            DATA_FORMAT: "csv",
            NAME: "test_load",
            UPSTREAM_NAME: "source",
            METHOD: "batch",
            MODE: "overwrite",
            LOCATION: "/path/to/output.csv",
            SCHEMA_LOCATION: None,
            OPTIONS: {},
        }

        result = Load.from_dict(config)

        assert isinstance(result, LoadFile)
        assert result.model.name == "test_load"
        assert result.model.upstream_name == "source"
        assert result.model.method == LoadMethod.BATCH
        assert result.model.mode == LoadMode.OVERWRITE
        assert result.model.data_format == LoadFormat.CSV
        assert result.model.location == "/path/to/output.csv"
        assert result.model.schema_location is None
        assert result.model.options == {}

    def test_from_dict__base_class_with_json_format__creates_correct_subclass(self):
        """Test Load.from_dict with JSON format creates LoadFile instance."""
        config = {
            DATA_FORMAT: "json",
            NAME: "test_load",
            UPSTREAM_NAME: "source",
            METHOD: "batch",
            MODE: "overwrite",
            LOCATION: "/path/to/output.json",
            SCHEMA_LOCATION: None,
            OPTIONS: {},
        }

        result = Load.from_dict(config)

        assert isinstance(result, LoadFile)
        assert result.model.name == "test_load"
        assert result.model.upstream_name == "source"
        assert result.model.method == LoadMethod.BATCH
        assert result.model.mode == LoadMode.OVERWRITE
        assert result.model.data_format == LoadFormat.JSON
        assert result.model.location == "/path/to/output.json"
        assert result.model.schema_location is None
        assert result.model.options == {}

    def test_from_dict__base_class_with_parquet_format__creates_correct_subclass(self):
        """Test Load.from_dict with Parquet format creates LoadFile instance."""
        config = {
            DATA_FORMAT: "parquet",
            NAME: "test_load",
            UPSTREAM_NAME: "source",
            METHOD: "batch",
            MODE: "overwrite",
            LOCATION: "/path/to/output.parquet",
            SCHEMA_LOCATION: None,
            OPTIONS: {},
        }

        result = Load.from_dict(config)

        assert isinstance(result, LoadFile)
        assert result.model.name == "test_load"
        assert result.model.upstream_name == "source"
        assert result.model.method == LoadMethod.BATCH
        assert result.model.mode == LoadMode.OVERWRITE
        assert result.model.data_format == LoadFormat.PARQUET
        assert result.model.location == "/path/to/output.parquet"
        assert result.model.schema_location is None
        assert result.model.options == {}

    def test_from_dict__invalid_format__raises_value_error_with_format_details(self):
        """Test Load.from_dict with invalid format raises ValueError."""
        config = {DATA_FORMAT: "invalid_format"}

        with pytest.raises(ValueError) as exc_info:
            Load.from_dict(config)

        assert "invalid_format" in str(exc_info.value)
        assert "not a valid LoadFormat" in str(exc_info.value)

    def test_from_dict__missing_data_format__raises_not_implemented_error_with_clear_message(self):
        """Test Load.from_dict with missing data_format key raises NotImplementedError."""
        config = {}

        with pytest.raises(NotImplementedError) as exc_info:
            Load.from_dict(config)

        assert "Load format <missing> is not supported" in str(exc_info.value)
