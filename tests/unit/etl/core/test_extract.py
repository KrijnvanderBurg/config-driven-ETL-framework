"""Unit tests for the extract module."""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrame

from flint.etl.core.extract import Extract, ExtractFile, ExtractRegistry
from flint.etl.models.model_extract import (
    DATA_FORMAT,
    LOCATION,
    METHOD,
    NAME,
    OPTIONS,
    SCHEMA,
    ExtractFileModel,
    ExtractFormat,
    ExtractMethod,
)
from flint.exceptions import FlintConfigurationKeyError
from flint.types import DataFrameRegistry
from flint.utils.schema import SchemaFilepathHandler


@pytest.fixture
def extract_file_model() -> ExtractFileModel:
    """Create a default ExtractFileModel instance for testing."""
    return ExtractFileModel(
        name="test_extract",
        method=ExtractMethod.BATCH,
        data_format=ExtractFormat.CSV,
        location="/path/to/test.csv",
        options={"header": "true", "inferSchema": "true"},
        schema=None,
    )


@pytest.fixture
def streaming_extract_file_model() -> ExtractFileModel:
    """Create a streaming ExtractFileModel instance for testing."""
    return ExtractFileModel(
        name="streaming_extract_test",
        method=ExtractMethod.STREAMING,
        data_format=ExtractFormat.CSV,
        location="/path/to/test.csv",
        options={"header": "true", "inferSchema": "true"},
        schema=None,
    )


class TestExtractRegistry:
    """Unit tests for the ExtractRegistry class."""

    def test_registry__singleton_pattern__returns_same_instance(self) -> None:
        """Test that ExtractRegistry implements singleton pattern correctly."""
        # Act
        registry1 = ExtractRegistry()
        registry2 = ExtractRegistry()

        # Assert
        assert registry1 is registry2
        assert id(registry1) == id(registry2)

    def test_register_and_get__valid_format__returns_registered_class(self) -> None:
        """Test registering and retrieving an extract class with valid format."""
        # Arrange
        registry = ExtractRegistry()

        # Create a test class to register
        class TestExtractClass:
            pass

        test_format = "test_format_unique"  # Use unique format to avoid conflicts

        # Act
        decorated_class = registry.register(test_format)(TestExtractClass)
        retrieved_class = registry.get(test_format)

        # Assert
        assert decorated_class == TestExtractClass
        assert retrieved_class == TestExtractClass

    def test_get__nonexistent_format__raises_key_error_with_available_keys(self) -> None:
        """Test that getting a non-existent extract format raises KeyError with helpful message."""
        # Arrange
        registry = ExtractRegistry()
        nonexistent_format = "absolutely_nonexistent_format"

        # Act & Assert
        with pytest.raises(KeyError) as exc_info:
            registry.get(nonexistent_format)

        assert "No class registered for key" in str(exc_info.value)
        assert nonexistent_format in str(exc_info.value)

    def test_register__duplicate_registration__does_not_duplicate_entries(self) -> None:
        """Test that registering the same class multiple times doesn't create duplicates."""
        # Arrange
        registry = ExtractRegistry()

        class TestExtractClass:
            pass

        test_format = "duplicate_test_format"

        # Act - Register the same class twice
        registry.register(test_format)(TestExtractClass)
        registry.register(test_format)(TestExtractClass)

        all_classes = registry.get_all(test_format)

        # Assert
        assert len(all_classes) == 1
        assert all_classes[0] == TestExtractClass


class TestExtract:
    """Unit tests for the Extract class and its implementations."""

    def test_init__valid_model__initializes_correctly(self, extract_file_model: ExtractFileModel) -> None:
        """Test Extract initialization with a valid model."""
        # Act
        extract = ExtractFile(model=extract_file_model)

        # Assert
        assert extract.model == extract_file_model
        assert extract.model.name == "test_extract"
        assert extract.model.method == ExtractMethod.BATCH
        assert extract.model.data_format == ExtractFormat.CSV
        assert isinstance(extract.data_registry, DataFrameRegistry)

    def test_from_dict__concrete_class_csv_format__creates_instance_with_correct_model(self) -> None:
        """Test creating ExtractFile from a dictionary using concrete class with CSV format."""
        # Arrange
        extract_dict = {
            NAME: "customer_extract",
            METHOD: "batch",
            DATA_FORMAT: "csv",
            LOCATION: "/data/customers.csv",
            SCHEMA: "/schemas/customers.json",
            OPTIONS: {"header": "true", "delimiter": ","},
        }

        # Mock justified: SchemaFilepathHandler.parse requires file system access
        with patch.object(SchemaFilepathHandler, "parse", return_value=None):
            # Act
            extract = ExtractFile.from_dict(extract_dict)

            # Assert
            assert isinstance(extract, ExtractFile)
            assert extract.model.name == "customer_extract"
            assert extract.model.method == ExtractMethod.BATCH
            assert extract.model.data_format == ExtractFormat.CSV
            assert extract.model.location == "/data/customers.csv"
            assert extract.model.options == {"header": "true", "delimiter": ","}
            assert extract.model.schema is None

    def test_from_dict__concrete_class_json_format__creates_instance_with_correct_model(self) -> None:
        """Test creating ExtractFile from a dictionary using concrete class with JSON format."""
        # Arrange
        extract_dict = {
            NAME: "order_extract",
            METHOD: "streaming",
            DATA_FORMAT: "json",
            LOCATION: "/data/orders/",
            SCHEMA: "/schemas/orders.json",
            OPTIONS: {"multiline": "true"},
        }

        # Mock justified: SchemaFilepathHandler.parse requires file system access
        with patch.object(SchemaFilepathHandler, "parse", return_value=None):
            # Act
            extract = ExtractFile.from_dict(extract_dict)

            # Assert
            assert isinstance(extract, ExtractFile)
            assert extract.model.name == "order_extract"
            assert extract.model.method == ExtractMethod.STREAMING
            assert extract.model.data_format == ExtractFormat.JSON
            assert extract.model.location == "/data/orders/"
            assert extract.model.options == {"multiline": "true"}

    def test_from_dict__concrete_class_parquet_format__creates_instance_with_correct_model(self) -> None:
        """Test creating ExtractFile from a dictionary using concrete class with Parquet format."""
        # Arrange
        extract_dict = {
            NAME: "analytics_extract",
            METHOD: "batch",
            DATA_FORMAT: "parquet",
            LOCATION: "/warehouse/analytics.parquet",
            SCHEMA: "/schemas/analytics.json",
            OPTIONS: {},
        }

        # Mock justified: SchemaFilepathHandler.parse requires file system access
        with patch.object(SchemaFilepathHandler, "parse", return_value=None):
            # Act
            extract = ExtractFile.from_dict(extract_dict)

            # Assert
            assert isinstance(extract, ExtractFile)
            assert extract.model.name == "analytics_extract"
            assert extract.model.method == ExtractMethod.BATCH
            assert extract.model.data_format == ExtractFormat.PARQUET
            assert extract.model.location == "/warehouse/analytics.parquet"
            assert extract.model.options == {}

    def test_from_dict__base_class_with_csv_format__creates_correct_subclass(self) -> None:
        """Test Extract.from_dict method with CSV format selects ExtractFile implementation."""
        # Arrange
        config = {
            DATA_FORMAT: "csv",
            NAME: "base_test_csv",
            METHOD: "batch",
            LOCATION: "/path/to/data.csv",
            SCHEMA: "/path/to/schema.json",
            OPTIONS: {"header": "true"},
        }

        # Mock justified: SchemaFilepathHandler.parse requires file system access
        with patch.object(SchemaFilepathHandler, "parse", return_value=None):
            # Act
            extract = Extract.from_dict(config)

            # Assert
            assert isinstance(extract, ExtractFile)
            assert extract.model.data_format == ExtractFormat.CSV
            assert extract.model.name == "base_test_csv"

    def test_from_dict__base_class_with_json_format__creates_correct_subclass(self) -> None:
        """Test Extract.from_dict method with JSON format selects ExtractFile implementation."""
        # Arrange
        config = {
            DATA_FORMAT: "json",
            NAME: "base_test_json",
            METHOD: "streaming",
            LOCATION: "/path/to/data.json",
            SCHEMA: "/path/to/schema.json",
            OPTIONS: {"multiline": "true"},
        }

        # Mock justified: SchemaFilepathHandler.parse requires file system access
        with patch.object(SchemaFilepathHandler, "parse", return_value=None):
            # Act
            extract = Extract.from_dict(config)

            # Assert
            assert isinstance(extract, ExtractFile)
            assert extract.model.data_format == ExtractFormat.JSON
            assert extract.model.name == "base_test_json"

    def test_from_dict__base_class_with_parquet_format__creates_correct_subclass(self) -> None:
        """Test Extract.from_dict method with Parquet format selects ExtractFile implementation."""
        # Arrange
        config = {
            DATA_FORMAT: "parquet",
            NAME: "base_test_parquet",
            METHOD: "batch",
            LOCATION: "/path/to/data.parquet",
            SCHEMA: "/path/to/schema.json",
            OPTIONS: {},
        }

        # Mock justified: SchemaFilepathHandler.parse requires file system access
        with patch.object(SchemaFilepathHandler, "parse", return_value=None):
            # Act
            extract = Extract.from_dict(config)

            # Assert
            assert isinstance(extract, ExtractFile)
            assert extract.model.data_format == ExtractFormat.PARQUET
            assert extract.model.name == "base_test_parquet"

    def test_from_dict__invalid_format__raises_value_error_with_format_details(self) -> None:
        """Test Extract.from_dict method with an invalid format raises ValueError from enum."""
        # Arrange
        config = {
            DATA_FORMAT: "invalid_format",
            NAME: "test",
            METHOD: "batch",
            LOCATION: "/path/to/data",
            SCHEMA: "/path/to/schema.json",
            OPTIONS: {},
        }

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            Extract.from_dict(config)

        assert "invalid_format" in str(exc_info.value)
        assert "not a valid ExtractFormat" in str(exc_info.value)

    def test_from_dict__missing_data_format__raises_not_implemented_error_with_clear_message(self) -> None:
        """Test Extract.from_dict method with missing data_format key raises NotImplementedError."""
        # Arrange
        config: dict[str, Any] = {
            NAME: "test",
            METHOD: "batch",
            LOCATION: "/path/to/data",
            OPTIONS: {},
        }

        # Act & Assert
        with pytest.raises(NotImplementedError) as exc_info:
            Extract.from_dict(config)

        assert "Extract format <missing> is not supported" in str(exc_info.value)

    def test_from_dict__missing_required_key__raises_flint_configuration_key_error(self) -> None:
        """Test Extract.from_dict method with missing required key raises FlintConfigurationKeyError."""
        # Arrange
        config = {
            DATA_FORMAT: "csv",
            # Missing NAME key
            METHOD: "batch",
            LOCATION: "/path/to/data.csv",
            SCHEMA: "/path/to/schema.json",
            OPTIONS: {},
        }

        # Act & Assert
        with pytest.raises(FlintConfigurationKeyError) as exc_info:
            Extract.from_dict(config)

        assert "name" in str(exc_info.value)

    def test_extract__batch_method__stores_dataframe_in_registry_with_correct_name(self) -> None:
        """Test extract method with batch extraction stores result in data registry with correct name."""
        # Arrange
        model = ExtractFileModel(
            name="batch_extract_test",
            method=ExtractMethod.BATCH,
            data_format=ExtractFormat.CSV,
            location="/path/to/test.csv",
            options={"header": "true", "inferSchema": "true"},
            schema=None,
        )
        extract = ExtractFile(model=model)
        mock_df = MagicMock(spec=DataFrame)

        # Mock justified: SparkHandler is external dependency requiring Spark session setup
        with patch.object(extract, "_spark") as mock_spark:
            mock_spark.session.read.load.return_value = mock_df
            mock_df.count.return_value = 100

            # Act
            extract.extract()

            # Assert
            assert extract.data_registry["batch_extract_test"] == mock_df
            assert "batch_extract_test" in extract.data_registry

    def test_extract__streaming_method__stores_dataframe_in_registry_with_correct_name(
        self, streaming_extract_file_model: ExtractFileModel
    ) -> None:
        """Test extract method with streaming extraction stores result in data registry with correct name."""
        # Arrange
        extract = ExtractFile(model=streaming_extract_file_model)
        mock_df = MagicMock(spec=DataFrame)

        # Mock justified: SparkHandler is external dependency requiring Spark session setup
        with patch.object(extract, "_spark") as mock_spark:
            mock_spark.session.readStream.load.return_value = mock_df

            # Act
            extract.extract()

            # Assert
            assert extract.data_registry["streaming_extract_test"] == mock_df
            assert "streaming_extract_test" in extract.data_registry

    def test_extract__batch_method__calls_spark_handler_add_configs(self) -> None:
        """Test extract method with batch method passes options to Spark read.load method."""
        # Arrange
        options = {"header": "true", "delimiter": "|", "encoding": "UTF-8"}
        model = ExtractFileModel(
            name="test_extract",
            method=ExtractMethod.BATCH,
            data_format=ExtractFormat.CSV,
            location="/path/to/test.csv",
            options=options,
            schema=None,
        )
        extract = ExtractFile(model=model)
        mock_df = MagicMock(spec=DataFrame)

        # Mock justified: SparkHandler is external dependency requiring Spark session setup
        with patch.object(extract, "_spark") as mock_spark:
            mock_spark.session.read.load.return_value = mock_df
            mock_df.count.return_value = 50

            # Act
            extract.extract()

            # Assert - Options should be passed to load method as **kwargs
            mock_spark.session.read.load.assert_called_once_with(
                path=model.location, format=model.data_format.value, schema=model.schema, **options
            )

    def test_extract__streaming_method__calls_spark_handler_add_configs(self) -> None:
        """Test extract method with streaming method passes options to Spark readStream.load method."""
        # Arrange
        options = {"maxFilesPerTrigger": "1", "latestFirst": "true"}
        model = ExtractFileModel(
            name="test_extract",
            method=ExtractMethod.STREAMING,
            data_format=ExtractFormat.CSV,
            location="/path/to/test.csv",
            options=options,
            schema=None,
        )
        extract = ExtractFile(model=model)
        mock_df = MagicMock(spec=DataFrame)

        # Mock justified: SparkHandler is external dependency requiring Spark session setup
        with patch.object(extract, "_spark") as mock_spark:
            mock_spark.session.readStream.load.return_value = mock_df

            # Act
            extract.extract()

            # Assert - Options should be passed to load method as **kwargs
            mock_spark.session.readStream.load.assert_called_once_with(
                path=model.location, format=model.data_format.value, schema=model.schema, **options
            )

    def test_extract__invalid_method__raises_value_error_with_method_details(
        self, extract_file_model: ExtractFileModel
    ) -> None:
        """Test extract method with invalid extraction method raises ValueError with clear message."""
        # Arrange
        extract = ExtractFile(model=extract_file_model)

        # Create a mock that looks like an enum but has an invalid value
        invalid_method = MagicMock()
        invalid_method.value = "invalid_extraction_method"
        extract.model.method = invalid_method

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            extract.extract()

        assert "Extraction method" in str(exc_info.value)
        assert "is not supported for PySpark" in str(exc_info.value)

    def test_model_cls__extract_file__returns_correct_model_class(self) -> None:
        """Test that ExtractFile.model_cls references the correct model class."""
        # Act & Assert
        assert ExtractFile.model_cls == ExtractFileModel

    def test_extract_file__concrete_class__has_model_cls_attribute(self) -> None:
        """Test that ExtractFile concrete class has model_cls attribute."""
        # Act & Assert
        assert hasattr(ExtractFile, "model_cls")
        assert ExtractFile.model_cls == ExtractFileModel

    def test_data_registry__multiple_extracts__stores_separate_dataframes(self) -> None:
        """Test that multiple extracts store separate DataFrames in the registry."""
        # Arrange
        model1 = ExtractFileModel(
            name="extract_1",
            method=ExtractMethod.BATCH,
            data_format=ExtractFormat.CSV,
            location="/path/to/test.csv",
            options={"header": "true", "inferSchema": "true"},
            schema=None,
        )
        model2 = ExtractFileModel(
            name="extract_2",
            method=ExtractMethod.BATCH,
            data_format=ExtractFormat.CSV,
            location="/path/to/test.csv",
            options={"header": "true", "inferSchema": "true"},
            schema=None,
        )
        extract1 = ExtractFile(model=model1)
        extract2 = ExtractFile(model=model2)

        mock_df1 = MagicMock(spec=DataFrame)
        mock_df2 = MagicMock(spec=DataFrame)

        # Mock justified: SparkHandler is external dependency requiring Spark session setup
        with patch.object(extract1, "_spark") as mock_spark1, patch.object(extract2, "_spark") as mock_spark2:
            mock_spark1.session.read.load.return_value = mock_df1
            mock_df1.count.return_value = 100

            # Act
            extract1.extract()

            # Setup for second extract
            mock_spark2.session.read.load.return_value = mock_df2
            mock_df2.count.return_value = 200
            extract2.extract()

            # Assert - Note: Each extract has its own registry due to instantiation
            assert extract1.data_registry["extract_1"] == mock_df1
            assert extract2.data_registry["extract_2"] == mock_df2
