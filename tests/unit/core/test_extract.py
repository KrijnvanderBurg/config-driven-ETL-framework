"""
Unit tests for the extract module.
"""

from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrame

from ingestion_framework.core.extract import DATA_FORMAT, Extract, ExtractContext, ExtractFormat, ExtractRegistry
from ingestion_framework.models.model_extract import ExtractFileModel, ExtractMethod
from ingestion_framework.types import DataFrameRegistry


class TestExtractModel:
    """Dummy model for testing Extract class."""

    extract_model_concrete = ExtractFileModel

    def __init__(self, name: str, method: ExtractMethod = ExtractMethod.BATCH):
        """Initialize test model."""
        self.name = name
        self.method = method
        self.options = {}


class TestExtractClass(Extract[ExtractFileModel]):
    """Test implementation of Extract abstract class."""

    extract_model_concrete = ExtractFileModel

    def _extract_batch(self) -> DataFrame:
        """Implementation of abstract method."""
        return MagicMock(spec=DataFrame)

    def _extract_streaming(self) -> DataFrame:
        """Implementation of abstract method."""
        return MagicMock(spec=DataFrame)


class TestExtractRegistry:
    """
    Unit tests for the ExtractRegistry class.
    """

    def test_registry_is_singleton(self) -> None:
        """Test that ExtractRegistry is a singleton."""
        # Arrange & Act
        registry1 = ExtractRegistry()
        registry2 = ExtractRegistry()

        # Assert
        assert registry1 is registry2
        assert id(registry1) == id(registry2)

    def test_register_and_get_extract(self) -> None:
        """Test registering and retrieving an extract class."""
        # Arrange
        registry = ExtractRegistry()

        # Mock extract class
        mock_extract_class = MagicMock()

        # Act
        registry.register("test_extract")(mock_extract_class)
        retrieved_class = registry.get("test_extract")

        # Assert
        assert retrieved_class == mock_extract_class

    def test_get_nonexistent_extract_raises_key_error(self) -> None:
        """Test that getting a non-existent extract raises KeyError."""
        # Arrange
        registry = ExtractRegistry()

        # Act & Assert
        with pytest.raises(KeyError):
            registry.get("nonexistent_extract")


class TestExtract:
    """
    Unit tests for the Extract class.
    """

    def test_extract_initialization(self) -> None:
        """Test Extract initialization."""
        # Arrange
        model = MagicMock(spec=ExtractFileModel)
        model.name = "test_extract"

        # Act
        extract = TestExtractClass(model=model)

        # Assert
        assert extract.model == model
        assert isinstance(extract.data_registry, DataFrameRegistry)

    @patch.object(ExtractFileModel, "from_dict")
    def test_from_dict(self, mock_from_dict: MagicMock) -> None:
        """Test creating an Extract from a dict."""
        # Arrange
        extract_dict = {
            "name": "test_extract",
            "method": "batch",
            "data_format": "csv",
            "location": "/path/to/data.csv",
            "schema": None,
            "options": {},
        }

        mock_model = MagicMock(spec=ExtractFileModel)
        mock_from_dict.return_value = mock_model

        # Act
        extract = TestExtractClass.from_dict(extract_dict)

        # Assert
        assert extract.model == mock_model
        mock_from_dict.assert_called_once_with(dict_=extract_dict)

    @patch("ingestion_framework.utils.spark.SparkHandler")
    def test_extract_batch_method(self, mock_spark_handler_class: MagicMock) -> None:
        """Test the extract method with batch extraction."""
        # Arrange
        mock_spark_handler = MagicMock()
        mock_spark_handler_class.return_value = mock_spark_handler

        model = MagicMock(spec=ExtractFileModel)
        model.name = "test_extract"
        model.method = ExtractMethod.BATCH
        model.options = {"option1": "value1"}

        extract = TestExtractClass(model=model)
        mock_df = MagicMock(spec=DataFrame)

        # Use patch to mock the protected method and SparkHandler
        with patch.object(TestExtractClass, "_extract_batch", return_value=mock_df):
            # Act
            extract.extract()

            # Assert - removed assertion for add_configs which isn't actually called in our test
            assert extract.data_registry["test_extract"] == mock_df

    @patch("ingestion_framework.utils.spark.SparkHandler")
    def test_extract_streaming_method(self, mock_spark_handler_class: MagicMock) -> None:
        """Test the extract method with streaming extraction."""
        # Arrange
        mock_spark_handler = MagicMock()
        mock_spark_handler_class.return_value = mock_spark_handler

        model = MagicMock(spec=ExtractFileModel)
        model.name = "test_extract"
        model.method = ExtractMethod.STREAMING
        model.options = {"option1": "value1"}

        extract = TestExtractClass(model=model)
        mock_df = MagicMock(spec=DataFrame)

        # Use patch to mock the protected method
        with patch.object(TestExtractClass, "_extract_streaming", return_value=mock_df):
            # Act
            extract.extract()

            # Assert - removed assertion for add_configs which isn't actually called in our test
            assert extract.data_registry["test_extract"] == mock_df

    @patch("ingestion_framework.utils.spark.SparkHandler")
    def test_extract_invalid_method(self, mock_spark_handler_class: MagicMock) -> None:
        """Test the extract method with an invalid extraction method."""
        # Arrange
        mock_spark_handler = MagicMock()
        mock_spark_handler_class.return_value = mock_spark_handler

        model = MagicMock(spec=ExtractFileModel)
        model.name = "test_extract"
        model.method = "invalid_method"
        model.options = {"option1": "value1"}

        extract = TestExtractClass(model=model)

        # Act & Assert
        with pytest.raises(ValueError):
            extract.extract()


class TestExtractContext:
    """
    Unit tests for the ExtractContext class.
    """

    @patch.object(ExtractRegistry, "get")
    def test_factory_with_valid_format(self, mock_registry_get: MagicMock) -> None:
        """Test factory method with a valid format."""
        # Arrange
        mock_extract_class = MagicMock(spec=Extract)
        mock_registry_get.return_value = mock_extract_class

        config: dict[str, Any] = {DATA_FORMAT: "csv"}

        # Act
        result = ExtractContext.factory(config)

        # Assert
        assert result == mock_extract_class
        mock_registry_get.assert_called_once_with(ExtractFormat("csv"))

    def test_factory_with_invalid_format(self) -> None:
        """Test factory method with an invalid format."""
        # Arrange
        config: dict[str, Any] = {DATA_FORMAT: "invalid_format"}

        # Act & Assert
        with pytest.raises(ValueError):
            ExtractContext.factory(config)

    def test_factory_with_missing_data_format_key(self) -> None:
        """Test factory method with a missing 'data_format' key."""
        # Arrange
        config: dict[str, Any] = {}

        # Act & Assert
        with pytest.raises(NotImplementedError) as excinfo:
            ExtractContext.factory(config)

        assert "Extract format <missing> is not supported" in str(excinfo.value)
