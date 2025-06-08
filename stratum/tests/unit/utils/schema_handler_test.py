from unittest.mock import mock_open, patch

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from stratum.utils.file_handler import FileJsonHandler
from stratum.utils.schema_handler import SchemaHandlerPyspark


class TestSchemaHandlerPysparkSchemaFactory:
    @pytest.fixture
    def schema(self) -> StructType:
        return StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("job_title", StringType(), True),
            ]
        )

    def test__schema_from_factory__from_file(self, schema: StructType) -> None:
        """Test `schema_from_factory` returns the correct strategy for given file extension."""
        # Arrange
        with patch.object(SchemaHandlerPyspark, "from_file", return_value=schema):
            # Act
            schema_read = SchemaHandlerPyspark.schema_factory(schema="schema.json")

            # Assert
            assert schema_read == schema

    def test__schema_from_factory__from_string(self, schema: StructType) -> None:
        """Test `schema_from_factory` returns the correct strategy for given json string."""
        # Arrange
        with patch.object(SchemaHandlerPyspark, "from_json", return_value=schema):
            # Act
            schema_read = SchemaHandlerPyspark.schema_factory(schema=schema.json())

            # Assert
            assert schema_read == schema

    def test__schema_from_factory__unsupported_extension__raises_not_implemented_error(self) -> None:
        """Test `schema_from_factory` raises `NotImplementedError` for unsupported file extension."""
        with pytest.raises(NotImplementedError):  # Assert
            SchemaHandlerPyspark.schema_factory("fail.testfile")  # Act

    def test__schema_from_json(self, schema: StructType) -> None:
        """Test reading JSON data from a string."""
        # Act
        schema_read: StructType = SchemaHandlerPyspark.from_json(schema=schema.json())
        # Assert
        assert schema_read.json() == schema.json()

    def test__schema_from_json__json_decode_error(self) -> None:
        """Test reading JSON string raises `JSONDecodeError` when string contains invalid JSON."""
        # Arrange
        invalid_json = '{"name": "John" "age": 30}'  # Missing comma makes it invalid json
        with patch("builtins.open", mock_open(read_data=invalid_json)):
            with pytest.raises(ValueError):  # Assert
                # Act
                SchemaHandlerPyspark.from_json(schema="schema.json")

    def test__schema_from_json_file(self, schema: StructType) -> None:
        """Test reading pyspark struct from a file."""

        schema_mock = schema.json()

        # Arrange
        with (
            patch("builtins.open", mock_open(read_data=schema_mock)),
            patch.object(FileJsonHandler, "_file_exists", return_value=True),
        ):
            # with patch.object(SchemaHandlerPyspark, "from_file", return_value=schema_mock):
            # Act
            schema_read: StructType = SchemaHandlerPyspark.from_file(filepath="schema.json")

            # Assert
            assert schema_read == schema

    def test__schema_from_json_file__file_not_found_error(self) -> None:
        """Test reading JSON file raises `FileNotFoundError` when file does not exist."""
        # Arrange
        with patch("builtins.open", side_effect=FileNotFoundError):
            with pytest.raises(FileNotFoundError):  # Assert
                # Act
                SchemaHandlerPyspark.from_file(filepath="schema.json")

    def test__schema_from_json_file__file_permission_error(self) -> None:
        """Test reading JSON file raises `PermissionError` when `builtins.open()` raises `PermissionError`."""
        # Arrange
        with (
            patch("builtins.open", side_effect=PermissionError),
            patch.object(FileJsonHandler, "_file_exists", return_value=True),
        ):
            with pytest.raises(PermissionError):  # Assert
                # Act
                SchemaHandlerPyspark.from_file(filepath="schema.json")

    def test__schema_from_json_file__json_decode_error(self) -> None:
        """Test reading JSON file raises `JSONDecodeError` when file contains invalid JSON."""
        # Arrange
        invalid_json = '{"name": "John" "age": 30}'  # Missing comma makes it invalid json
        with (
            patch("builtins.open", mock_open(read_data=invalid_json)),
            patch.object(FileJsonHandler, "_file_exists", return_value=True),
        ):
            with pytest.raises(ValueError):  # Assert
                SchemaHandlerPyspark.from_file(filepath="schema.json")

    def test__schema_empty_return_none(self) -> None:
        """Test `schema` method returns `None` when schema is an empty string."""
        # Act
        schema = SchemaHandlerPyspark.schema_factory(schema="")
        # Assert
        assert schema is None
