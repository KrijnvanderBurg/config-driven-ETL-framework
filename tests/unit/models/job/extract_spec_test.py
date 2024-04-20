"""
Extract class tests.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from unittest.mock import mock_open, patch

import jsonschema
import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from datastore.models.job.extract_spec import ExtractFormat, ExtractMethod, ExtractSpecPyspark
from datastore.utils.json_handler import JsonHandler


class TestExtractMethod:
    """
    Test class for ExtractMethod enum.
    """

    extract_spec__method__values = [
        "batch",
        "streaming",
    ]  # Purposely not dynamically get all values as trip wire

    if extract_spec__method__values.sort() == sorted([enum.value for enum in ExtractMethod]):  # type: ignore
        raise pytest.fail(
            "Testing values does not match ExtractMethod values."
            "Did you add or remove some? update tests and fixtures accordingly."
        )

    @pytest.mark.parametrize("valid_value", extract_spec__method__values)
    def test__valid_members(self, valid_value: str) -> None:
        """
        Test creating ExtractMethod from valid values.

        Args:
            valid_value (str): Valid values for ExtractMethod.
        """
        # Act
        extract_method = ExtractMethod(valid_value)

        # Assert
        assert isinstance(extract_method, ExtractMethod)
        assert extract_method.value == valid_value


class TestExtractFormat:
    """
    Test class for ExtractFormat enum.
    """

    extract_spec__format__values = [
        "parquet",
        "json",
        "csv",
    ]  # Purposely not dynamically get all values as trip wire

    if extract_spec__format__values.sort() == sorted([enum.value for enum in ExtractFormat]):  # type: ignore
        raise pytest.fail(
            "Testing values does not match ExtractMethod values."
            "Did you add or remove some? update tests and fixtures accordingly."
        )

    @pytest.mark.parametrize("valid_value", extract_spec__format__values)
    def test__valid_members(self, valid_value: str) -> None:
        """
        Test creating ExtractFormat from valid values.

        Args:
            valid_value (str): Valid values for ExtractFormat.
        """
        # Act
        extract_format = ExtractFormat(valid_value)

        # Assert
        assert isinstance(extract_format, ExtractFormat)
        assert extract_format.value == valid_value


# ExtractSpecPyspark fixtures


@pytest.fixture(name="schema_pyspark")
def fixture_schema_pyspark() -> StructType:
    """
    Fixture for a Spark DataFrame with three columns: name, age, job_title.

    Returns:
        StructType: Fixture schema
    """
    return StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("job_title", StringType(), True),
        ]
    )


@pytest.fixture(name="extract_spec_pyspark")
def fixture__extract_spec__pyspark(schema_pyspark: StructType) -> ExtractSpecPyspark:
    """
    Fixture for creating ExtractSpec from valid value.

    Args:
        schema_pyspark (StructType): schema pyspark fixture.

    Returns:
        ExtractSpecPyspark: ExtractSpecPyspark fixture.
    """
    with (patch.object(ExtractSpecPyspark, "schema_factory", return_value=schema_pyspark),):
        return ExtractSpecPyspark(
            name="bronze-test-extract-dev",
            method=ExtractMethod.BATCH.value,
            data_format=ExtractFormat.CSV.value,
            location=f"path/extract.{ExtractFormat.CSV.value}",
            schema=schema_pyspark.json(),
        )


@pytest.fixture(name="extract_spec_confeti_pyspark")
def fixture__extract_spec__confeti__pyspark(
    extract_spec_pyspark: ExtractSpecPyspark,
) -> dict:
    """
    Fixture for creating ExtractSpec from confeti.

    Args:
        extract_spec_pyspark (ExtractSpecPyspark): ExtractSpecPyspark instance fixture.

    Returns:
        dict: ExtractSpecPyspark confeti fixture.
    """
    with (
        patch.object(
            ExtractSpecPyspark,
            "schema_factory",
            return_value=extract_spec_pyspark.schema,
        ),
    ):
        return {
            "name": extract_spec_pyspark.name,
            "method": extract_spec_pyspark.method.value,
            "data_format": extract_spec_pyspark.data_format.value,
            "location": f"path/extract.{extract_spec_pyspark.data_format.value}",
            "schema": extract_spec_pyspark.schema.json(),  # type: ignore[union-attr]
        }


# ExtractSpecPyspark tests


class TestExtractSpecPyspark:
    """
    Test class for ExtractSpec class.
    """

    def test__init(self, extract_spec_pyspark: ExtractSpecPyspark) -> None:
        """
        Assert that all ExtractSpecPyspark attributes are of correct type.

        Args:
            extract_spec_pyspark (ExtractSpecPyspark): Instance of ExtractSpecPyspark.
        """
        # Assert
        assert isinstance(extract_spec_pyspark.name, str)
        assert isinstance(extract_spec_pyspark.method, ExtractMethod)
        assert isinstance(extract_spec_pyspark.data_format, ExtractFormat)
        assert isinstance(extract_spec_pyspark.location, str)
        assert isinstance(extract_spec_pyspark.schema, StructType)

    def test__from_confeti(
        self,
        extract_spec_pyspark: ExtractSpecPyspark,
        extract_spec_confeti_pyspark: dict,
    ) -> None:
        """
        Assert that all ExtractSpecPyspark attributes are of correct type.

        Args:
            extract_spec_pyspark (ExtractSpecPyspark): Instance of ExtractSpecPyspark fixture.
            extract_spec_confeti_pyspark (dict): ExtractSpecPyspark confeti fixture.
        """
        # Act
        extract_spec = ExtractSpecPyspark.from_confeti(confeti=extract_spec_confeti_pyspark)

        # Assert
        assert extract_spec_pyspark.method == extract_spec.method
        assert extract_spec_pyspark.data_format == extract_spec.data_format
        assert extract_spec_pyspark.location == extract_spec.location
        assert extract_spec_pyspark.schema == extract_spec.schema

    @pytest.mark.parametrize("missing_property", ["name", "method", "data_format", "location"])
    def test__validate_json__missing_required_properties(
        self, missing_property: str, extract_spec_confeti_pyspark: dict
    ):
        """
        Test case for validating that each required property is missing one at a time.

        Args:
            missing_property (str): Property to be removed for testing.
            extract_spec_confeti_pyspark (dict): ExtractSpecPyspark confeti fixture.
        """
        with pytest.raises(jsonschema.ValidationError):  # Assert
            # Act
            del extract_spec_confeti_pyspark[missing_property]
            JsonHandler.validate_json(extract_spec_confeti_pyspark, ExtractSpecPyspark.confeti_schema)

    def test__schema_from_factory__from_file(self, schema_pyspark: StructType) -> None:
        """Test `schema_from_factory` returns the correct strategy for given file extension."""
        # Arrange
        with (patch.object(ExtractSpecPyspark, "schema_from_json_file", return_value=schema_pyspark),):
            # Act
            schema_read = ExtractSpecPyspark.schema_factory(schema="schema.json")

            # Assert
            assert schema_read == schema_pyspark

    def test__schema_from_factory__from_string(self, schema_pyspark: StructType) -> None:
        """Test `schema_from_factory` returns the correct strategy for given json string."""
        # Arrange
        with (patch.object(ExtractSpecPyspark, "schema_from_json", return_value=schema_pyspark),):
            # Act
            schema_read = ExtractSpecPyspark.schema_factory(schema=schema_pyspark.json())

            # Assert
            assert schema_read == schema_pyspark

    def test__schema_from_factory__unsupported_extension__raises_not_implemented_error(
        self,
    ) -> None:
        """Test `schema_from_factory` raises `NotImplementedError` for unsupported file extension."""
        with pytest.raises(NotImplementedError):  # Assert
            ExtractSpecPyspark.schema_factory("fail.testfile")  # Act

    def test__schema_from_json(self, schema_pyspark: StructType) -> None:
        """Test reading JSON data from a string."""
        # Act
        schema_read: StructType = ExtractSpecPyspark.schema_from_json(schema=schema_pyspark.json())
        # Assert
        assert schema_read.json() == schema_pyspark.json()

    def test__schema_from_json__json_decode_error(self) -> None:
        """Test reading JSON string raises `JSONDecodeError` when string contains invalid JSON."""
        # Arrange
        invalid_json = '{"name": "John" "age": 30}'  # Missing comma makes it invalid json
        with (patch("builtins.open", mock_open(read_data=invalid_json)),):
            with pytest.raises(ValueError):  # Assert
                # Act
                ExtractSpecPyspark.schema_from_json(schema="schema.json")

    def test__schema_from_json_file(self, schema_pyspark: StructType) -> None:
        """Test reading pyspark struct from a file."""
        # Arrange
        with (patch("builtins.open", mock_open(read_data=schema_pyspark.json())),):
            # Act
            schema_read: StructType = ExtractSpecPyspark.schema_from_json_file(filepath="schema.json")

            # Assert
            assert schema_read == schema_pyspark

    def test__schema_from_json_file__file_not_found_error(self) -> None:
        """Test reading JSON file raises `FileNotFoundError` when file does not exist."""
        # Arrange
        with (patch("builtins.open", side_effect=FileNotFoundError),):
            with pytest.raises(FileNotFoundError):  # Assert
                # Act
                ExtractSpecPyspark.schema_from_json_file(filepath="schema.json")

    def test__schema_from_json_file__file_permission_error(self) -> None:
        """Test reading JSON file raises `PermissionError` when `builtins.open()` raises `PermissionError`."""
        # Arrange
        with (patch("builtins.open", side_effect=PermissionError),):
            with pytest.raises(PermissionError):  # Assert
                # Act
                ExtractSpecPyspark.schema_from_json_file(filepath="schema.json")

    def test__schema_from_json_file__json_decode_error(self) -> None:
        """Test reading JSON file raises `JSONDecodeError` when file contains invalid JSON."""
        # Arrange
        invalid_json = '{"name": "John" "age": 30}'  # Missing comma makes it invalid json
        with (patch("builtins.open", mock_open(read_data=invalid_json)),):
            with pytest.raises(ValueError):  # Assert
                ExtractSpecPyspark.schema_from_json_file(filepath="schema.json")
