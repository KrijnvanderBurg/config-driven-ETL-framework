"""
Test schema handler class.

| ✓ | Tests
|---|----------------------------------------------------------------
| ✓ | Create schema from a JSON file.
| ✓ | Create schema from a JSON string.
| ✓ | Raise JSONDecodeError when creating a schema from invalid JSON.
| ✓ | Raise TypeError when creating a schema from an invalid type.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

import json
from unittest import mock

import pytest
from datastore.schema import Schema
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# ============ Fixtures ================


@pytest.fixture(name="schema")
def fixture_test_schema() -> StructType:
    """
    Fixture for the schema of a test DataFrame.

    Returns:
        StructType: The schema of the test DataFrame.
    """
    # Arrange: Define the schema for the test DataFrame
    return StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("job_title", StringType(), True),
        ]
    )


@pytest.fixture(name="schema_file")
def fixture_test_schema_file(tmpdir, schema: StructType) -> str:
    """
    Fixture for the schema of a test DataFrame.

    Returns:
        StructType: The schema of the test DataFrame.
    """
    # Arrange: Define the schema for the test DataFrame
    # If path exists and is a file
    filepath = f"{tmpdir}/schema.json"

    with open(file=filepath, mode="w", encoding="utf-8") as f:
        json.dump(schema.json(), fp=f)
    return filepath


@pytest.fixture(name="json_invalid_schema")
def fixture_json_invalid_schema() -> str:
    """
    Fixture providing an invalid JSON string.

    Returns:
        str: valid json but invalid schema.

    """
    return '{"key": "value"}'


# ============== Tests =================


def test_schema_from_spec_return_type(schema: StructType, schema_file: str) -> None:
    """
    Assert that schema is returned primarily from schema or else schema file.

    Args:
        schema (StructType): Schema fixture.
        schema_file (str): Filepath to schema file fixture.
    """
    # Act
    test_schema = Schema.from_spec(schema=schema.json(), filepath=schema_file)

    # Assert
    if schema:
        assert isinstance(test_schema, StructType)
    if schema_file:
        assert isinstance(test_schema, StructType)
    if not schema and not schema_file:
        assert isinstance(test_schema, type(None))


def test_schema_from_spec_called(schema: StructType, schema_file: str) -> None:
    """
    Assert that from_json() or from_file() functions are called inside .from_spec() method
    for schema and schema_filepath respectively.

    Args:
        schema (StructType): Schema fixture.
        schema_file (str): Filepath to schema file fixture.
    """
    # Arrange
    with mock.patch.object(Schema, "from_json") as from_json_mock, mock.patch.object(
        Schema, "from_file"
    ) as from_file_mock:
        # Act
        Schema.from_spec(schema=schema.json(), filepath=schema_file)

    # Assert
    if schema:
        from_json_mock.assert_called_once_with(schema=schema.json())
    if schema_file and not schema:
        from_json_mock.assert_not_called()
        from_file_mock.assert_called_once_with(filepath=schema_file)
    if not schema and not schema_file:
        from_json_mock.assert_not_called()
        from_file_mock.assert_not_called()


def test_schema_from_file_json(schema: StructType, schema_file: str) -> None:
    """
    Assert that schema fixture equals schema extract from file.

    Args:
        schema (StructType): Schema fixture.
        schema_file (str): Filepath to schema file fixture.
    """
    # Assert
    assert schema == Schema.from_file(schema_file)


def test_schema_from_json(schema: StructType) -> None:
    """
    Assert that Schema is returned from a valid json schema string.

    Args:
        schema (StructType): Schema fixture.
    """
    # Arrange

    j = json.loads(schema.json())

    # Assert
    assert schema == StructType.fromJson(j)


def test_schema_from_json_invalid_json(json_invalid_schema):
    """'
    Assert that JSONDecodeError is raised when creating schema from invalid json.

    Args:
        json_invalid_schema (str): Invalid json for schema.
    """
    # Act
    with pytest.raises(KeyError):
        # Assert invalid json
        Schema.from_json(json_invalid_schema)
