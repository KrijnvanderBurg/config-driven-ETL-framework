"""
File extract class tests.

| ✓ | Tests
|---|------------------------------------------------------------------
| ✓ | Test extract method returns correct type.
| ✓ | Test writing a df and extract it results in equal df.
| ✓ | Test that protected methods call the respective extract method.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from collections.abc import Generator
from unittest import mock

import pytest
from datastore.extract.base import ExtractMethod, ExtractSpec
from datastore.extract.file import ExtractFile
from datastore.spark_handler import SparkHandler
from pyspark import testing
from pyspark.sql import DataFrame, SparkSession

# ============ Fixtures ============


@pytest.fixture(name="extract_file_matrix")
def fixture_extract_file(extract_spec_matrix: ExtractSpec) -> Generator[ExtractFile, None, None]:
    """
    Matrix fixture function to create ExtractFile instance.

    Args:
        extract_spec_matrix (ExtractSpec): Matrix ExtractSpec fixture.

    Yields:
        ExtractFile: A ExtractFile instance with the provided ExtractSpec.
    """
    yield ExtractFile(spec=extract_spec_matrix)


# ============== Tests =============


def test_extract_file_extract_type(df: DataFrame, extract_spec: ExtractSpec) -> None:
    """
    Assert that the return type of the extract method is a DataFrame.

    Args:
        df (Dataframe): DataFrame fixture.
        extract_spec (ExtractSpec): The ExtractSpec for testing.
    """
    # Arrange
    SparkHandler.get_or_create()
    extract_file = ExtractFile(spec=extract_spec)

    df.write.format(extract_file.spec.data_format.value).save(extract_file.spec.location)

    # Act
    return_type = extract_file.extract()

    # Assert
    assert isinstance(return_type, DataFrame)


def test_extract_file_extract_called(extract_file_matrix: ExtractFile) -> None:
    """
    Assert that protected methods are called for the respective method.

    Args:
        extract_file_matrix (ExtractFile): Matrix ExtractFile fixture.
    """
    # Arrange
    SparkHandler.get_or_create()

    with (
        mock.patch.object(extract_file_matrix, "_extract_batch") as extract_batch_mock,
        mock.patch.object(extract_file_matrix, "_extract_streaming") as extract_streaming_mock,
    ):
        # Act
        extract_file_matrix.extract()

    # Assert
    if extract_file_matrix.spec.method == ExtractMethod.BATCH:
        extract_batch_mock.assert_called_once()
        extract_streaming_mock.assert_not_called()

    if extract_file_matrix.spec.method == ExtractMethod.STREAMING:
        extract_streaming_mock.assert_called_once()
        extract_batch_mock.assert_not_called()


def test_extract_file_extract_df_equals(spark: SparkSession, df: DataFrame, extract_file_matrix: ExtractFile) -> None:
    """
    Assert that extract method writes the DataFrame without any modifications.

    Args:
        spark (SparkSession): SparkSession fixture.
        df (DataFrame): Test DataFrame fixture.
        extract_file_matrix (ExtractFile): Matrix ExtractFile fixture.

    Returns: None
    """
    # Arrange
    SparkHandler.get_or_create()
    df.write.format(extract_file_matrix.spec.data_format.value).save(extract_file_matrix.spec.location)

    # Act
    extract_df = extract_file_matrix.extract()

    if extract_file_matrix.spec.method == ExtractMethod.STREAMING:
        query = extract_df.writeStream.outputMode("append").format("memory").queryName("memory_table").start()
        query.awaitTermination(timeout=3)
        query.stop()
        extract_df = spark.sql("SELECT * FROM memory_table")

    # Assert
    testing.assertDataFrameEqual(actual=extract_df, expected=df)
