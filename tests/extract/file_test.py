"""
File extract class tests.

# | ✓ | Tests
# |---|----------------------------------------------------------------
# | ✓ | Correct functions are called in .extract() for batch and streaming.
# | ✓ | Writing to and extracting from file results in equal dataframe.
# | ✓ | Read returns the correct object instance.

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
from pyspark import testing
from pyspark.sql import DataFrame, SparkSession

# ==================================
# ======== ExtractFile class ========
# ==================================

# ============ Fixtures ============


@pytest.fixture(name="extract_file")
def fixture_extract_file(extract_spec: ExtractSpec) -> Generator[ExtractFile, None, None]:
    """
    Fixture function to create a ExtractFile instance for testing.

    Args:
        extract_spec (ExtractSpec): The ExtractSpec for testing.

    Yields:
        ExtractFile: A ExtractFile instance with the provided ExtractSpec.
    """
    yield ExtractFile(spec=extract_spec)


# ============== Tests =================


def test_extract_file_extract_called(extract_file: ExtractFile) -> None:
    """
    Assert that _extract_batch() and _extract_streaming() functions are called inside .extract() method
    for batch and streaming respectively.

    Args:
        extract_file (ExtractFile): ExtractFile fixture.

    Returns: None
    """
    # Arrange
    with mock.patch.object(extract_file, "_extract_batch") as extract_batch_mock, mock.patch.object(
        extract_file, "_extract_streaming"
    ) as extract_streaming_mock:
        # Act
        extract_file.extract()

    if extract_file.spec.method == ExtractMethod.BATCH:
        # Assert
        extract_batch_mock.assert_called_once()

    if extract_file.spec.method == ExtractMethod.STREAMING:
        # Assert
        extract_streaming_mock.assert_called_once()


def test_extract_file_extract(spark: SparkSession, df: DataFrame, extract_file: ExtractFile) -> None:
    """
    Assert that the write method of LoadFile writes the input DataFrame without any modifications.

    Args:
        extract_file (ExtractFile): ExtractFile fixture.
        spark (SparkSession): SparkSession fixture.
        df (DataFrame): Test DataFrame fixture.

    Returns: None
    """
    # Arrange
    df.write.format(extract_file.spec.data_format.value).save(extract_file.spec.location)

    # Act
    extract_df = extract_file.extract()

    if extract_file.spec.method == ExtractMethod.STREAMING:
        query = extract_df.writeStream.outputMode("append").format("memory").queryName("memory_table").start()
        query.awaitTermination(timeout=3)
        query.stop()
        extract_df = spark.sql("SELECT * FROM memory_table")

    # Assert
    testing.assertDataFrameEqual(actual=extract_df, expected=df)


def test_extract_file_extract_return_type(extract_file: ExtractFile, df: DataFrame) -> None:
    """
    Assert that the return type of the extract method in ExtractFile.

    Args:
        extract_file (ExtractFile): ExtractFile fixture.
        df (Dataframe): DataFrame ifxture.
    """
    # Arrange
    df.write.format(extract_file.spec.data_format.value).save(extract_file.spec.location)

    # Act
    return_type = extract_file.extract()

    # Assert
    assert isinstance(return_type, DataFrame)
