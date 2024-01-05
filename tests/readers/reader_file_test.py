"""
File reader class tests.

# | ✓ | Tests
# |---|----------------------------------------------------------------
# | ✓ | Correct functions are called in .read() for batch and streaming.
# | ✓ | Writing to and reading from file results in equal dataframe.
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
from datastore.reader import ReaderSpec, ReaderType
from datastore.readers.reader_file import ReaderFile
from pyspark import testing
from pyspark.sql import DataFrame, SparkSession

# ==================================
# ======== ReaderFile class ========
# ==================================

# ============ Fixtures ============


@pytest.fixture(name="reader_file")
def fixture_reader_file(reader_spec: ReaderSpec) -> Generator[ReaderFile, None, None]:
    """
    Fixture function to create a ReaderFile instance for testing.

    Args:
        reader_spec (ReaderSpec): The ReaderSpec for testing.

    Yields:
        ReaderFile: A ReaderFile instance with the provided ReaderSpec.
    """
    yield ReaderFile(spec=reader_spec)


# ============== Tests =================


def test_reader_file_read_called(reader_file: ReaderFile) -> None:
    """
    Assert that _read_batch() and _read_streaming() functions are called inside .read() method
    for batch and streaming respectively.

    Args:
        reader_file (ReaderFile): ReaderFile fixture.

    Returns: None
    """
    # Arrange
    with mock.patch.object(reader_file, "_read_batch") as read_batch_mock, mock.patch.object(
        reader_file, "_read_streaming"
    ) as read_streaming_mock:
        # Act
        reader_file.read()

    if reader_file.spec.reader_type == ReaderType.BATCH:
        # Assert
        read_batch_mock.assert_called_once()

    if reader_file.spec.reader_type == ReaderType.STREAMING:
        # Assert
        read_streaming_mock.assert_called_once()


def test_reader_file_read(spark: SparkSession, df: DataFrame, reader_file: ReaderFile) -> None:
    """
    Assert that the write method of WriterFile writes the input DataFrame without any modifications.

    Args:
        reader_file (ReaderFile): ReaderFile fixture.
        spark (SparkSession): SparkSession fixture.
        df (DataFrame): Test DataFrame fixture.

    Returns: None
    """
    # Arrange
    df.write.format(reader_file.spec.reader_format.value).save(reader_file.spec.location)

    # Act
    read_df = reader_file.read()

    if reader_file.spec.reader_type == ReaderType.STREAMING:
        query = read_df.writeStream.outputMode("append").format("memory").queryName("memory_table").start()
        query.awaitTermination(timeout=3)
        query.stop()
        read_df = spark.sql("SELECT * FROM memory_table")

    # Assert
    testing.assertDataFrameEqual(actual=read_df, expected=df)


def test_reader_file_read_return_type(reader_file: ReaderFile, df: DataFrame) -> None:
    """
    Assert that the return type of the read method in ReaderFile.

    Args:
        reader_file (ReaderFile): ReaderFile fixture.
        df (Dataframe): DataFrame ifxture.
    """
    # Arrange
    df.write.format(reader_file.spec.reader_format.value).save(reader_file.spec.location)

    # Act
    return_type = reader_file.read()

    # Assert
    assert isinstance(return_type, DataFrame)
