"""
File writer class tests.

# | ✓ | Tests
# |---|----------------------------------------------------------------
# | ✓ | Correct functions are called in .write() for batch and streaming.
# | ✓ | Writing to and reading from file results in equal dataframe.
# | ✓ | Write returns the correct object instance.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from collections.abc import Generator
from unittest import mock

import pytest
from datastore.writer import WriterSpec, WriterType
from datastore.writers.writer_file import WriterFile
from pyspark import testing
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

# ==================================
# ======= WriterFile class =========
# ==================================

# ============ Fixtures ============


@pytest.fixture(name="writer_file")
def fixture_writer_file(
    tmpdir, spark: SparkSession, writer_spec: WriterSpec, df: DataFrame, schema: StructType
) -> Generator[WriterFile, None, None]:
    """
    Fixture to create instances of WriterFile.

    Args:
        tmpdir (str): Temporary directory fixture.
        spark (SparkSession): SparkSession fixture.
        writer_spec (WriterSpec): The WriterSpec for creating WriterFile fixture.
        df (DataFrame): Test DataFrame fixture.
        schema (StructType): The schema of the test DataFrame fixture.

    Yields:
        writer_file (WriterFile): An instance of WriterFile for testing.
    """

    if writer_spec.writer_type == WriterType.BATCH:
        yield WriterFile(spec=writer_spec, dataframe=df)

    if writer_spec.writer_type == WriterType.STREAMING:
        filepath = f"{tmpdir}/test_read.{writer_spec.writer_format}"

        df.write.format(writer_spec.writer_format.value).save(filepath)
        df_readstream = spark.readStream.load(
            path=filepath,
            format=writer_spec.writer_format.value,
            schema=schema,
        )
        yield WriterFile(spec=writer_spec, dataframe=df_readstream)


# ============== Tests =================


def test_writer_file_write_called(writer_file: WriterFile):
    """
    Assert that _write_batch() and _write_streaming() functions are called inside .write() method
    for batch and streaming respectively.

    Args:
        writer_file (WriterFile): WriterFile fixture.
    """
    # Arrange
    with mock.patch.object(writer_file, "_write_batch") as write_batch_mock, mock.patch.object(
        writer_file, "_write_streaming"
    ) as write_streaming_mock:
        # Act
        try:
            writer_file.write()
        except AnalysisException:
            pytest.skip("Streaming witer format and type combination is unsupported.")

    if writer_file.spec.writer_type == WriterType.BATCH:
        # Assert
        write_batch_mock.assert_called_once()

    if writer_file.spec.writer_type == WriterType.STREAMING:
        # Assert
        write_streaming_mock.assert_called_once()


def test_writer_file_write(writer_file: WriterFile, spark: SparkSession, df: DataFrame, schema: StructType) -> None:
    """
    Assert that the write method of WriterFile writes the input DataFrame without any modifications.

    Args:
        writer_file (WriterFile): WriterFile fixture.
        spark (SparkSession): SparkSession fixture.
        df (DataFrame): Test DataFrame fixture.
        schema (StructType): The schema of the test DataFrame fixture.
    """
    # Act
    try:
        writer = writer_file.write()
    except AnalysisException:
        pytest.skip("Streaming witer format and type combination is unsupported.")

    if writer_file.spec.writer_type == WriterType.BATCH:
        read_df = spark.read.load(
            path=writer_file.spec.location, format=writer_file.spec.writer_format.value, schema=schema
        )

        # Assert
        testing.assertDataFrameEqual(actual=read_df, expected=df, checkRowOrder=False)

    if writer_file.spec.writer_type == WriterType.STREAMING:
        if writer:
            writer.awaitTermination(timeout=3)
            read_df = spark.read.load(
                path=writer_file.spec.location, format=writer_file.spec.writer_format.value, schema=schema
            )
            writer.stop()

            # Assert
            testing.assertDataFrameEqual(actual=read_df, expected=df, checkRowOrder=False)


def test_writer_file_write_return_type(writer_file: WriterFile) -> None:
    """
    Assert that the return type of the write method in WriterFile.

    Args:
        writer_file (WriterFile): WriterFile fixture.
    """
    # Act
    try:
        return_type = writer_file.write()
    except AnalysisException:
        pytest.skip("Streaming witer format and type combination is unsupported.")

    # Assert
    if writer_file.spec.writer_type == WriterType.BATCH:
        assert isinstance(return_type, type(None))
    elif writer_file.spec.writer_type == WriterType.STREAMING:
        assert isinstance(return_type, StreamingQuery)
