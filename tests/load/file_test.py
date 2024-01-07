"""
File load class tests.

| ✓ | Tests
|---|----------------------------------------------------------------
| ✓ | Correct functions are called in .write() for batch and streaming.
| ✓ | Writing to and extracting from file results in equal dataframe.
| ✓ | Write returns the correct object instance.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from collections.abc import Generator
from unittest import mock

import pytest
from datastore.load.base import LoadMethod, LoadSpec
from datastore.load.file import LoadFile
from pyspark import testing
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

# ==================================
# ======== LoadFile class ==========
# ==================================

# ============ Fixtures ============


@pytest.fixture(name="load_file")
def fixture_load_file(
    tmpdir, spark: SparkSession, load_spec: LoadSpec, df: DataFrame, schema: StructType
) -> Generator[LoadFile, None, None]:
    """
    Fixture to create instances of LoadFile.

    Args:
        tmpdir (str): Temporary directory fixture.
        spark (SparkSession): SparkSession fixture.
        load_spec (LoadSpec): The LoadSpec for creating LoadFile fixture.
        df (DataFrame): Test DataFrame fixture.
        schema (StructType): The schema of the test DataFrame fixture.

    Yields:
        load_file (LoadFile): An instance of LoadFile for testing.
    """

    if load_spec.method == LoadMethod.BATCH:
        yield LoadFile(spec=load_spec, dataframe=df)

    if load_spec.method == LoadMethod.STREAMING:
        filepath = f"{tmpdir}/test_extract.{load_spec.data_format}"

        df.write.format(load_spec.data_format.value).save(filepath)
        df_extractstream = spark.readStream.load(
            path=filepath,
            format=load_spec.data_format.value,
            schema=schema,
        )
        yield LoadFile(spec=load_spec, dataframe=df_extractstream)


# ============== Tests =================


def test_load_file_load_called(load_file: LoadFile):
    """
    Assert that _load_batch() and _load_streaming() functions are called inside .write() method
    for batch and streaming respectively.

    Args:
        load_file (LoadFile): LoadFile fixture.
    """
    # Arrange
    with mock.patch.object(load_file, "_load_batch") as write_batch_mock, mock.patch.object(
        load_file, "_load_streaming"
    ) as write_streaming_mock:
        # Act
        try:
            load_file.load()
        except AnalysisException:
            pytest.skip("Streaming witer format and type combination is unsupported.")

    if load_file.spec.method == LoadMethod.BATCH:
        # Assert
        write_batch_mock.assert_called_once()

    if load_file.spec.method == LoadMethod.STREAMING:
        # Assert
        write_streaming_mock.assert_called_once()


def test_load_file_load(spark: SparkSession, df: DataFrame, schema: StructType, load_file: LoadFile) -> None:
    """
    Assert that the write method of LoadFile writes the input DataFrame without any modifications.

    Args:
        spark (SparkSession): SparkSession fixture.
        df (DataFrame): Test DataFrame fixture.
        schema (StructType): The schema of the test DataFrame fixture.
        load_file (LoadFile): LoadFile fixture.
    """
    # Act
    try:
        load = load_file.load()
    except AnalysisException:
        pytest.skip("Streaming witer format and type combination is unsupported.")

    if load_file.spec.method == LoadMethod.BATCH:
        extract_df = spark.read.load(
            path=load_file.spec.location, format=load_file.spec.data_format.value, schema=schema
        )

        # Assert
        testing.assertDataFrameEqual(actual=extract_df, expected=df, checkRowOrder=False)

    if load_file.spec.method == LoadMethod.STREAMING:
        if load:
            load.awaitTermination(timeout=3)
            extract_df = spark.read.load(
                path=load_file.spec.location, format=load_file.spec.data_format.value, schema=schema
            )
            load.stop()

            # Assert
            testing.assertDataFrameEqual(actual=extract_df, expected=df, checkRowOrder=False)


def test_load_file_load_return_type(load_file: LoadFile) -> None:
    """
    Assert that the return type of the write method in LoadFile.

    Args:
        load_file (LoadFile): LoadFile fixture.
    """
    # Act
    try:
        return_type = load_file.load()
    except AnalysisException:
        pytest.skip("Streaming witer format and type combination is unsupported.")

    # Assert
    if load_file.spec.method == LoadMethod.BATCH:
        assert isinstance(return_type, type(None))
    elif load_file.spec.method == LoadMethod.STREAMING:
        assert isinstance(return_type, StreamingQuery)
