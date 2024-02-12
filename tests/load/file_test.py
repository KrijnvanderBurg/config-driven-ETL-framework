"""
File load class tests.

| ✓ | Tests
|---|------------------------------------------------------------------
| ✓ | Test load method returns correct type.
| ✓ | Test writing a dataframe and load it results in equal dataframe.
| ✓ | Test that protected methods call the respective load method.


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

# ============ Fixtures ============


@pytest.fixture(name="load_file_matrix")
def fixture_load_file(
    tmpdir, spark: SparkSession, df: DataFrame, schema: StructType, load_spec_matrix: LoadSpec
) -> Generator[LoadFile, None, None]:
    """
    Matrix fixture to create LoadFile instance.

    Args:
        tmpdir (str): Temporary directory fixture.
        spark (SparkSession): SparkSession fixture.
        df (DataFrame): DataFrame fixture.
        schema (StructType): Schema fixture.
        load_spec_matrix (LoadSpec): Matrix LoadSpec fixture.

    Yields:
        load_file (LoadFile): An instance of LoadFile for testing.
    """

    if load_spec_matrix.method == LoadMethod.BATCH:
        yield LoadFile(spec=load_spec_matrix, dataframe=df)

    if load_spec_matrix.method == LoadMethod.STREAMING:
        filepath = f"{tmpdir}/load.{load_spec_matrix.data_format}"

        df.write.format(load_spec_matrix.data_format.value).save(filepath)
        df_loadstream = spark.readStream.load(
            path=filepath,
            format=load_spec_matrix.data_format.value,
            schema=schema,
        )

        yield LoadFile(spec=load_spec_matrix, dataframe=df_loadstream)


# ============== Tests =============


def test_load_file_load_return_type(df: DataFrame, load_spec: LoadSpec) -> None:
    """
    Assert that the return type of the load method is a DataFrame.

    Args:
        df (DataFrame): Test DataFrame fixture.
        load_spec (LoadSpec): LoadFile fixture.
    """
    # Arrange
    load_file = LoadFile(spec=load_spec, dataframe=df)

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


def test_load_file_load_called(df: DataFrame, load_spec: LoadSpec):
    """
    Assert that protected methods are called for the respective method.

    Args:
        df (DataFrame): Test DataFrame fixture.
        load_spec (LoadSpec): LoadFile fixture.
    """
    # Arrange
    load_file = LoadFile(spec=load_spec, dataframe=df)

    # Arrange
    with (
        mock.patch.object(load_file, "_load_batch") as write_batch_mock,
        mock.patch.object(load_file, "_load_streaming") as write_streaming_mock,
    ):
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


def test_load_file_load_df_equals(
    spark: SparkSession, df: DataFrame, schema: StructType, load_file_matrix: LoadFile
) -> None:
    """
    Assert that load method writes the DataFrame without any modifications.

    Args:
        spark (SparkSession): SparkSession fixture.
        df (DataFrame): Test DataFrame fixture.
        schema (StructType): The schema of the test DataFrame fixture.
        load_file_matrix (LoadFile): LoadFile fixture.
    """
    # Act
    try:
        load = load_file_matrix.load()
    except AnalysisException:
        pytest.skip("Streaming witer format and type combination is unsupported.")

    if load_file_matrix.spec.method == LoadMethod.BATCH:
        load_df = spark.read.load(
            path=load_file_matrix.spec.location, format=load_file_matrix.spec.data_format.value, schema=schema
        )

        # Assert
        testing.assertDataFrameEqual(actual=load_df, expected=df, checkRowOrder=False)

    if load_file_matrix.spec.method == LoadMethod.STREAMING:
        if load:
            load.awaitTermination(timeout=3)
            load_df = spark.read.load(
                path=load_file_matrix.spec.location, format=load_file_matrix.spec.data_format.value, schema=schema
            )
            load.stop()

            # Assert
            testing.assertDataFrameEqual(actual=load_df, expected=df, checkRowOrder=False)
