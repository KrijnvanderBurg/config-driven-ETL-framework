"""Parquet reader class tests.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""
from os import environ, path

import pytest
from datastore.reader import ReaderSpec
from datastore.reader_factory import ReaderFactory
from datastore.readers.parquet_reader import ParquetReader
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual

environ["PYARROW_IGNORE_TIMEZONE"] = "1"  # to disable warning in pytest


@pytest.fixture(name="spark")
def fixture_spark_session():
    """Spark session fixture."""
    return SparkSession.builder.master("local").appName("datastore").getOrCreate()


@pytest.fixture(name="df")
def fixture_test_dataframe(spark):
    """Fixture of Spark DataFrame with three columns: name, age, job_title."""
    data = [("Alice", 27, "Engineer"), ("Bob", 32, "Analyst"), ("Charlie", 73, "Manager")]
    columns = ["name", "age", "job_title"]

    return spark.createDataFrame(data, columns)


@pytest.fixture(name="parquet_test_file")
def fixture_parquet_test_file(tmpdir, df):
    """Fixture to write Spark dataframe to Parquet file."""
    # Write the DataFrame to a Parquet file
    parquet_file_path = path.join(tmpdir, "test.parquet")
    df.write.parquet(parquet_file_path)

    yield parquet_file_path


@pytest.fixture(name="parquet_reader_spec_batch")
def fixture_parquet_reader_spec_batch():
    """Fixture for ReaderSpec with parquet batch values set."""
    return ReaderSpec(
        spec_id="test_bronze",
        reader_type="batch",
        reader_format="parquet",
        location="/datastore/reader/test.parquet",
    )


@pytest.fixture(name="parquet_reader_spec_streaming")
def fixture_parquet_reader_spec_streaming():
    """Fixture for ReaderSpec with parquet streaming values set."""
    return ReaderSpec(
        spec_id="test_bronze",
        reader_type="streaming",
        reader_format="parquet",
        location="/datastore/reader/test.parquet",
    )


@pytest.fixture(name="factory_parquet_reader_spec_batch")
def fixture_parquet_reader_factory_batch(parquet_reader_spec_batch):
    """Fixture for return ParquetReader batch object by calling ReaderFactory."""
    return ReaderFactory.get(spec=parquet_reader_spec_batch)


@pytest.fixture(name="factory_parquet_reader_spec_streaming")
def fixture_parquet_reader_factory_streaming(parquet_reader_spec_streaming):
    """Fixture for return ParquetReader streaming object by calling ReaderFactory."""
    return ReaderFactory.get(spec=parquet_reader_spec_streaming)


# def test_parquet_reader_creation_without_factory():
#     """Test if default ParquetReader parameter values are set."""
#     spec = ReaderSpec(
#         id="test_bronze",
#         reader_type="batch",
#         reader_format="parquet",
#         location="/path/to/data/test.parquet",
#     )
#     reader = ParquetReader(spec=spec)
#     # currently no default values exist therefore cannot assert anything yet
#     # assert reader.attribute_with_defaul_value is None


def test_reader_factory_create_parquet_batch(factory_parquet_reader_spec_batch):
    """Test if ReaderFactory created ParquetReader object from parquet batch spec."""
    assert isinstance(factory_parquet_reader_spec_batch, ParquetReader)


def test_reader_factory_create_parquet_streaming(factory_parquet_reader_spec_streaming):
    """Test if ReaderFactory created ParquetReader object from parquet streaming spec."""
    assert isinstance(factory_parquet_reader_spec_streaming, ParquetReader)


def test_parquet_reader_streaming_equals_df(df, parquet_test_file):
    """Test if written dataframe equals the parquet_reader dataframe."""

    spec = ReaderSpec(
        spec_id="test_bronze",
        reader_type="batch",
        reader_format="parquet",
        location=parquet_test_file,
    )

    reader = ReaderFactory.get(spec=spec)
    assertDataFrameEqual(df, reader.read())
