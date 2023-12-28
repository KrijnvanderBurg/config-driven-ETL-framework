import pytest
from os import path, remove

from pyspark.sql import SparkSession

from datastore.reader import ReaderSpec, ReaderFactory
from datastore.reader.parquet_reader import ParquetReader

@pytest.fixture(name = "spark")
def spark_session():
    return SparkSession.builder.master("local").appName("datastore").getOrCreate()

@pytest.fixture(name = "df")
def create_test_dataframe(spark):
    # Create a DataFrame with three columns
    data = [('Alice', 27, 'Engineer'), ('Bob', 32, 'Analyst'), ('Charlie', 73, 'Manager')]
    columns = ['name', 'age', 'job_title']

    return spark.createDataFrame(data, columns)

@pytest.fixture
def create_parquet_test_file(df):
    # Write the DataFrame to a Parquet file
    parquet_file_path = "/datastore/reader/test.parquet"
    df.write.parquet(parquet_file_path)

    yield parquet_file_path

    # Teardown, delete parquet file after test
    if path.exists(parquet_file_path):
        remove(parquet_file_path)

@pytest.fixture
def parquet_reader_spec_batch():
    return ReaderSpec(
        id = "test_bronze",
        reader_type = "batch",
        reader_format = "parquet",
        location = "/datastore/reader/test.parquet",
    )

@pytest.fixture
def parquet_reader_spec_streaming():
    return ReaderSpec(
        id = "test_bronze",
        reader_type = "streaming",
        reader_format = "parquet",
        location = "/datastore/reader/test.parquet",
    )

@pytest.fixture
def parquet_reader_factory_batch(parquet_reader_spec_batch):
    return ReaderFactory(spec = parquet_reader_spec_batch) 

@pytest.fixture
def parquet_reader_factory_batch(parquet_reader_spec_streaming):
    return ReaderFactory(spec = parquet_reader_spec_streaming) 

def test_parquet_reader_creation_without_factory():
    spec = ReaderSpec(
        id = "test_bronze",
        reader_type = "batch",
        reader_format = "parquet",
        location = "/path/to/data/test.parquet",
    )

    reader = ParquetReader(reader_spec = spec)
    assert reader._spec == spec

def test_reader_factory_create_parquet(parquet_reader):
    # test if ReaderFactory created ParquetReader object from parquet spec
    assert isinstance(parquet_reader, ParquetReader)

def test_parquet_reader_batch_equals_df(df, parquet_reader_spec_batch):
    # test if written df equals the parquet_reader df
    assert df == parquet_reader_spec_batch.read()

def test_parquet_reader_batch_equals_df(df, parquet_reader_spec_streaming):
    # test if written df equals the parquet_reader df
    assert df == parquet_reader_spec_streaming.read()
