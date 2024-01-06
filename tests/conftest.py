"""
Fixtures to reuse.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from collections.abc import Generator

import pytest
from datastore.schema import Schema
from pyspark.sql import DataFrame

pytest_plugins = [
    "tests.file_test",
    "tests.logger_test",
    "tests.schema_test",
    "tests.spark_test",
    "tests.extract.base_test",
    "tests.extract.factory_test",
    "tests.load.base_test",
    "tests.load.factory_test",
]


# ============ Fixtures ================


@pytest.fixture(name="df")
def fixture_test_dataframe(spark, schema) -> DataFrame:
    """
    Fixture for a Spark DataFrame with three columns: name, age, job_title.

    Args:
        spark (SparkSession): The Spark session object.
        schema (StructType): Test dataframe schema fixture.

    Returns:
        DataFrame: The test DataFrame.
    """
    # Arrange: Prepare data for the test DataFrame
    data = [("Alice", 27, "Engineer"), ("Bob", 32, "Analyst"), ("Charlie", 73, "Manager")]

    # Act: Create a Spark DataFrame with the provided data and schema
    return spark.createDataFrame(data=data, schema=schema)


@pytest.fixture(name="df_empty")
def fixture_test_dataframe_empty(spark, schema: Schema) -> DataFrame:
    """
    Fixture for an empty Spark DataFrame

    Args:
        spark (SparkSession): The Spark session object.
        schema (Schema): Test dataframe schema fixture.

    Returns:
        DataFrame: Empty test DataFrame.
    """
    return spark.createDataFrame(data=[], schema=schema)


@pytest.fixture(name="confeti")
def fixture_confeti(extract_spec_confeti: dict, load_spec_confeti: dict) -> Generator[dict, None, None]:
    """
    Fixture for valid confeti file.

    Args:
        extract_spec_confeti (dict): ExtractSpec confeti fixture.
        load_spec_confeti (dict): LoadSpec confeti fixture.

    Yields:
        (dict): valid confeti.
    """
    confeti = {}
    confeti["extract"] = extract_spec_confeti
    confeti["load"] = load_spec_confeti

    yield confeti
