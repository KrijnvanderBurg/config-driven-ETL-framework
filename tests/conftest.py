"""
Fixtures to reuse.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

import pytest
from datastore.schema import Schema
from pyspark.sql import DataFrame, SparkSession

pytest_plugins = [
    "tests.file_handler_test",
    "tests.logger_test",
    "tests.schema_test",
    "tests.spark_handler_test",
    "tests.extract.base_extract_test",
    "tests.extract.base_format_test",
    "tests.extract.base_method_test",
    "tests.extract.base_spec_test",
    "tests.extract.factory_test",
    "tests.extract.file_test",
    "tests.transform.base_spec_test",
    "tests.transform.base_transform_test",
    "tests.transform.factory_test",
    "tests.transform.functions.column_test",
    "tests.load.base_format_test",
    "tests.load.base_load_test",
    "tests.load.base_method_test",
    "tests.load.base_operation_test",
    "tests.load.base_spec_test",
    "tests.load.factory_test",
    "tests.load.file_test",
]

# ============ Fixtures ================


@pytest.fixture(name="df")
def fixture_test_df(spark: SparkSession, schema: Schema) -> DataFrame:
    """
    Fixture for a Spark DataFrame with three columns: name, age, job_title.

    Args:
        spark (SparkSession): The Spark session object.
        schema (StructType): Test df schema fixture.

    Returns:
        DataFrame: The test DataFrame.
    """
    # Arrange: Prepare data for the test DataFrame
    data = [("Alice", 27, "Engineer"), ("Bob", 32, "Analyst"), ("Charlie", 73, "Manager")]

    # Act: Create a Spark DataFrame with the provided data and schema
    return spark.createDataFrame(data=data, schema=schema)


@pytest.fixture(name="df_empty")
def fixture_test_df_empty(spark: SparkSession, schema: Schema) -> DataFrame:
    """
    Fixture for an empty Spark DataFrame

    Args:
        spark (SparkSession): The Spark session object.
        schema (Schema): Test df schema fixture.

    Returns:
        DataFrame: Empty test DataFrame.
    """
    return spark.createDataFrame(data=[], schema=schema)
