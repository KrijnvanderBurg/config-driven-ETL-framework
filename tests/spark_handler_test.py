"""
Spark class tests.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from unittest import mock

import pytest
from datastore.spark_handler import SparkHandler
from pyspark.sql import SparkSession

# ============ Fixtures ================


@pytest.fixture(name="spark")
def fixture_spark_session() -> SparkSession:
    """
    Fixture for creating a Spark session.

    returns:
        SparkSession: The Spark session object.
    """
    # Arrange: Create a Spark session
    spark = SparkSession.builder.master("local").appName("datastore").getOrCreate()

    # Disable warnings about temporary checkpoint location
    spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")

    # Disable warning about adaptive execution in streaming
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    # Yield the Spark session
    return spark


# ============== Tests =================


def test_get_or_create_with_session(spark) -> None:
    """
    Assert that Spark session can be set when explicitly provided.

    Args:
        spark (SparkSession): SparkSession fixture.
    """
    # Act
    SparkHandler.get_or_create(session=spark)

    # Assert
    assert SparkHandler.session == spark


def test_get_or_create_without_session() -> None:
    """
    Assert that Spark session is created when none is provided.
    """
    # Act
    SparkHandler.get_or_create()

    # Assert
    assert SparkHandler.session is not None


def test_get_or_create_without_session_default_app_name() -> None:
    """
    Assert that Spark session app name is set to the default when no session is given and none exists.
    """
    # Arrange
    with mock.patch("pyspark.sql.SparkSession.getActiveSession", return_value=None):
        # Act
        SparkHandler.get_or_create()

    # Assert
    assert SparkHandler.session is not None
    assert SparkHandler.session.sparkContext.appName == "datastore"


def test_get_or_create_with_active_session() -> None:
    """
    Assert that Spark session is set from an active session with a specified app name.
    """
    # Arrange
    mock_session = SparkSession.builder.master("local").appName("mock").getOrCreate()

    with mock.patch("pyspark.sql.SparkSession.getActiveSession", return_value=mock_session):
        # Act
        SparkHandler.get_or_create()

    # Assert
    assert SparkHandler.session == mock_session


def test_get_or_create_with_active_session_no_app_name() -> None:
    """
    Assert that Spark session is set from an active session without an app name.
    """
    # Arrange
    mock_session = SparkSession.builder.master("local").getOrCreate()

    with mock.patch("pyspark.sql.SparkSession.getActiveSession", return_value=mock_session):
        # Act
        SparkHandler.get_or_create()

    # Assert
    assert SparkHandler.session == mock_session
    assert SparkHandler.session.sparkContext.appName == "datastore"
