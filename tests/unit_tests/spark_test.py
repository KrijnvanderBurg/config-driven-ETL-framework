"""
Spark class tests.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from unittest.mock import patch

import pytest
from datastore.spark import Spark
from pyspark.sql import SparkSession


@pytest.fixture(name="spark")
def spark_session():
    """Spark session fixture."""
    return SparkSession.builder.master("local").appName("datastore").getOrCreate()


def test_get_or_create_with_session(spark):
    """Test if Spark session can be set."""
    Spark.get_or_create(session=spark)
    assert Spark.session == spark


def test_get_or_create_without_session():
    """Test if Spark session is none if none is given and no instance exists."""
    Spark.get_or_create()
    assert Spark.session is not None


def test_get_or_create_without_session_default_app_name():
    """Test Spark session app name."""
    # Patching SparkSession.getActiveSession() to return None for this test
    with patch("pyspark.sql.SparkSession.getActiveSession", return_value=None):
        Spark.get_or_create()
    assert Spark.session is not None
    assert Spark.session.sparkContext.appName == "datastore"


def test_get_or_create_with_active_session():
    """Test Spark session being set from get active session with app name."""
    # Patching SparkSession.getActiveSession() to return a mock session for this test
    mock_session = SparkSession.builder.master("local").appName("mock").getOrCreate()
    with patch("pyspark.sql.SparkSession.getActiveSession", return_value=mock_session):
        Spark.get_or_create()
    assert Spark.session == mock_session


def test_get_or_create_with_active_session_no_app_name():
    """Test Spark session being set from get active session without app name."""
    # Patching SparkSession.getActiveSession() to return a mock session with no app name for this test
    mock_session = SparkSession.builder.master("local").getOrCreate()
    with patch("pyspark.sql.SparkSession.getActiveSession", return_value=mock_session):
        Spark.get_or_create()
    assert Spark.session == mock_session
    assert Spark.session.sparkContext.appName == "datastore"
