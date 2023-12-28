import pytest
from pyspark.sql import SparkSession

from datastore.spark import Spark

@pytest.fixture
def spark_session():
    return SparkSession.builder.master("local").appName("datastore").getOrCreate()

def test_get_or_create_with_session(spark_session):
    # test if spark session can be set
    Spark.get_or_create(session = spark_session)
    assert Spark.session == spark_session

def test_get_or_create_without_session():
    # test if spark session is none if none is given and no instance exists
    Spark.get_or_create()
    assert Spark.session is not None

def test_get_or_create_without_session_default_app_name():
    # Patching SparkSession.getActiveSession() to return None for this test
    with pytest.patch('pyspark.sql.SparkSession.getActiveSession', return_value = None):
        Spark.get_or_create()
    assert Spark.session is not None
    assert Spark.session.sparkContext.appName == "datastore"

def test_get_or_create_with_active_session():
    # Patching SparkSession.getActiveSession() to return a mock session for this test
    mock_session = SparkSession.builder.master("local").appName("mock").getOrCreate()
    with pytest.patch('pyspark.sql.SparkSession.getActiveSession', return_value = mock_session):
        Spark.get_or_create()
    assert Spark.session == mock_session

def test_get_or_create_with_active_session_no_app_name():
    # Patching SparkSession.getActiveSession() to return a mock session with no app name for this test
    mock_session = SparkSession.builder.master("local").getOrCreate()
    with pytest.patch('pyspark.sql.SparkSession.getActiveSession', return_value = mock_session):
        Spark.get_or_create()
    assert Spark.session == mock_session
    assert Spark.session.sparkContext.appName == "datastore"
