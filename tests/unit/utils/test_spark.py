from unittest.mock import Mock, patch

import pytest
from pyspark.sql import SparkSession

from ingestion_framework.utils.spark import SparkHandler


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset the singleton instance before each test."""
    SparkHandler._instances.clear()  # type: ignore


@patch.object(SparkSession, "Builder")
def test_init_default(mock_builder: Mock) -> None:
    """Test default initialization of SparkHandler."""
    SparkHandler()

    mock_builder.assert_called_once()
    mock_builder.return_value.appName.assert_called_once_with(name="ingestion_framework")
    mock_builder.return_value.appName().config.assert_not_called()


@patch.object(SparkSession, "Builder")
def test_init_custom(mock_builder: Mock) -> None:
    """Test custom initialization of SparkHandler."""
    SparkHandler(app_name="test_app", options={"spark.executor.memory": "1g"})

    mock_builder.assert_called_once()
    mock_builder.return_value.appName.assert_called_once_with(name="test_app")
    mock_builder.return_value.appName().config.assert_called_once_with(key="spark.executor.memory", value="1g")


@patch.object(SparkSession, "Builder")
def test_session_getter(_: Mock) -> None:
    """Test getting session property."""
    spark_handler = SparkHandler()

    assert spark_handler._session == spark_handler.session


@patch("pyspark.sql.SparkSession")
def test_session_deleter(mock_session: Mock) -> None:
    """Test session deletion."""
    spark_handler = SparkHandler()

    spark_handler._session = mock_session.return_value  # type: ignore
    del spark_handler.session

    mock_session.return_value.stop.assert_called_once()


@patch("pyspark.sql.SparkSession")
def test_add_configs(mock_session: Mock) -> None:
    """Test adding configurations."""
    spark_handler = SparkHandler()
    spark_handler._session = mock_session.return_value  # type: ignore

    configs = {"spark.executor.memory": "2g", "spark.executor.cores": "4"}
    spark_handler.add_configs(configs)

    for key, value in configs.items():
        mock_session.return_value.conf.set.assert_any_call(key=key, value=value)
