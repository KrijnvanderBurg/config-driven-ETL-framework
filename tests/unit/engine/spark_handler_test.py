"""
Tests for the SparkHandler class.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession

from datastore.engine.spark_handler import SparkHandler


class TestSparkHandler:
    """
    Test cases for the SparkHandler class.
    """

    def test_get_or_create_with_provided_session(self):
        """Test get_or_create method assigns provided session and avoids new creation."""
        # Arrange
        with patch("pyspark.sql.SparkSession.builder") as mock_spark_session_builder:
            mock_session = MagicMock(spec=SparkSession)
            mock_spark_session_builder.appName().getOrCreate.return_value = mock_session

            # Act
            returned_session = SparkHandler.get_or_create(session=mock_session)

            # Assert
            assert SparkHandler.session == mock_session
            assert returned_session == mock_session
            mock_spark_session_builder.appName().getOrCreate.assert_not_called()

    def test_get_or_create_without_provided_session(self):
        """Test get_or_create method creates a new session when none provided."""
        # Arrange
        with patch("pyspark.sql.SparkSession.builder") as mock_spark_session_builder:
            mock_session = MagicMock(spec=SparkSession)
            mock_spark_session_builder.appName().getOrCreate.return_value = mock_session

            # Act
            returned_session = SparkHandler.get_or_create()

            # Assert
            assert SparkHandler.session == mock_session
            assert returned_session == mock_session
            mock_spark_session_builder.appName().getOrCreate.assert_called_once_with()

    def test_get_or_create_with_config(self):
        """Test get_or_create method assigns provided configuration."""
        # Arrange
        with patch("pyspark.sql.SparkSession.builder") as mock_spark_session_builder:
            mock_session = MagicMock(spec=SparkSession)
            mock_spark_session_builder.appName().getOrCreate.return_value = mock_session
            config = {"spark.executor.memory": "4g", "spark.executor.cores": "2"}

            # Act
            returned_session = SparkHandler.get_or_create(config=config)

            # Assert
            assert SparkHandler.session == mock_session
            assert returned_session == mock_session
            mock_spark_session_builder.appName().config.assert_any_call("spark.executor.memory", "4g")
            mock_spark_session_builder.appName().config.assert_any_call("spark.executor.cores", "2")
            assert mock_spark_session_builder.appName().config.call_count == 2
            mock_spark_session_builder.appName().getOrCreate.assert_called_once_with()
