import unittest
from unittest.mock import Mock, patch

from pyspark.sql import SparkSession

from stratum.utils.spark_handler import SparkHandler


class TestSparkHandler(unittest.TestCase):
    def setUp(self) -> None:
        SparkHandler._instances.clear()  # type: ignore

    @patch.object(SparkSession, "Builder")
    def test_init__default(self, mock_builder: Mock) -> None:
        SparkHandler()

        mock_builder.assert_called_once()
        mock_builder.return_value.appName.assert_called_once_with(name="stratum")
        mock_builder.return_value.appName().config.assert_not_called()

    @patch.object(SparkSession, "Builder")
    def test_init__custom(self, mock_builder: Mock) -> None:
        SparkHandler(app_name="test_app", options={"spark.executor.memory": "1g"})

        mock_builder.assert_called_once()
        mock_builder.return_value.appName.assert_called_once_with(name="test_app")
        mock_builder.return_value.appName().config.assert_called_once_with(key="spark.executor.memory", value="1g")

    @patch.object(SparkSession, "Builder")
    def test__session_getter(self, mock_builder: Mock) -> None:
        spark_handler = SparkHandler()

        self.assertEqual(spark_handler._session, spark_handler.session)  # type: ignore

    @patch("pyspark.sql.SparkSession")
    def test__session_deleter(self, mock_session: Mock) -> None:
        spark_handler = SparkHandler()

        spark_handler._session = mock_session.return_value  # type: ignore
        del spark_handler.session

        mock_session.return_value.stop.assert_called_once()

    @patch("pyspark.sql.SparkSession")
    def test_add_configs(self, mock_session: Mock) -> None:
        spark_handler = SparkHandler()
        spark_handler._session = mock_session.return_value  # type: ignore

        configs = {"spark.executor.memory": "2g", "spark.executor.cores": "4"}
        spark_handler.add_configs(configs)

        for key, value in configs.items():
            mock_session.return_value.conf.set.assert_any_call(key=key, value=value)
